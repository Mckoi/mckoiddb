/**
 * com.mckoi.sdb.SimpleDatabase  Jun 26, 2009
 *
 * Mckoi Database Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2010  Diehl and Associates, Inc.
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License version 3
 * along with this program.  If not, see ( http://www.gnu.org/licenses/ ) or
 * write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * Change Log:
 *
 *
 */

package com.mckoi.sdb;

import com.mckoi.data.DataFile;
import com.mckoi.data.Iterator64Bit;
import com.mckoi.data.KeyObjectTransaction;
import com.mckoi.data.OrderedList64Bit;
import com.mckoi.data.PropertySet;
import com.mckoi.data.TreeSystemTransaction;
import com.mckoi.network.CommitFaultException;
import com.mckoi.network.ConsensusDDBConnection;
import com.mckoi.network.ConsensusProcessor;
import com.mckoi.network.DataAddress;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * An implementation of a ConsensusProcessor for the Simple Database data
 * model. This class manages a simple database which implements a
 * directory of file and table objects.
 * <p>
 * Simple Database provides concurrency control models for the file and table
 * objects it supports. Files may not be concurrently modified (a concurrent
 * modification to a file will generate an exception and rollback at commit
 * time). Tables may be concurrently modified provided the change does not
 * clash (for example, multiple transactions commit a row delete on the
 * same row).
 * <p>
 * The concurrency control for the meta structures (the file and table name
 * directory) will causes a commit fault if concurrent transactions try to
 * create or delete an object with the same name.
 * <p>
 * This data model does not implement features you'd expect from a full
 * file system such as file meta-data (file creation time, access lists for
 * security, etc). There is no notion of a directory tree with sub-directories,
 * however file names can be of any length so an ad-hoc directory hierarchy can
 * be created by the name string.
 *
 * @author Tobias Downer
 */

public class SimpleDatabase implements ConsensusProcessor {

  // ----- Plug in Information -----

  /**
   * The name of this processor, displayed in the adminsitration user
   * interface.
   */
  public String getName() {
    return "Mckoi Simple Database";
  }

  /**
   * A description of this processor appropriate for display in the help
   * section of the user interface.
   */
  public String getDescription() {
    return "A simple database model that supports file and table object types.";
  }

  // ----- Function -----

  /**
   * Returns the size and number of files stored in the most recent snapshot.
   */
  public String getStats(ConsensusDDBConnection connection,
                         DataAddress snapshot) {
    try {
      // Turn it into a transaction
      KeyObjectTransaction t = connection.createTransaction(snapshot);
      // Turn it into a TreeSystemTransaction object,
      TreeSystemTransaction tst = (TreeSystemTransaction) t;

      // Turn it into an SDBTransaction object,
      SDBTransaction sfs_t = new SDBTransaction(null, snapshot, t);
      // Size of the current snapshot in bytes,
      long size = tst.fastSizeCalculate();
      // Number of files,
      long table_count = sfs_t.getTableCount();
      long file_count = sfs_t.getFileCount();

      return size + " bytes (in " + table_count + " tables " +
             file_count + " files)";

    }
    catch (Throwable e) {
      // Catch all exception, we return an error string
      return "getStats Exception: " + e.getMessage();
    }
  }

  /**
   * {@inheritDoc }
   */
  public void initialize(ConsensusDDBConnection connection) {
    // Get the current root,
    DataAddress current_root = connection.getCurrentSnapshot();
    // Turn it into a transaction
    KeyObjectTransaction transaction = connection.createTransaction(current_root);
    // Initialize the magic property set, etc
    DataFile df = transaction.getDataFile(SDBTransaction.MAGIC_KEY, 'w');
    PropertySet magic_set = new PropertySet(df);
    magic_set.setProperty("ob_type", "com.mckoi.sdb.SimpleDatabase");
    magic_set.setProperty("version", "1.0");
    // Flush and publish the change
    DataAddress final_root = connection.flushTransaction(transaction);
    connection.publishToPath(final_root);
  }

//  private final static Object NO_OB = new Object();

  /**
   * {@inheritDoc }
   */
  public synchronized DataAddress commit(ConsensusDDBConnection connection,
                            DataAddress proposal) throws CommitFaultException {

    // Turn the proposal into a proposed_transaction,
    KeyObjectTransaction t = connection.createTransaction(proposal);
    SDBTransaction proposed_transaction = new SDBTransaction(null, proposal, t);

    try {
      // Fetch the base root from the proposed_transaction log,
      BufferedReader reader =
                       new BufferedReader(proposed_transaction.getLogReader());
      String base_root_str = reader.readLine();
      // If 'base_root_str' is "no base root" then it means we are commiting
      // an introduced transaction that is not an iteration of previous
      // snapshots.
      if (base_root_str.equals("no base root")) {

        // In which case, we publish the proposed snapshot unconditionally
        // and return.
        connection.publishToPath(proposal);
        return proposal;

      }

      DataAddress base_root = DataAddress.parseString(base_root_str);

      // Find all the entries since this base
      DataAddress[] roots = connection.getSnapshotsSince(base_root);

//      System.out.println("ROOTS SINCE: ");
//      for (DataAddress da : roots) {
//        System.out.print(da);
//        System.out.print(", ");
//      }
//      System.out.println();

      // If there are no roots, we can publish the proposed_transaction
      // unconditionally
      if (roots.length == 0) {

        connection.publishToPath(proposal);
        return proposal;

      }
      else {

        // Check historical log for clashes, and if none, replay the commands
        // in the log.

        // For each previous root, we build a structure that can answer the
        // following questions;
        // * Is file [name] created, deleted or changed in this root?
        // * Is table [name] created or deleted in this root?
        // * Is table [name] structurally changed in this root?
        // * Has row [rowid] been deleted in table [name] in this root?

        RootEvents[] root_event_set = new RootEvents[roots.length];
        int i = 0;

        // PENDING: RootEvents is pre-computed information which we could
        //   store in a local cache for some speed improvements, so we don't
        //   need to walk through through the same data multiple times.

        for (DataAddress root : roots) {
          RootEvents root_events = new RootEvents();
          root_event_set[i] = root_events;
          ++i;
          // Create a transaction object for this root
          KeyObjectTransaction root_t = connection.createTransaction(root);
          SDBTransaction root_transaction =
                                       new SDBTransaction(null, root, root_t);
          // Make a reader object for the log,
          BufferedReader root_reader =
                          new BufferedReader(root_transaction.getLogReader());
          // Read the base root from this transaction,
          String base_root_parent = root_reader.readLine();
          // If 'bast_root_parent' is 'no base root' then it means a version
          // has been introduced that is not an iteration of previous
          // snapshots. In this case, it is not possible to merge updates
          // therefore we generate a commit fault.
          if (base_root_parent.equals("no base root")) {
            throw new CommitFaultException(
                          "Transaction history contains introduced version.");
          }

          // Go through each log entry and determine if there's a clash,
          String root_line = root_reader.readLine();
          while (root_line != null) {
            String mfile = root_line.substring(2);
            // This represents a file modification,
            boolean unknown_command = false;
            if (root_line.startsWith("F")) {
              root_events.setFileChange(mfile);
            }
            // This is a table modification,
            else if (root_line.startsWith("T")) {
              char c = root_line.charAt(1);
              // If this is a table create or delete event,
              if (c == 'C' || c == 'D') {
                root_events.setTableCreateOrDelete(mfile);
              }
              // This is a table structural change,
              else if (c == 'S') {
                root_events.setTableStructuralChange(mfile);
              }
              // This is a table data change event,
              else if (c == 'M') {
                SDBTable table = root_transaction.getTable(mfile);
                root_events.setTableDataChange(mfile, table);
              }
              else {
                unknown_command = true;
              }
            }
            else {
              unknown_command = true;
            }
            if (unknown_command) {
              throw new RuntimeException(
                                 "Unknown transaction command: " + root_line);
            }
            // Read the next log entry,
            root_line = root_reader.readLine();
          }
        }

        // Now we have a set of RootEvents objects that describe what
        // happens in each previous root.

        // Now replay the events in the proposal transaction in the latest
        // transaction.

        // A transaction representing the current state,
        DataAddress current_root = connection.getCurrentSnapshot();
        KeyObjectTransaction current_t =
                                   connection.createTransaction(current_root);
        SDBTransaction current_transaction =
                            new SDBTransaction(null, current_root, current_t);
        String entry = reader.readLine();
        while (entry != null) {
          String mfile = entry.substring(2);
          // If it's a file entry, we need to check the file hasn't been
          // changed in any way in any roots
          if (entry.startsWith("F")) {
            for (RootEvents events : root_event_set) {
              events.checkFileChange(mfile);
            }
//            System.out.println("REPLAYING: " + entry);
            // All checks passed, so perform the operation
            current_transaction.replayFileLogEntry(entry, proposed_transaction);
          }
          // If it's a table entry,
          else if (entry.startsWith("T")) {
            // Check that a table with this name hasn't been created, deleted
            // or modified,
            for (RootEvents events : root_event_set) {
              // This fails on any event on this table, except a data change
              // (insert or delete)
              events.checkTableMetaChange(mfile);
            }
            // The type of operation,
            char c = entry.charAt(1);
            // Is it a table structural change?
            if (c == 'S') {
              // A structural change can only happen if all the roots leave the
              // table untouched,
              for (RootEvents events : root_event_set) {
                // This fails if it finds a delete event for this rowid
                events.checkTableDataChange(mfile);
              }
            }
            // Is it a table modification command?
            else if (c == 'M') {
              // This is a table modification, we need to check the rowid
              // logs and look for possible clashes,
              // The delete set from the proposed transaction,
              SDBTable proposed_table = proposed_transaction.getTable(mfile);
              OrderedList64Bit delete_set = proposed_table.getDeleteSet();
              Iterator64Bit dsi = delete_set.iterator();
              while (dsi.hasNext()) {
                long rowid = dsi.next();
                for (RootEvents events : root_event_set) {
                  // This fails if it finds a delete event for this rowid
                  events.checkTableDataDelete(mfile, rowid);
                }
              }
            }
            // Go through each root, if the data in the table was changed
            // by any of the roots, we set 'has_data_changes' to true;
            boolean has_data_changes = false;
            for (RootEvents events : root_event_set) {
              if (events.hasTableDataChanges(mfile)) {
                has_data_changes = true;
              }
            }

            // Ok, checks passed, so reply all the data changes on the table
            current_transaction.replayTableLogEntry(
                    entry, proposed_transaction, has_data_changes);
          }
          else {
            throw new RuntimeException(
                                     "Unknown transaction command: " + entry);
          }

          // Read the next log entry,
          entry = reader.readLine();
        }

        // Refresh the transaction log
        current_transaction.refreshTransactionLog();

        // Flush and publish the change
        DataAddress final_root = connection.flushTransaction(current_t);
        connection.publishToPath(final_root);

        // Done.
        return final_root;
      }

    }
    catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("IO Error: " + e.getMessage());
    }

  }



  /**
   * An object that records information that can be queried about an historic
   * transaction. Used during the commit process to determine if a change
   * is incompatible.
   */
  private static class RootEvents {

    // The set of all files changed in this root,
    private HashSet<String> files_changed;
    // The set of all tables created or deleted in this root,
    private HashSet<String> tables_created_deleted;
    // The set of all tables structurally changed in this root,
    private HashSet<String> tables_structural;
    // A map of table to SDBTable of all tables that were changed in the root,
    private HashMap<String, SDBTable> table_data_changed;


    /**
     * Returns true if there are data changes recorded for this table in this
     * root.
     */
    boolean hasTableDataChanges(String table) {
      if (table_data_changed == null) {
        return false;
      }
      return table_data_changed.containsKey(table);
    }

    /**
     * Signifies the root changed the file is some way (either created, deleted
     * or modified the contents of).
     */
    void setFileChange(String file_name) {
      if (files_changed == null) {
        files_changed = new HashSet();
      }
      files_changed.add(file_name);
    }

    /**
     * Signifies the root created or deleted a table with this name.
     */
    void setTableCreateOrDelete(String table_name) {
      if (tables_created_deleted == null) {
        tables_created_deleted = new HashSet();
      }
      tables_created_deleted.add(table_name);
    }

    /**
     * Signifies the root caused a structural change to happen to the table
     * with this name (column or indexed added/removed).
     */
    void setTableStructuralChange(String table_name) {
      if (tables_structural == null) {
        tables_structural = new HashSet();
      }
      tables_structural.add(table_name);
    }

    /**
     * Signifies the root caused rows to be added or deleted from the table
     * with the given name. The given SDBTable object can be used to query the
     * rows added or deleted in the table in this root.
     */
    void setTableDataChange(String table_name, SDBTable table) {
      if (table_data_changed == null) {
        table_data_changed = new HashMap();
      }
      table_data_changed.put(table_name, table);
    }

    // ----- Queries -----

    /**
     * Checks if a file was modified in any way. If so, generates an
     * appropriate CommitFaultException.
     */
    void checkFileChange(String file) throws CommitFaultException {
      if (files_changed != null &&
          files_changed.contains(file)) {
        throw new CommitFaultException(
                "File ''{0}'' was modified by a concurrent transaction", file);
      }
    }

    /**
     * Checks if a table was modified in any way, including if the data
     * changed, created, deleted or structurally changed.
     */
    void checkTableDataChange(String table) throws CommitFaultException {
      if (table_data_changed != null &&
          table_data_changed.containsKey(table)) {
        throw new CommitFaultException(
              "Table ''{0}'' was modified by a concurrent transaction", table);
      }
    }

    /**
     * Checks if the structure of a table was changed in any way. This includes
     * adding or removing columns and indexes, or if a table with this name was
     * created or deleted. If so, generates an appropriate
     * CommitFaultException. This will not fail if rows were added or deleted
     * in the table.
     */
    void checkTableMetaChange(String table) throws CommitFaultException {
      // Check if the table was created or deleted in this root
      if (tables_created_deleted != null &&
          tables_created_deleted.contains(table)) {
        throw new CommitFaultException(
            "Table ''{0}'' was modified by a concurrent transaction", table);
      }
      // Check if the table was structurally changed in this root
      if (tables_structural != null &&
          tables_structural.contains(table)) {
        throw new CommitFaultException(
            "Table ''{0}'' was structurally modified by a concurrent transaction",
            table);
      }
    }

    /**
     * Checks if the given rowid of the given table was deleted in this root.
     * If so, generates an appropriate CommitFaultException.
     * <p>
     * Returns true if this root has changes to the data stored in the table.
     */
    void checkTableDataDelete(String table, long rowid)
                                                 throws CommitFaultException {
      // Is it in the modification set?
      if (table_data_changed != null) {
        SDBTable t = table_data_changed.get(table);
        if (t != null) {
          // Yes, so check if the given row in the modification set,
          OrderedList64Bit delete_set = t.getDeleteSet();
          if (delete_set.containsSortKey(rowid)) {
            // Yes, so generate a commit fault,
            throw new CommitFaultException(
              "Row in Table ''{0}'' was modified by a concurrent transaction",
              table);
          }
        }
      }
    }

  }

}
