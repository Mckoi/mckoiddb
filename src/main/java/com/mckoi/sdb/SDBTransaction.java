/**
 * com.mckoi.sdb.SDBTransaction  Jun 26, 2009
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
import com.mckoi.data.Key;
import com.mckoi.data.KeyObjectTransaction;
import com.mckoi.data.StringData;
import com.mckoi.network.CommitFaultException;
import com.mckoi.network.DataAddress;
import com.mckoi.network.MckoiDDBClient;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A transaction class that represents a snapshot view of a Simple Database
 * path instance. This class provides methods for managing the primary data
 * types supported: Files (com.mckoi.sdb.SDBFile) and Tables
 * (com.mckoi.sdb.SDBTable).
 * <p>
 * <b>THREAD SAFETY NOTICE</b>: Instances of SDBTransaction and instances of
 * all objects created by SDBTransaction are <b>NOT</b> thread safe
 * and may not be interacted with by concurrent threads. Doing so could result
 * in corrupting a path instance state. If you wish to interact with a path
 * instance from multiple threads, an SDBTransaction object must
 * be created for each individual thread.
 * <p>
 * See the MckoiDDB documentation for examples of using this class to access
 * and modify the contents of a database.
 *
 * @author Tobias Downer
 */

// Not SDBTrustedObject. Most returned classes are trusted, however.
public class SDBTransaction {

  /**
   * The session object this transaction is part of.
   */
  private final SDBSession session;

  /**
   * The base root DataAddress of this transaction. This is the base root as
   * returned when the transaction was originally created. Used when building
   * the commit log.
   */
  private final DataAddress base_root;

  /**
   * The KeyObjectTransaction object.
   */
  private final KeyObjectTransaction transaction;

  /**
   * The log of operations on file objects during this transaction.
   */
  private final ArrayList<String> log;

  /**
   * Set to true when this transaction object is invalidated.
   */
  private boolean invalidated = false;

  /**
   * The map of all SDBTable objects created during this transaction.
   */
  private final HashMap<String, SDBTable> table_map;


  /**
   * The DirectorySet for all files stored in the data model.
   */
  private final DirectorySet file_set;

  /**
   * The DirectorySet for all tables stored in the data model.
   */
  private final DirectorySet table_set;



  /**
   * The Key that contains static information about the data model, such as
   * the magic value, etc.
   */
  static final Key MAGIC_KEY = new Key((short) 0, 0, 0);


  /**
   * File set keys,
   */
  static final Key FILE_SET_PROPERTIES = new Key((short) 0, 1, 13);
  static final Key FILE_SET_NAMEMAP    = new Key((short) 0, 1, 14);
  static final Key FILE_SET_INDEX      = new Key((short) 0, 1, 15);

  /**
   * Table set keys,
   */
  static final Key TABLE_SET_PROPERTIES = new Key((short) 0, 1, 18);
  static final Key TABLE_SET_NAMEMAP    = new Key((short) 0, 1, 19);
  static final Key TABLE_SET_INDEX      = new Key((short) 0, 1, 20);

  /**
   * The Key that contains the log information for this transaction.
   */
  static final Key TRANSACTION_LOG_KEY = new Key((short) 0, 1, 11);


  /**
   * Constructor.
   */
  SDBTransaction(SDBSession session,
                 DataAddress base_root, KeyObjectTransaction transaction) {

    this.session = session;
    this.base_root = base_root;
    this.transaction = transaction;
    this.log = new ArrayList();
    this.table_map = new HashMap();

    this.file_set = new DirectorySet(transaction,
            FILE_SET_PROPERTIES,
            FILE_SET_NAMEMAP,
            FILE_SET_INDEX,
            (short) 0, 10);
    this.table_set = new DirectorySet(transaction,
            TABLE_SET_PROPERTIES,
            TABLE_SET_NAMEMAP,
            TABLE_SET_INDEX,
            (short) 0, 11);

  }





  /**
   * Returns the KeyObjectTransaction object this transaction is mapped to.
   */
  KeyObjectTransaction getKeyObjectTransaction() {
    return transaction;
  }

  /**
   * Invalidates this transaction object.
   */
  private void invalidate() {
    invalidated = true;
  }

  /**
   * Checks if this transaction is invalidated or not. If it is throws a
   * runtime exception.
   */
  void checkValid() {
    if (invalidated) {
      throw new RuntimeException("Transaction has been invalidated");
    }
  }

  /**
   * Checks that the given file name contains valid characters only. Generates
   * an exception if not.
   */
  private void checkFileNameValid(String name) {
    // Sanity checks,
    if (name == null) {
      throw new NullPointerException("Null file name");
    }
    int len = name.length();
    if (len <= 0 || len > 1024) {
      throw new RuntimeException("Invalid file name: " + name);
    }
    for (int i = 0; i < len; ++i) {
      char c = name.charAt(i);
      if (Character.isSpaceChar(c) || !Character.isDefined(c)) {
        throw new RuntimeException("Invalid file name: " + name);
      }
    }
  }

  /**
   * Checks that the given table name contains valid characters only. Generates
   * an exception if not.
   */
  private void checkTableNameValid(String name) {
    // Sanity checks,
    if (name == null) {
      throw new NullPointerException("Null table name");
    }
    int len = name.length();
    if (len <= 0 || len > 1024) {
      throw new RuntimeException("Invalid table name: " + name);
    }
    for (int i = 0; i < len; ++i) {
      char c = name.charAt(i);
      if (Character.isSpaceChar(c) || !Character.isDefined(c)) {
        throw new RuntimeException("Invalid table name: " + name);
      }
    }
  }

  /**
   * Logs that a file mutation has happened.
   */
  void logFileChange(String file_name) {
    // Checks the log for any mutations on this filename, if not found adds the
    // mutation to the log.
    String mutation_log_entry = "FM" + file_name;
    for (String entry : log) {
      if (entry.equals(mutation_log_entry)) {
        // Found so return,
        return;
      }
    }
    // Not found, so add it to the end
    log.add(mutation_log_entry);
  }

  /**
   * Returns the log file as a Reader object, where the contents can be read
   * sequentially.
   */
  Reader getLogReader() {
    DataFile transaction_log =
                            transaction.getDataFile(TRANSACTION_LOG_KEY, 'r');
    StringData log_file = new StringData(transaction_log);
    return log_file.getReader();
  }

  /**
   * Clears the transaction log file, and then appends the entries from the
   * 'log' ArrayList into the log.
   */
  void refreshTransactionLog() {
    // Output the change log to the proposed transaction to commit,
    DataFile transaction_log =
                          transaction.getDataFile(TRANSACTION_LOG_KEY, 'w');
    transaction_log.delete();
    StringData log_file = new StringData(transaction_log);

    // Record the base root in the log,
    log_file.append(base_root.formatString());
    log_file.append("\n");

    // Write out every log entry,
    for (String entry : log) {
      log_file.append(entry);
      log_file.append("\n");
    }
  }

  /**
   * Static function used by system functions to write a transaction log that
   * indicates to the commit function that this is an introduced SimpleDatabase
   * transaction with no version checks. This would be used to write out a
   * transaction to be committed where the data has been imported generically
   * (such as a backup recovery procedure).
   */
  public static void writeForcedTransactionIntroduction(
                                           KeyObjectTransaction transaction) {
    // Output the change log to the proposed transaction to commit,
    DataFile transaction_log =
                          transaction.getDataFile(TRANSACTION_LOG_KEY, 'w');
    transaction_log.delete();
    StringData log_file = new StringData(transaction_log);

    // Record that there is no base root for this transaction,
    log_file.append("no base root");
    log_file.append("\n");
  }

  /**
   * Create a cleared transaction log.
   */
  private void writeClearedTransactionLog() {
    writeForcedTransactionIntroduction(transaction);
  }

  // Transaction local cache of files that have been copied into this
  // transaction.
  private HashSet<String> file_copy_set = null;
  private HashSet<String> table_copy_set = null;
  /**
   * Replays a log entry from the given transaction in this transaction.
   */
  void replayFileLogEntry(String entry, SDBTransaction from_transaction) {
    // Get the operation type and operation code,
    char t = entry.charAt(0);
    char op = entry.charAt(1);
    String name = entry.substring(2);
    // If this is a file operation,
    if (t == 'F') {
      if (op == 'C') {
        createFile(name);
      }
      else if (op == 'D') {
        deleteFile(name);
      }
      else if (op == 'M') {
        if (file_copy_set == null) {
          file_copy_set = new HashSet();
        }
        // Copy the contents from the source if it hasn't already been
        // copied.
        if (!file_copy_set.contains(name)) {
          from_transaction.file_set.copyTo(name, file_set);
          file_copy_set.add(name);
        }
        // Make sure to copy this event into the log in this transaction,
        log.add(entry);
      }
      else {
        throw new RuntimeException("Transaction log entry error: " + entry);
      }
    }
    else {
      throw new RuntimeException("Transaction log entry error: " + entry);
    }
  }

  /**
   * Replays a log entry from the given transaction in this transaction.
   */
  void replayTableLogEntry(String entry, SDBTransaction from_transaction,
                           boolean has_historic_data_changes) {

    char t = entry.charAt(0);
    char op = entry.charAt(1);
    String name = entry.substring(2);
    // If this is a table operation,
    if (t == 'T') {
      if (op == 'C') {
        createTable(name);
      }
      else if (op == 'D') {
        deleteTable(name);
      }
      else if (op == 'M' || op == 'S') {
        // If it's a TS event (a structural change to the table), we need to
        // pass this to the table merge function.
        boolean structural_change = (op == 'S');
        // To replay a table modification
        if (table_copy_set == null) {
          table_copy_set = new HashSet();
        }
        if (!table_copy_set.contains(name)) {
          SDBTable st = from_transaction.getTable(name);
          SDBTable dt = getTable(name);
          // Merge the source table into the destination table,
          dt.mergeFrom(st, structural_change, has_historic_data_changes);
          table_copy_set.add(name);
        }
        // Make sure to copy this event into the log in this transaction,
        log.add(entry);
      }
      else {
        throw new RuntimeException("Transaction log entry error: " + entry);
      }
    }
    else {
      throw new RuntimeException("Transaction log entry error: " + entry);
    }
  }






  /**
   * Returns the KeyObjectTransaction object that backs this SDBTransaction.
   * The returned object will not contain the changes made to this
   * SDBTransaction since it was created. The returned KeyObjectTransaction can
   * be changed but the changes will not manifest in the data managed by this
   * SDBTransaction.
   * <p>
   * It is intended that this object be used for diagnostic and debugging
   * purposes only.
   */
  public KeyObjectTransaction createBaseTransaction() {
    return session.getDatabaseClient().createTransaction(base_root);
  }

  /**
   * Returns the SDBRootAddress that represents the current base root node for
   * this transaction. This can be used by clients to record a snapshot state
   * locally to be recreated into an SDBTransaction without any additional
   * network interaction needed.
   */
  public SDBRootAddress getBaseRoot() {
    return new SDBRootAddress(session, base_root);
  }

  /**
   * Returns the name of the path of the session of this transaction.
   */
  public String getSessionPathName() {
    return session.getPathName();
  }


  // ----- User operations -----

  // ----- FILES

  /**
   * Returns the total number of files currently stored in this snapshot.
   */
  public long getFileCount() {
    return file_set.size();
  }

  /**
   * Provides the list of all file names stored in this snapshot. The contents
   * of the list are fetched dynamically as the list is traversed so can
   * efficiently handle a very large set of files stored.
   * <p>
   * Note that the returned List object is invalidated if the directory is
   * changed in this transaction (eg, a file is created or deleted). If the
   * directory is changed, a call to 'fileList' must be made to fetch the
   * new updated list.
   */
  public List<String> fileList() {
    checkValid();
    return file_set.itemSet();
  }

  /**
   * Checks if the file name exists, returning true if it does.
   */
  public boolean fileExists(String file_name) {
    checkValid();
    checkFileNameValid(file_name);

    return file_set.getItem(file_name) != null;
  }

  /**
   * Creates a new named file of zero length. If returns true, a new file was
   * created in the local transaction. If returns false, the file could not be
   * created because it already exists. Throws an exception if the file name
   * contains an invalid character.
   */
  public boolean createFile(String file_name) {
    checkValid();
    checkFileNameValid(file_name);
    
    Key k = file_set.getItem(file_name);
    if (k != null) {
      return false;
    }

    k = file_set.addItem(file_name);
    // Log this operation,
    log.add("FC" + file_name);

    // File created so return success,
    return true;

  }

  /**
   * Deletes the given file name from this snapshot. Returns false if the file
   * could not be deleted because it doesn't exist. Returns true if the file
   * exists and was deleted.
   */
  public boolean deleteFile(String file_name) {
    checkValid();
    checkFileNameValid(file_name);

    Key k = file_set.getItem(file_name);
    if (k == null) {
      return false;
    }

    file_set.removeItem(file_name);
    // Log this operation,
    log.add("FD" + file_name);

    // File deleted so return success,
    return true;

  }

  /**
   * Fetches an SDBFile object representing the content of a file in this
   * snapshot. Generates an exception if the file does not exist.
   */
  public SDBFile getFile(String file_name) {
    checkValid();
    checkFileNameValid(file_name);

    Key k = file_set.getItem(file_name);
    if (k == null) {
      // Doesn't exist, so throw an exception
      throw new RuntimeException("File doesn't exist: " + file_name);
    }

    // Wrap the object in order to capture update events,
    return new SDBFile(this, file_name,
                       file_set.getItemDataFile(file_name));

  }

  // ----- TABLES

  /**
   * Returns the total count of tables currently stored in this snapshot.
   */
  public long getTableCount() {
    return table_set.size();
  }

  /**
   * Provides the list of all table names stored in this snapshot. The contents
   * of the list are fetched dynamically as the list is traversed so can
   * efficiently handle a very large set of tables stored.
   * <p>
   * Note that the returned List object is invalidated if the directory is
   * changed in this transaction (eg, a table is created or deleted). If the
   * directory is changed, a call to 'tableList' must be made to fetch the
   * new updated list.
   */
  public List<String> tableList() {
    checkValid();
    return table_set.itemSet();
  }

  /**
   * Checks if the table exists, and returns true if it does.
   */
  public boolean tableExists(String table_name) {
    checkValid();
    checkTableNameValid(table_name);

    return table_set.getItem(table_name) != null;
  }

  /**
   * Creates a new named empty table. If returns true, the new table was
   * created in the transaction. If returns false, the table could not be
   * created because it already exists. Throws an exception if the table
   * name contains an invalid character.
   */
  public boolean createTable(String table_name) {
    checkValid();
    checkTableNameValid(table_name);

    Key k = table_set.getItem(table_name);
    if (k != null) {
      return false;
    }

    k = table_set.addItem(table_name);

    long kid = k.getPrimary();
    if (kid > Long.MAX_VALUE) {
      // We ran out of keys so can't make any more table items,
      // This happens after 2 billion tables created. We need to note this
      // as a limitation.
      throw new RuntimeException("Id pool exhausted for table item.");
    }

    // Log this operation,
    log.add("TC" + table_name);

    // Table created so return success,
    return true;

  }

  /**
   * Deletes a table with the given name from the local transaction. Returns
   * false if the table could not be deleted because it doesn't exist. Returns
   * true if the table exists and was deleted.
   */
  public boolean deleteTable(String table_name) {
    checkValid();
    checkTableNameValid(table_name);

    Key k = table_set.getItem(table_name);
    if (k == null) {
      return false;
    }

    // Fetch the table, and delete all data associated with it,
    SDBTable table;
    synchronized (table_map) {
      table = getTable(table_name);
      table_map.remove(table_name);
    }
    table.deleteFully();

    // Remove the item from the table directory,
    table_set.removeItem(table_name);

    // Log this operation,
    log.add("TD" + table_name);

    // Table deleted so return success,
    return true;

  }

  /**
   * Fetches an SDBTable object representing the content of a table in this
   * snapshot. Generates an exception if the table does not exist.
   */
  public SDBTable getTable(String table_name) {
    checkValid();
    checkTableNameValid(table_name);

    // Is it in the map?
    synchronized (table_map) {
      SDBTable table = table_map.get(table_name);
      if (table != null) {
        // It's there, so return it,
        return table;
      }

      Key k = table_set.getItem(table_name);
      if (k == null) {
        // Doesn't exist, so throw an exception
        throw new RuntimeException("Table doesn't exist: " + table_name);
      }

      long kid = k.getPrimary();
      if (kid > Long.MAX_VALUE) {
        // We ran out of keys so can't make any more table items,
        // This happens after 2 billion tables created. We need to note this
        // as a limitation.
        throw new RuntimeException("Id pool exhausted for table item.");
      }

      // Turn the key into an SDBTable,
      table = new SDBTable(this,
                           table_set.getItemDataFile(table_name),(int) kid);
      table_map.put(table_name, table);
      // And return it,
      return table;
    }

  }








//  /**
//   * FOR TESTING ONLY
//   */
//  public SDBTable getTestTable() {
//    return new SDBTable(this, 100);
//  }




  /**
   * Commits the current transaction. If no changes have been made then
   * this does nothing. If changes have been made, the proposed changes are
   * passed to the consensus function on the root server managing this path
   * instance. The consensus function may choose to reject a proposed
   * change in which case a commit fault exception is generated.
   */
  public SDBRootAddress commit() throws CommitFaultException {
    checkValid();

    try {
      // Update transaction information on the tables that were modified during
      // this transaction.
      synchronized (table_map) {
        Set<String> tables = table_map.keySet();
        for (String table_name : tables) {
          SDBTable table = table_map.get(table_name);
          if (table.wasModified()) {
            // Log this modification,
            // If there was a structural change to the table we log as a TS
            // event
            if (table.hasStructuralModification()) {
              log.add("TS" + table_name);
            }
            // Otherwise we log as a TM event (rows or columns were deleted).
            else {
              log.add("TM" + table_name);
            }
            // Write out the table log
            table.prepareForCommit();
          }
        }
      }

      // Process the transaction log and write it out to a DataFile for the
      // consensus processor to handle,

      // If there are changes to commit,
      if (log.size() > 0) {

        // Refresh the transaction log with the entries stored in 'log'
        refreshTransactionLog();

        // The database client,
        MckoiDDBClient db_client = session.getDatabaseClient();

        // Flush the transaction to the network
        DataAddress proposal = db_client.flushTransaction(transaction);
        // Perform the commit operation,
        return new SDBRootAddress(session,
                db_client.performCommit(session.getPathName(), proposal));

      }
      else {
        // No changes, so return base root
        return new SDBRootAddress(session, base_root);
      }

    }
    // Make sure transaction is invalidated
    finally {
      // Invalidate this transaction
      invalidate();
    }
  }

  /**
   * Forces the publication of this transaction as the current version of
   * the given SDBSession, causing any data in the database represented by the
   * session to be lost and replaced with the view of the database in this
   * transaction. After this transaction snapshot has been published,
   * any transactions that have snapshot views previous to when this publish
   * operation happened will commit fault if they try and commit.
   * <p>
   * Use this function with care! This operation bypasses all the commit
   * checks. The operation of publishing a transaction will not fail with a
   * CommitFaultException like committing a transaction can. After this method
   * returns, this transaction will represent the current state of the
   * destination path instance. This does not make a change to this
   * transaction's database instance unless destination_session is this
   * transaction's database instance.
   * <p>
   * This operation would typically be used if the published state of a
   * database instance needs to be rolled back to a previous version. It can
   * also be used to mount test branches of an active database.
   * <p>
   * Returns the SDBRootAddress of the transaction created in the destination
   * session.
   */
  public SDBRootAddress publishToSession(SDBSession destination_session) {
    // Refresh the transaction log with a cleared 'no base root' version.
    // This operation sets up the transaction log appropriately so that the
    // 'commit' process of future transactions will understand that this
    // version is not an iteration of previous versions.
    writeClearedTransactionLog();

    // The database client,
    MckoiDDBClient db_client = session.getDatabaseClient();
    // Flush the transaction to the network
    DataAddress to_publish = db_client.flushTransaction(transaction);

    try {
      DataAddress published =
              db_client.performCommit(destination_session.getPathName(),
                                      to_publish);
      // Return the root in the destionation session,
      return new SDBRootAddress(destination_session, published);
    }
    catch (CommitFaultException e) {
      // This shouldn't be thrown,
      throw new RuntimeException("Unexpected Commit Fault", e);
    }
  }

  /**
   * The GC finalizer method for this object.
   */
  public void finalize() throws Throwable {
//    System.out.println(this.toString() + " was disposed.");
    // For a network tree system, this isn't really necessary,
    session.getDatabaseClient().disposeTransaction(transaction);
  }

}
