/*
 * Mckoi Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2015  Diehl and Associates, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mckoi.odb;

import com.mckoi.data.*;
import com.mckoi.network.CommitFaultException;
import com.mckoi.network.ConsensusDDBConnection;
import com.mckoi.network.ConsensusProcessor;
import com.mckoi.network.DataAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of a ConsensusProcessor for the Object Database data
 * model. This class manages a the transaction rules of the object data
 * model.
 * <p>
 * As a summary commit faults are generated in the following situations; 1)
 * When an object is mutated concurrently (mutable components are ODBObjects
 * with mutable fields and ODBData). 2) When a duplicate key is added to a
 * list that does not allow duplicates. 3) When the same entry is removed from
 * a list. 4) When the same named item is created or deleted concurrently. 5)
 * When an object is created or modified and it references an object that is
 * no longer valid, or the reference is added to a list or as a named item and
 * is invalid.
 *
 * @author Tobias Downer
 */

public class ObjectDatabase implements ConsensusProcessor {

  /**
   * The logger.
   */
  private static final Logger LOG = Logger.getLogger("com.mckoi.network.Log");

  private final Object commit_lock = new Object();

  // ----- Plug in Information -----

  /**
   * The name of this processor, displayed in the administration user
   * interface.
   * @return 
   */
  @Override
  public String getName() {
    return "Mckoi Object Database";
  }

  /**
   * A description of this processor appropriate for display in the help
   * section of the user interface.
   * @return 
   */
  @Override
  public String getDescription() {
    return "An object data model that supports primitives for building " +
           "persistent graph structures.";
  }


  @Override
  public String getStats(ConsensusDDBConnection connection,
                         DataAddress snapshot) {
    try {
      // Turn it into a transaction
      KeyObjectTransaction t = connection.createTransaction(snapshot);
      // Turn it into a TreeSystemTransaction object,
      TreeSystemTransaction tst = (TreeSystemTransaction) t;

      // TODO: This is just the very basic info of the number of bytes stored
      //   in the tree. We should include object count and other stats.

      // Size of the current snapshot in bytes,
      long size = tst.fastSizeCalculate();

      return size + " bytes";

    }
    catch (Throwable e) {
      // Catch all exception, we return an error string
      return "getStats Exception: " + e.getMessage();
    }
  }

  @Override
  public void initialize(ConsensusDDBConnection connection) {
    // Get the current root,
    DataAddress current_root = connection.getCurrentSnapshot();
    // Turn it into a transaction
    KeyObjectTransaction transaction = connection.createTransaction(current_root);
    // Initialize the magic property set, etc
    DataFile df = transaction.getDataFile(ODBTransactionImpl.MAGIC_KEY, 'w');
    PropertySet magic_set = new PropertySet(df);
    magic_set.setProperty("ob_type", "com.mckoi.odb.ObjectDatabase");
    magic_set.setProperty("version", "1.0");
    // Flush and publish the change
    DataAddress final_root = connection.flushTransaction(transaction);
    connection.publishToPath(final_root);
  }

  @Override
  public DataAddress commit(ConsensusDDBConnection connection,
                            DataAddress proposal) throws CommitFaultException {

    synchronized (commit_lock) {

      // Turn the proposal into a proposed ODBTransaction object,
      KeyObjectTransaction t = connection.createTransaction(proposal);
      ODBTransactionImpl proposed_transaction =
                              new ODBTransactionImpl(null, proposal, t, false);

      // The transaction log,
      ObjectLog proposed_object_log = proposed_transaction.getProposedObjectLog();
//      object_log.printDebug();

      // The base root proposal of this transaction log,
      DataAddress base_root = proposed_object_log.getBaseRoot();

      // If 'base_root' is null then it means we are commiting an introduced
      // transaction that is not an iteration of previous snapshots.
      if (base_root == null) {

        // In which case, we publish the proposed snapshot unconditionally
        // and return.
        connection.publishToPath(proposal);
        return proposal;

      }

      // Find all the entries since this base
      DataAddress[] roots = connection.getSnapshotsSince(base_root);

//      System.out.println("ROOTS SINCE: ");
//      for (DataAddress da : roots) {
//        System.out.print(da);
//        System.out.print(", ");
//      }
//      System.out.println();

      // If there are no previous entries, we can publish the
      // proposed_transaction unconditionally
      if (roots.length == 0) {

        connection.publishToPath(proposal);
        return proposal;

      }
      else {

        // Create a new transaction representing the current snapshot,
        DataAddress current_root = connection.getCurrentSnapshot();
        KeyObjectTransaction current_t =
                                   connection.createTransaction(current_root);
        ODBTransactionImpl current_transaction =
                  new ODBTransactionImpl(null, current_root, current_t, false);

        // There are roots, so we need to go through a merge process and
        // create a new proposal and ensure it is consistent.

        // The merge process...
        // --------------------

//        object_log.printDebug();

//        int counter = 0;

        // Merge dictionary entries,

        Iterator<DictionaryEvent> di =
                                proposed_object_log.getDictionaryAddIterator();
        while (di.hasNext()) {
          DictionaryEvent evt = di.next();
          proposed_transaction.copyDictionaryAdditionTo(
                                                     current_transaction, evt);
//          ++counter;
        }

//        System.out.println("Dictionary Adds = " + counter);

        // We move all the new data from the proposed_transaction into the
        // current_transaction,

        // Move allocations of data/list and class buckets from the proposal
        // first;

        HashMap<Key, String> resource_map = new HashMap<>();
        final String NEW_KEY  = "NK";
        final String SAME_KEY = "SK";

        Key max_key = ZERO_KEY;

//        counter = 0;

        // For each root,
        for (DataAddress root : roots) {
          KeyObjectTransaction rt = connection.createTransaction(root);
          ODBTransactionImpl root_transaction =
                                 new ODBTransactionImpl(null, root, rt, false);

          // The transaction log,
          ObjectLog root_log = root_transaction.getProposedObjectLog();

          // The maximum resource value of this transaction root,
          Key root_max_key = root_transaction.getResourceLookupMaxKey();
          // Make sure the new transaction inherits the largest key from the
          // intermediate roots,
          if (root_max_key.compareTo(max_key) > 0) {
            max_key = root_max_key;
          }

          // Get an iterator for resources allocated in the proposed
          // transaction,
          Iterator<KeyAllocation> pi =
                                     proposed_object_log.getKeyAllocIterator();

          // For each resource allocated in the proposal,
          while (pi.hasNext()) {
            KeyAllocation alloc = pi.next();
            Key src_key = alloc.getKey();
            // Does the root log also allocate this?
            if (root_log.hasKeyAllocated(src_key)) {
              // If so, this is a clash condition, so allocate a new resource
              // and add it to the map,
              resource_map.put(src_key, NEW_KEY);
            }
            // If it doesn't, we copy it with the same key only if we haven't
            // previously decided this resource needs to be remapped,
            else {
              if (resource_map.get(src_key) == null) {
                resource_map.put(src_key, SAME_KEY);
              }
            }
            // Make sure the max_key is as large as the greatest key in the
            // proposed set
            if (src_key.compareTo(max_key) > 0) {
              max_key = src_key;
            }

//            ++counter;

          }

        }

//        System.out.println("Key allocs = " + counter);

        // -----

        // Make sure the lookup table is updated,
        if (max_key.compareTo(ZERO_KEY) > 0) {
          current_transaction.updateResourceLookupMaxKey(max_key);

          // Now we move all the resources created in the proposal over to the
          // current transaction.

          Iterator<KeyAllocation> pi =
                                     proposed_object_log.getKeyAllocIterator();

          // For each resource allocated in the proposal,
          while (pi.hasNext()) {
            KeyAllocation alloc = pi.next();

            Key src_key = alloc.getKey();
            Key dst_key;

            // If the key needs to be mapped to a new key,
            if (resource_map.get(src_key).equals(NEW_KEY)) {
              dst_key = proposed_transaction.copyResourceAsNewKeyTo(
                                 current_transaction, alloc.getRef(), src_key);
              if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, "Resource key remapped, {0} to {1}",
                        new Object[] { src_key, dst_key });
              }
            }
            // Otherwise we copy under the same key,
            else {
              dst_key = proposed_transaction.copyResourceTo(
                                 current_transaction, alloc.getRef(), src_key);
            }

          }

        }

        // -----

        // Make sure there are no clashing change events,

//        counter = 0;

        // For each root,
        for (DataAddress root : roots) {
          KeyObjectTransaction rt = connection.createTransaction(root);
          ODBTransactionImpl root_transaction =
                                 new ODBTransactionImpl(null, root, rt, false);

          // The transaction log,
          ObjectLog root_log = root_transaction.getProposedObjectLog();

          // Check for object change clashes,
          Iterator<ObjectChangeEvent> obi =
                                 proposed_object_log.getObjectChangeIterator();
          while (obi.hasNext()) {
            ObjectChangeEvent evt = obi.next();

            // Check there is no clashing change in the root log
            if (root_log.hasObjectChange(evt)) {
              // The object changed historically, so generate a commit fault,
              throw new CommitFaultException(
                          "Object at reference {0} concurrently modified",
                          evt.getObjectReference());
            }
//            ++counter;
          }

          // Check for data change clashes,
          Iterator<DataChangeEvent> dbi =
                                 proposed_object_log.getDataChangeIterator();
          while (dbi.hasNext()) {
            DataChangeEvent evt = dbi.next();

            // Check there is no clashing change in the root log
            if (root_log.hasDataChange(evt)) {
              // The data changed historically, so generate a commit fault,
              throw new CommitFaultException(
                          "Data at reference {0} concurrently modified",
                          evt.getDataReference());
            }
//            ++counter;
          }

        }

//        System.out.println("Object changes = " + (counter / roots.length));

        // Now every object construction and change in the proposal must be
        // copied to the current transaction.

        Iterator<ObjectChangeEvent> obi =
                               proposed_object_log.getObjectChangeIterator();
        while (obi.hasNext()) {
          ObjectChangeEvent evt = obi.next();

          // Replay this change or construction
          current_transaction.replayObjectChange(proposed_transaction, evt);

        }

        // Copy every data change in the proposal to the current transaction.

        Iterator<DataChangeEvent> dbi =
                               proposed_object_log.getDataChangeIterator();
        while (dbi.hasNext()) {
          DataChangeEvent evt = dbi.next();

          // Replay this object change or construction
          // This performs a straight copy of the data item from the
          // proposal
          current_transaction.replayDataChange(proposed_transaction, evt);

        }

        // -----

        // If a list has not been changed in the roots and changed in the
        // proposal, we can simply copy the list and log entries for the list
        // into the new transaction.

        // The set of lists changed in the proposal also changed in the
        // roots list,
        HashSet<Reference> list_builds = new HashSet<>();

        // For each root,
        for (DataAddress root : roots) {
          KeyObjectTransaction rt = connection.createTransaction(root);
          ODBTransactionImpl root_transaction =
                                 new ODBTransactionImpl(null, root, rt, false);

          // The transaction log,
          ObjectLog root_log = root_transaction.getProposedObjectLog();

          Iterator<ListChangeEvent> lci =
                                   proposed_object_log.getListChangeIterator();
          while (lci.hasNext()) {
            ListChangeEvent evt = lci.next();

            // Check there is no clashing change in the root log
            if (root_log.hasListChange(evt)) {
              list_builds.add(evt.getListReference());
            }
          }
        }

        // The lists that were not changed in the history
        Iterator<ListChangeEvent> lci =
                                 proposed_object_log.getListChangeIterator();
        while (lci.hasNext()) {
          ListChangeEvent evt = lci.next();
          // If the list is not in the list builds, then we can simply copy
          // it over,
          if (!list_builds.contains(evt.getListReference())) {
            // Copy the list and the log entries over from the proposed
            // transaction,
            proposed_transaction.copyListTo(
                                  current_transaction, evt.getListReference(),
                                  proposed_object_log);
          }
        }

        // Finally replay list operations from the proposal in the current
        // transaction. A commit fault happens if a record is deleted that
        // is not found.

        // Get from the log an iterator of the list items that were added
        // during this proposed transaction,
        {
//          counter = 0;

          Iterator<ListItemChangeEvent> lii =
                                  proposed_object_log.getListItemAddIterator();
          while (lii.hasNext()) {
            ListItemChangeEvent evt = lii.next();

            if (list_builds.contains(evt.getListReference())) {
              try {
                // We add the item to the list,
                current_transaction.replayListItemAdd(
                               proposed_transaction,
                               evt.getListReference(), evt.getObjectReference(),
                               evt.getListClassReference());
              }
              catch (ConstraintViolationException e) {
                throw new CommitFaultException(e.getMessage());
              }
//              ++counter;
            }
          }

//          System.out.println("List item add = " + counter);
        }

        // Get from the log an iterator of the list items that were removed
        // during this proposed transaction,
        {
//          counter = 0;

          Iterator<ListItemChangeEvent> lii =
                               proposed_object_log.getListItemRemoveIterator();
          while (lii.hasNext()) {
            ListItemChangeEvent evt = lii.next();

            if (list_builds.contains(evt.getListReference())) {
              try {
                // We add the item to the list,
                current_transaction.replayListItemRemove(
                             proposed_transaction,
                             evt.getListReference(), evt.getObjectReference(),
                             evt.getListClassReference());
              }
              catch (ConstraintViolationException e) {
                throw new CommitFaultException(e.getMessage());
              }
//              ++counter;
            }
          }

//          System.out.println("List item remove = " + counter);
        }

        // Done.


        // Flush and publish the new proposal with the merged changes,
        DataAddress final_root = connection.flushTransaction(current_t);
        connection.publishToPath(final_root);

        // Done.
        return final_root;

      }

    }

  }



  // ---------- Inner classes ----------

//  private static class ResourceMap {
//
//    private final HashMap<KeyAllocation, Key> transfer_map;
//
//    /**
//     * Constructor.
//     */
//    ResourceMap() {
//      transfer_map = new HashMap(45);
//    }
//
//    /**
//     * Puts a re-mapped key into this map.
//     */
//    void putDestinationKey(KeyAllocation src, Key dst) {
//
//      transfer_map.put(src, dst);
//
//    }
//
//    /**
//     * Given a key in the source transaction (the proposed transaction),
//     * returns a key that is unique within the created transaction.
//     */
//    Key getDestinationKey(final KeyAllocation source_key) {
//
//      // If the source key is in the transfer map,
//      Key tkey = transfer_map.get(source_key);
//      // If not in the map, return the original key otherwise returned the
//      // mapped key.
//      if (tkey == null) {
//        return source_key.getKey();
//      }
//      return tkey;
//
//    }
//
//  }


  private static final Key ZERO_KEY = new Key((short) 0, (int) 0, (long) 0);

}
