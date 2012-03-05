/**
 * com.mckoi.odb.ObjectDatabase  Aug 3, 2010
 *
 * Mckoi Database Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2012  Diehl and Associates, Inc.
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

package com.mckoi.odb;

import com.mckoi.data.*;
import com.mckoi.network.CommitFaultException;
import com.mckoi.network.ConsensusDDBConnection;
import com.mckoi.network.ConsensusProcessor;
import com.mckoi.network.DataAddress;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

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

  private final Object commit_lock = new Object();

  // ----- Plug in Information -----

  /**
   * The name of this processor, displayed in the administration user
   * interface.
   */
  @Override
  public String getName() {
    return "Mckoi Object Database";
  }

  /**
   * A description of this processor appropriate for display in the help
   * section of the user interface.
   */
  @Override
  public String getDescription() {
    return "An object data model that supports primitives for building " +
           "persistent graph structures.";
  }

  /**
   * {@inheritDoc}
   */
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

  /**
   * {@inheritDoc}
   */
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

  /**
   * {@inheritDoc}
   */
  @Override
  public DataAddress commit(ConsensusDDBConnection connection,
                            DataAddress proposal) throws CommitFaultException {

    synchronized (commit_lock) {

      // Turn the proposal into a proposed ODBTransaction object,
      KeyObjectTransaction t = connection.createTransaction(proposal);
      ODBTransactionImpl proposed_transaction =
                                    new ODBTransactionImpl(null, proposal, t);

      // The transaction log,
      ObjectLog object_log = proposed_transaction.getProposedObjectLog();
//      object_log.printDebug();

      // The base root proposal of this transaction log,
      DataAddress base_root = object_log.getBaseRoot();

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
                        new ODBTransactionImpl(null, current_root, current_t);

        // There are roots, so we need to go through a merge process and
        // create a new proposal and ensure it is consistent.

        // The merge process...
        // --------------------

//        object_log.printDebug();

//        int counter = 0;

        // Merge dictionary entries,

        Iterator<DictionaryEvent> di = object_log.getDictionaryAddIterator();
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

        HashMap<Key, String> resource_map = new HashMap();
        final String NEW_KEY  = "NK";
        final String SAME_KEY = "SK";

        Key max_key = ZERO_KEY;

//        counter = 0;

        // For each root,
        for (DataAddress root : roots) {
          KeyObjectTransaction rt = connection.createTransaction(root);
          ODBTransactionImpl root_transaction =
                                        new ODBTransactionImpl(null, root, rt);

          // The transaction log,
          ObjectLog root_log = root_transaction.getProposedObjectLog();

          // Get an iterator for resources allocated in the proposed
          // transaction,
          Iterator<KeyAllocation> pi = object_log.getKeyAllocIterator();

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
          current_transaction.updateResourceLookupManager(max_key);

          // Now we move all the resources created in the proposal over to the
          // current transaction.

          Iterator<KeyAllocation> pi = object_log.getKeyAllocIterator();

          // For each resource allocated in the proposal,
          while (pi.hasNext()) {
            KeyAllocation alloc = pi.next();

            Key src_key = alloc.getKey();
            Key dst_key;

            // If the key needs to be mapped to a new key,
            if (resource_map.get(src_key).equals(NEW_KEY)) {
              dst_key =
                 proposed_transaction.copyResourceAsNewKeyTo(current_transaction,
                        alloc.getRef(), src_key);
            }
            // Otherwise we copy under the same key,
            else {
              dst_key =
                 proposed_transaction.copyResourceTo(current_transaction,
                        alloc.getRef(), src_key);
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
                                        new ODBTransactionImpl(null, root, rt);

          // The transaction log,
          ObjectLog root_log = root_transaction.getProposedObjectLog();

          // Check for object change clashes,
          Iterator<ObjectChangeEvent> obi = object_log.getObjectChangeIterator();
          while (obi.hasNext()) {
            ObjectChangeEvent evt = obi.next();

            // Check there is no clashing change in the root log
            if (root_log.hasObjectChange(evt)) {
              // The object changed historically, so generate a commit fault,
              throw new CommitFaultException(MessageFormat.format(
                          "Object at reference {0} concurrently modified",
                          evt.getObjectReference()));
            }
//            ++counter;
          }

          // Check for data change clashes,
          Iterator<DataChangeEvent> dbi = object_log.getDataChangeIterator();
          while (dbi.hasNext()) {
            DataChangeEvent evt = dbi.next();

            // Check there is no clashing change in the root log
            if (root_log.hasDataChange(evt)) {
              // The data changed historically, so generate a commit fault,
              throw new CommitFaultException(MessageFormat.format(
                          "Data at reference {0} concurrently modified",
                          evt.getDataReference()));
            }
//            ++counter;
          }

        }

//        System.out.println("Object changes = " + (counter / roots.length));

        // Now every object construction and change in the proposal must be
        // copied to the current transaction.

        Iterator<ObjectChangeEvent> obi = object_log.getObjectChangeIterator();
        while (obi.hasNext()) {
          ObjectChangeEvent evt = obi.next();

          // Replay this change or construction
          current_transaction.replayObjectChange(proposed_transaction, evt);

        }

        // Copy every data change in the proposal to the current transaction.

        Iterator<DataChangeEvent> dbi = object_log.getDataChangeIterator();
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
        HashSet<Reference> list_builds = new HashSet();

        // For each root,
        for (DataAddress root : roots) {
          KeyObjectTransaction rt = connection.createTransaction(root);
          ODBTransactionImpl root_transaction =
                                       new ODBTransactionImpl(null, root, rt);

          // The transaction log,
          ObjectLog root_log = root_transaction.getProposedObjectLog();

          Iterator<ListChangeEvent> lci = object_log.getListChangeIterator();
          while (lci.hasNext()) {
            ListChangeEvent evt = lci.next();

            // Check there is no clashing change in the root log
            if (root_log.hasListChange(evt)) {
              list_builds.add(evt.getListReference());
            }
          }
        }

        // The lists that were not changed in the history
        Iterator<ListChangeEvent> lci = object_log.getListChangeIterator();
        while (lci.hasNext()) {
          ListChangeEvent evt = lci.next();
          // If the list is not in the list builds, then we can simply copy
          // it over,
          if (!list_builds.contains(evt.getListReference())) {
            // Copy the list and the log entries over from the proposed
            // transaction,
            proposed_transaction.copyListTo(
                                  current_transaction, evt.getListReference(),
                                  object_log);
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
                                        object_log.getListItemAddIterator();
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
                                        object_log.getListItemRemoveIterator();
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


//        // First, any resources allocated must be checked to ensure they don't
//        // clash with resource keys allocated in concurrent roots. If there are
//        // clashes, the key needs to be reallocated against a bucket of keys
//        // that are known to be fresh. Any constructed objects with clashed
//        // keys must be modified with the new allocation.
//
//        // This method creates an object that can be queried to determine the
//        // destination of any created resource.
//
//        ResourceMap resource_map = createResourceMap(connection, roots,
//                                              object_log, current_transaction);
//
//        // Copy all the objects that were constructed or changed from the
//        // proposal into the new transaction, and any resource associated with
//        // the object using the resource map to remap any resource keys.
//
//        copyModifiedObjectsTo(resource_map, object_log,
//                              proposed_transaction, current_transaction);
//
//        // Now 'current_transaction' will contain a copy of all the new and
//        // modified objects and resources from the proposal. Now we must
//        // merge the lists/indexes.
//        // This will commit fault if a list operation is not possible (for
//        // example, to delete a reference not in the list).
//
//        mergeModifiedListsTo(resource_map, object_log,
//                             proposed_transaction, current_transaction);

//        // The list operations
//        // -------------------
//
//        // This merge process is performed as follows;
//        // 1) Fetch the latest snapshot published (we will call LS)
//        // 2) Scan proposed log and create a list of reference lists that were
//        //    changed in the proposal (CRL), and a list of reference lists that
//        //    were removed (RRL).
//        // 3) Scan the historical logs, if a reference list was removed that is
//        //    in RRL then commit fault. If an item in CRL was changed in an
//        //    historical snapshot then add to a new list called MCRL.
//        // 4) Scan CRL, if the item is not in MCRL then do a full copy.
//        //    Otherwise, perform a merge operation.
//
//        // Reference lists that were changed,
//        ArrayList<Reference> crl = new ArrayList(30);
//        // Reference lists that were removed,
//        ArrayList<Reference> rrl = new ArrayList(7);
//
//        // Reference lists that need to be merged (changed both in the proposal
//        // and in a commit event since the base root),
//        ArrayList<Reference> mcrl = new ArrayList(30);
//
//        object_log.queryChangedLists(crl);
//        object_log.queryFreedLists(rrl);
//
//        int sz = roots.length;
//        int n = 0;
//        ObjectLog[] event_logs = new ObjectLog[sz];
//
//        // For each root since the base root of the proposal,
//        for (DataAddress root : roots) {
//          KeyObjectTransaction event_t = connection.createTransaction(root);
//          ODBTransaction event_transaction =
//                                  new ODBTransaction(null, proposal, event_t);
//
//          // The event's transaction log,
//          ObjectLog event_object_log =
//                                     event_transaction.getProposedObjectLog();
//
//          // Put into the event log array,
//          event_logs[n] = event_object_log;
//
//          // Inconsistent list free operation?
//          if (event_object_log.didListFreeOn(rrl)) {
//            // NOTE: Might be better just not to bother checking for this
//            //   fault, because the 'free' operation would happen during a
//            //   Garbage Collection like operation on the object data so a
//            //   double free should never happen unless multiple clients are
//            //   doing GC operations.
//            //   Silent double frees and leaving the snapshot consistent (the
//            //   list being gone regardless).
//            throw new CommitFaultException("Double list free");
//          }
//
//          // Find out the lists that changed in 'event_object_log' that are
//          // also in crl and put them in mcrl.
//          event_object_log.queryListChangeMerge(crl, mcrl);
//
//          ++n;
//        }
//
//        // Create a new transaction representing the current snapshot,
//        DataAddress current_root = connection.getCurrentSnapshot();
//        KeyObjectTransaction current_t =
//                                   connection.createTransaction(current_root);
//        ODBTransaction current_transaction =
//                            new ODBTransaction(null, current_root, current_t);
//
//        // Merge any dictionary additions
//        dictionaryMergeOperation(object_log, proposal, current_transaction);
//
//        // Merge all the object addition and changes to the new
//        // transaction.
//        Iterator<ObjectChangeEvent> objects_changed =
//                                          object_log.getObjectChangeIterator();
//        while (objects_changed.hasNext()) {
//          ObjectChangeEvent r = objects_changed.next();
//          // For each log in previous events,
//          for (ObjectLog event_log : event_logs) {
//            if (event_log.hasObjectChange(r)) {
//              // The object changed historically, so generate a commit fault,
//              throw new CommitFaultException(MessageFormat.format(
//                          "Object at reference {0} concurrently modified", r));
//            }
//          }
//          // This modification was not concurrently modified, so copy it to the
//          // current transaction,
//          objectCopyOperation(r, proposal, current_transaction);
//        }
//
//        // Perform the transactional list operations (and checks),
//
//        // For each list in crl
//        for (Reference list_ref : crl) {
//          // If the list is also in mcrl, then we have to merge it the slow way,
//          if (referenceListContains(mcrl, list_ref)) {
//            // Merge from 'proposal' to 'current_transaction'
//            listMergeOperation(list_ref, object_log,
//                               proposal, current_transaction);
//          }
//          // Otherwise, we can do a much faster copy,
//          else {
//            // Copy from 'proposal' to 'current_transaction'
//            listCopyOperation(list_ref, object_log,
//                              proposal, current_transaction);
//          }
//        }



        // Flush and publish the new proposal with the merged changes,
        DataAddress final_root = connection.flushTransaction(current_t);
        connection.publishToPath(final_root);

        // Done.
        return final_root;

      }

    }

  }



//  /**
//   * Remaps any keys that are discovered to clash.
//   */
//  private void remapClashedKeys(ConsensusDDBConnection connection,
//                   DataAddress[] roots, ObjectLog object_log,
//                   ODBTransaction current_transaction) {
//
//    // For each root,
//    for (DataAddress root : roots) {
//      KeyObjectTransaction t = connection.createTransaction(root);
//      ODBTransaction proposed_transaction = new ODBTransaction(null, root, t);
//
//      // The transaction log,
//      ObjectLog root_log = proposed_transaction.getProposedObjectLog();
//
//      // Get an iterator for resources allocated in the proposed transaction,
//      Iterator<KeyAllocation> pi = object_log.getKeyAllocIterator();
//
//      // For each resource allocated in the proposal,
//      while (pi.hasNext()) {
//        KeyAllocation alloc = pi.next();
//        Key src_key = alloc.getKey();
//        // Does the root log also allocate this?
//        if (root_log.hasKeyAllocated(src_key)) {
//          // If so, this is a clash condition, so allocate a new resource and
//          // add it to the map,
//          Key dst_key =
//                   current_transaction.allocateKeyForReference(alloc.getRef());
//
//          // Copy the content of the key to the fresh key,
//          current_transaction.remapReferenceTo(
//                                      alloc.getRef(), alloc.getKey(), dst_key);
//
//        }
//      }
//
//    }
//
//  }

//  /**
//   * Creates a ResourceMap object.
//   */
//  private ResourceMap createResourceMap(ConsensusDDBConnection connection,
//                   DataAddress[] roots, ObjectLog object_log,
//                   ODBTransaction current_transaction) {
//
//    ResourceMap map = new ResourceMap();
//
//    // For each root,
//    for (DataAddress root : roots) {
//      KeyObjectTransaction t = connection.createTransaction(root);
//      ODBTransaction proposed_transaction = new ODBTransaction(null, root, t);
//
//      // The transaction log,
//      ObjectLog root_log = proposed_transaction.getProposedObjectLog();
//
////      // Get an iterator for resources allocated in this log,
////      Iterator<ODBTransaction.ResourceKey> ri =
////                                         root_log.getResourceAllocIterator();
//      // Get an iterator for resources allocated in the proposed transaction,
//      Iterator<KeyAllocation> pi = object_log.getKeyAllocIterator();
//
//      // For each resource allocated in the proposal,
//      while (pi.hasNext()) {
//        KeyAllocation alloc = pi.next();
//        // Does the root log also allocate this?
//        if (root_log.hasKeyAllocated(alloc.getKey())) {
//          // If so, this is a clash condition, so allocate a new resource and
//          // add it to the map,
//          Key mapped_key =
//                   current_transaction.allocateKeyForReference(alloc.getRef());
//          map.putDestinationKey(alloc, mapped_key);
//        }
//      }
//
//    }
//
//    return map;
//
//  }



  // ---------- Inner classes ----------

  private static class ResourceMap {

    private final HashMap<KeyAllocation, Key> transfer_map;

    /**
     * Constructor.
     */
    ResourceMap() {
      transfer_map = new HashMap(45);
    }

    /**
     * Puts a re-mapped key into this map.
     */
    void putDestinationKey(KeyAllocation src, Key dst) {

      transfer_map.put(src, dst);

    }

    /**
     * Given a key in the source transaction (the proposed transaction),
     * returns a key that is unique within the created transaction.
     */
    Key getDestinationKey(final KeyAllocation source_key) {

      // If the source key is in the transfer map,
      Key tkey = transfer_map.get(source_key);
      // If not in the map, return the original key otherwise returned the
      // mapped key.
      if (tkey == null) {
        return source_key.getKey();
      }
      return tkey;

    }

  }


  private static final Key ZERO_KEY = new Key((short) 0, (int) 0, (long) 0);

}
