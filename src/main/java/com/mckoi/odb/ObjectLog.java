/**
 * com.mckoi.odb.ObjectLog  Nov 1, 2010
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

package com.mckoi.odb;

import com.mckoi.data.ByteArray;
import com.mckoi.data.DataFile;
import com.mckoi.data.Integer128Bit;
import com.mckoi.data.JavaByteArray;
import com.mckoi.data.Key;
import com.mckoi.data.KeyObjectTransaction;
import com.mckoi.data.OrderedSetData;
import com.mckoi.data.PropertySet;
import com.mckoi.network.DataAddress;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A structure that maps over a DataFile and stores a serial log of changes
 * made to objects and lists in the object database during a transaction
 * session.  This log is used to determine if the operations that happen
 * during a transaction can be considered consistent with the model. It is
 * also used in the MVCC merge process during a commit.
 *
 * @author Tobias Downer
 */

class ObjectLog {

  /**
   * The backed transaction.
   */
  private final KeyObjectTransaction transaction;
  private final boolean use_existing;

  private OrderedSetData sorted_log;




  /**
   * Constructor.
   */
  ObjectLog(KeyObjectTransaction transaction, boolean use_existing) {
    this.transaction = transaction;
    this.use_existing = use_existing;
  }

  /**
   * Returns the OrderedSetData object.
   */
  private OrderedSetData getSortedLog() {
    // Ensure the 'sorted_log' member is set,
    if (sorted_log == null) {
      // We need to create the sorted_log entry,
      DataFile log_file =
              transaction.getDataFile(ODBTransaction.TRANSACTION_LOG_KEY, 'w');
      if (!use_existing) {
        // If we are not using an existing log, we need to clear any existing
        // entries.
        log_file.delete();
      }
      sorted_log = new OrderedSetData(log_file, LOG_COLLATOR);
    }

    return sorted_log;
  }

  /**
   * Returns an Integer128Bit object that is the encoded form of a Key.
   */
  private Integer128Bit keyAsWord(Key key) {
    long v1 = (((long) key.getType()) & 0x0FFFFFFFFL) << 32;
    v1 |= (((long) key.getSecondary()) & 0x0FFFFFFFFL);
    long v2 = key.getPrimary();

    return new Integer128Bit(v1, v2);
  }


  /**
   * Generic function that adds an entry to the log.
   */
  void log(byte code, Integer128Bit... params) {

    ByteArrayOutputStream bout = new ByteArrayOutputStream(49);
    DataOutputStream dout = new DataOutputStream(bout);
    try {
      dout.writeByte(code);
      for (Integer128Bit p : params) {
        dout.writeLong(p.getHighLong());
        dout.writeLong(p.getLowLong());
      }
      getSortedLog().add(new JavaByteArray(bout.toByteArray()));
    }
    catch (IOException e) {
      // Shouldn't be possible,
      throw new RuntimeException(e);
    }
  }



//  /**
//   * Adds a log entry for the command that constructs a new object.
//   */
//  void logAllocateObject(Reference object_class, Reference ob_ref) {
//    log((byte) 0x002, object_class, ob_ref);
//  }

  /**
   * Adds a log entry for the command that frees an object (called by GC).
   */
  void logFreeObject(Reference object_class, Reference ob_ref) {
    log((byte) 0x003, object_class, ob_ref);
  }


  /**
   * Adds a log entry for the allocation of a new resource on the given key.
   */
  void logAllocateResource(Key resource_key, Reference resource_ref) {
    log((byte) 0x004, keyAsWord(resource_key), resource_ref);
  }

  /**
   * Adds a log entry for the allocation of a new list on the given key.
   */
  void logAllocateList(Key list_key, Reference list_ref) {
    log((byte) 0x004, keyAsWord(list_key), list_ref);
  }

  /**
   * Adds a log entry for the allocation of a new class bucket on the
   * given key.
   */
  void logAllocateClassBucket(Key bucket_key, Reference bucket_ref) {
    log((byte) 0x004, keyAsWord(bucket_key), bucket_ref);
  }



  /**
   * Adds a log entry for a data object change.
   */
  void logDataChange(Reference resource_ref) {
    log((byte) 0x007, resource_ref);
  }



//  /**
//   * Adds a log entry for the allocation of a new resource.
//   */
//  void logAllocateResource(Reference resource_ref) {
//    log((byte) 0x004, resource_ref);
//  }

//  /**
//   * Adds a log entry for the command that creates a new list structure in an
//   * object.
//   */
//  void logAllocList(ODBClass list_class,
//                    ODBTransaction.ResourceKey list_key) {
//
//    log((byte) 0x005, list_key, list_class.getReference());
//
//  }
//
//  /**
//   * Adds a log entry for the command that removes an existing list structure.
//   */
//  void logFreeList(ODBClass list_class,
//                   ODBTransaction.ResourceKey list_key) {
//
//    log((byte) 0x006, list_key, list_class.getReference());
//
//  }

  /**
   * Adds a list addition operation to the log.
   */
  void logListAddition(Reference list_reference, Reference object_added,
                       Reference list_class_ref) {

    log((byte) 0x009, list_reference, object_added, list_class_ref);
    // Log a list change event for this list,
    if (!hasListChange(new ListChangeEvent(list_reference))) {
      log((byte) 0x00b, list_reference);
    }

  }

  /**
   * Adds a list remove operation to the log.
   */
  void logListRemoval(Reference list_reference, Reference object_removed,
                      Reference list_class_ref) {

    log((byte) 0x00a, list_reference, object_removed, list_class_ref);
    // Log a list change event for this list,
    if (!hasListChange(new ListChangeEvent(list_reference))) {
      log((byte) 0x00b, list_reference);
    }

  }

  /**
   * Notes an object modification operation in the log.
   */
  void logObjectChange(Reference object_class, Reference object_modified) {

    // Check the entry doesn't already exist,
    if (!hasObjectChange(
                       new ObjectChangeEvent(object_class, object_modified))) {
      log((byte) 0x00f, object_class, object_modified);
    }

  }

  /**
   * Notes a dictionary addition operation in the log.
   */
  void logDictionaryAddition(Reference dictionary_ref) {

    log((byte) 0x013, dictionary_ref);

  }



//  /**
//   * Find all lists that were changed in this log and add the unique set of
//   * references to the given list. The references will be populated in the
//   * array list in sorted order.
//   */
//  void queryChangedLists(ArrayList<Reference> ref) {
//
//  }




  /**
   * Returns true if the given key has been allocated to be used. This is used
   * to discover key clashes.
   */
  boolean hasKeyAllocated(Key key) {

    try {

      Integer128Bit key_val = keyAsWord(key);

      ByteArrayOutputStream bout = new ByteArrayOutputStream(36);
      DataOutputStream dout = new DataOutputStream(bout);
      dout.writeByte(0x004);
      dout.writeLong(key_val.getHighLong());
      dout.writeLong(key_val.getLowLong());

      JavaByteArray evt_elem = new JavaByteArray(bout.toByteArray());

      // Look at the first in the tail set,
      OrderedSetData tail = getSortedLog().tailSet(evt_elem);

      // If the query is not empty,
      if (!tail.isEmpty()) {
        ByteArray barr = tail.first();
        DataInputStream din = barr.getDataInputStream();
        // Read the item (code, ref high, ref low)
        byte c = din.readByte();
        if (c == 0x004) {
          long rrh = din.readLong();
          long rrl = din.readLong();
          Integer128Bit rrk = new Integer128Bit(rrh, rrl);

          // If the references are equal, return true (found)
          if (rrk.equals(key_val)) {
            return true;
          }
        }
      }

      // Not found so return false,
      return false;

    }
    catch (IOException e) {
      // Shouldn't be possible
      throw new RuntimeException(e);
    }

  }



  private void copyEntries(byte entry_code,
               Reference list_ref, ObjectLog destination) throws IOException {

    // The list addition or removal codes to copy,
    ByteArrayOutputStream bout = new ByteArrayOutputStream(50);
    DataOutputStream dout = new DataOutputStream(bout);
    dout.writeByte(entry_code);
    dout.writeLong(list_ref.getHighLong());
    dout.writeLong(list_ref.getLowLong());
    dout.writeLong(0);
    dout.writeLong(0);
    dout.writeLong(0);
    dout.writeLong(0);

    JavaByteArray evt_elem = new JavaByteArray(bout.toByteArray());
    final OrderedSetData tail = getSortedLog().tailSet(evt_elem);

    // If the tail set is not empty,
    if (!tail.isEmpty()) {

      // Get the first item in the tail set and see if it's equal,
      ByteArray code = tail.first();
      DataInputStream din = code.getDataInputStream();
      byte c = din.readByte();
      if (c == entry_code) {
        long crh = din.readLong();
        long crl = din.readLong();
        if (crh == list_ref.getHighLong() &&
            crl == list_ref.getLowLong()) {

          long v1h = din.readLong();
          long v1l = din.readLong();
          long v2h = din.readLong();
          long v2l = din.readLong();

          // Log the list entry,
          destination.log((byte) entry_code, list_ref,
                          new Reference(v1h, v1l), new Reference(v2h, v2l));
          // Log a list change event for this list,
          if (!destination.hasListChange(new ListChangeEvent(list_ref))) {
            destination.log((byte) 0x00b, list_ref);
          }
        }
      }
    }
  }

  /**
   * Copies all the list operations on the given list to the destination
   * log.
   */
  void copyAllListOperationsTo(Reference list_ref, ObjectLog destination) {

    try {

      // The code for list additions.
      copyEntries((byte) 0x009, list_ref, destination);
      // The code for list removals,
      copyEntries((byte) 0x00a, list_ref, destination);

    }
    catch (IOException e) {
      // Shouldn't be possible
      throw new RuntimeException(e);
    }

  }




//  private boolean didListFreeOn(Reference ref) {
//    try {
//
//      ByteArrayOutputStream bout = new ByteArrayOutputStream(36);
//      DataOutputStream dout = new DataOutputStream(bout);
//      dout.writeByte(0x006);
//      dout.writeLong(ref.getHighLong());
//      dout.writeLong(ref.getLowLong());
//
//      JavaByteArray evt_elem = new JavaByteArray(bout.toByteArray());
//
//      // Look at the first in the tail set,
//      OrderedSetData tail = getSortedLog().tailSet(evt_elem);
//
//      // If the query is not empty,
//      if (!tail.isEmpty()) {
//        ByteArray barr = tail.first();
//        DataInputStream din = barr.getDataInputStream();
//        // Read the item (code, ref high, ref low)
//        byte c = din.readByte();
//        if (c == 0x006) {
//          long rrh = din.readLong();
//          long rrl = din.readLong();
//          Reference rr_ref = new Reference(rrh, rrl);
//
//          // If the references are equal, return true (found)
//          if (rr_ref.equals(ref)) {
//            return true;
//          }
//        }
//      }
//
//      // Not found so return false,
//      return false;
//
//    }
//    catch (IOException e) {
//      // Shouldn't be possible
//      throw new RuntimeException(e);
//    }
//  }
//
//  /**
//   * Returns true if this log contains a list free operation on any one of
//   * the given list of list references.
//   */
//  boolean didListFreeOn(ArrayList<Reference> refs) {
//
//    // Trivial case when the refs list is empty,
//    if (refs.isEmpty()) {
//      return false;
//    }
//
//    // Check that there are 'list free' references in the log. We can
//    // quickly skip if none found,
//
//    try {
//
//      ByteArrayOutputStream bout = new ByteArrayOutputStream(36);
//      DataOutputStream dout = new DataOutputStream(bout);
//      dout.writeByte(0x006);
//      dout.writeLong(0);
//      dout.writeLong(0);
//
//      JavaByteArray evt_elem = new JavaByteArray(bout.toByteArray());
//
//      // Look at the first in the tail set,
//      OrderedSetData tail = getSortedLog().tailSet(evt_elem);
//
//      // If the query is not empty,
//      if (!tail.isEmpty()) {
//        ByteArray barr = tail.first();
//        DataInputStream din = barr.getDataInputStream();
//        byte c = din.readByte();
//        if (c == 0x006) {
//
//          // Yes there are list free operations in this log, so process
//          // the ref list,
//          for (Reference r : refs) {
//            if (didListFreeOn(r)) {
//              return true;
//            }
//          }
//        }
//      }
//
//      return false;
//
//    }
//    catch (IOException e) {
//      // Shouldn't be possible
//      throw new RuntimeException(e);
//    }
//
//  }



  /**
   * Returns true if the given data object was changed in this log. This is
   * used to discover object change clashes.
   */
  boolean hasDataChange(DataChangeEvent evt) {

    Reference data_ref = evt.getDataReference();

    try {

      ByteArrayOutputStream bout = new ByteArrayOutputStream(36);
      DataOutputStream dout = new DataOutputStream(bout);
      dout.writeByte(0x007);
      dout.writeLong(data_ref.getHighLong());
      dout.writeLong(data_ref.getLowLong());

      JavaByteArray evt_elem = new JavaByteArray(bout.toByteArray());

      // Look at the first in the tail set,
      OrderedSetData tail = getSortedLog().tailSet(evt_elem);

      // If the tail set is not empty,
      if (!tail.isEmpty()) {

        // Get the first item in the tail set and see if it's equal,
        ByteArray code = tail.first();
        DataInputStream din = code.getDataInputStream();
        byte c = din.readByte();
        if (c == 0x007) {
          long crh = din.readLong();
          long crl = din.readLong();
          if (crh == data_ref.getHighLong() &&
              crl == data_ref.getLowLong()) {
            return true;
          }
        }

      }

      // Not found,
      return false;

    }
    catch (IOException e) {
      // Shouldn't be possible
      throw new RuntimeException(e);
    }

  }


  /**
   * Returns true if this log contains an object change event on the same
   * object instance, false otherwise.
   */
  boolean hasObjectChange(ObjectChangeEvent evt) {
    Reference class_ref = evt.getClassReference();
    Reference object_ref = evt.getObjectReference();

    try {

      ByteArrayOutputStream bout = new ByteArrayOutputStream(36);
      DataOutputStream dout = new DataOutputStream(bout);
      dout.writeByte(0x00f);
      dout.writeLong(class_ref.getHighLong());
      dout.writeLong(class_ref.getLowLong());
      dout.writeLong(object_ref.getHighLong());
      dout.writeLong(object_ref.getLowLong());

      JavaByteArray evt_elem = new JavaByteArray(bout.toByteArray());

      // Look at the first in the tail set,
      OrderedSetData tail = getSortedLog().tailSet(evt_elem);

      // If the tail set is not empty,
      if (!tail.isEmpty()) {

        // Get the first item in the tail set and see if it's equal,
        ByteArray code = tail.first();
        DataInputStream din = code.getDataInputStream();
        byte c = din.readByte();
        if (c == 0x00f) {
          long crh = din.readLong();
          long crl = din.readLong();
          if (crh == class_ref.getHighLong() &&
              crl == class_ref.getLowLong()) {
            long orh = din.readLong();
            long orl = din.readLong();
            if (orh == object_ref.getHighLong() &&
                orl == object_ref.getLowLong()) {
              return true;
            }
          }
        }

      }

      // Not found,
      return false;

    }
    catch (IOException e) {
      // Shouldn't be possible
      throw new RuntimeException(e);
    }

  }

  /**
   * Returns true if this log contains a list change event on the same
   * list instance, false otherwise.
   */
  boolean hasListChange(ListChangeEvent evt) {
    Reference list_ref = evt.getListReference();

    try {

      ByteArrayOutputStream bout = new ByteArrayOutputStream(36);
      DataOutputStream dout = new DataOutputStream(bout);
      dout.writeByte(0x00b);
      dout.writeLong(list_ref.getHighLong());
      dout.writeLong(list_ref.getLowLong());

      JavaByteArray evt_elem = new JavaByteArray(bout.toByteArray());

      // Look at the first in the tail set,
      OrderedSetData tail = getSortedLog().tailSet(evt_elem);

      // If the tail set is not empty,
      if (!tail.isEmpty()) {

        // Get the first item in the tail set and see if it's equal,
        ByteArray code = tail.first();
        DataInputStream din = code.getDataInputStream();
        byte c = din.readByte();
        if (c == 0x00b) {
          long crh = din.readLong();
          long crl = din.readLong();
          if (crh == list_ref.getHighLong() &&
              crl == list_ref.getLowLong()) {
            return true;
          }
        }

      }

      // Not found,
      return false;

    }
    catch (IOException e) {
      // Shouldn't be possible
      throw new RuntimeException(e);
    }

  }

  /**
   * Returns true if the log contains a remove event on the given list with
   * the given object.
   */
  boolean hasListRemove(Reference list_ref, Reference object_ref) {

    try {

      ByteArrayOutputStream bout = new ByteArrayOutputStream(50);
      DataOutputStream dout = new DataOutputStream(bout);
      dout.writeByte(0x00a);
      dout.writeLong(list_ref.getHighLong());
      dout.writeLong(list_ref.getLowLong());
      dout.writeLong(object_ref.getHighLong());
      dout.writeLong(object_ref.getLowLong());
      dout.writeLong(0);
      dout.writeLong(0);

      JavaByteArray evt_elem = new JavaByteArray(bout.toByteArray());

      // Look at the first in the tail set,
      OrderedSetData tail = getSortedLog().tailSet(evt_elem);

      // If the tail set is not empty,
      if (!tail.isEmpty()) {

        // Get the first item in the tail set and see if it's equal,
        ByteArray code = tail.first();
        DataInputStream din = code.getDataInputStream();
        byte c = din.readByte();
        if (c == 0x00a) {
          // Check the list reference is equal,
          long crh = din.readLong();
          long crl = din.readLong();
          if (crh == list_ref.getHighLong() &&
              crl == list_ref.getLowLong()) {
            // Check the object reference is equal
            long orh = din.readLong();
            long orl = din.readLong();
            if (orh == object_ref.getHighLong() &&
                orl == object_ref.getLowLong()) {
              // Ok, the entry is found in the log,
              return true;
            }
          }
        }

      }

      // Not found,
      return false;

    }
    catch (IOException e) {
      // Shouldn't be possible
      throw new RuntimeException(e);
    }

  }




  /**
   * Flushes the log out in the proposal format.
   */
  void flush(DataAddress base_root) {
    // Update the transaction properties,
    DataFile dfile = transaction.getDataFile(
                              ODBTransaction.TRANSACTION_PROPERTIES_KEY, 'w');
    PropertySet pset = new PropertySet(dfile);
    if (base_root == null) {
      pset.setProperty("B", null);
    }
    else {
      pset.setProperty("B", base_root.formatString());
    }
  }


  /**
   * Returns the DataAddress that is the base root for this transaction
   * log.
   */
  DataAddress getBaseRoot() {
    DataFile dfile = transaction.getDataFile(
            ODBTransaction.TRANSACTION_PROPERTIES_KEY, 'w');
    PropertySet pset = new PropertySet(dfile);
    String base_root_str = pset.getProperty("B");
    if (base_root_str == null) {
      return null;
    }
    else {
      return DataAddress.parseString(base_root_str);
    }
  }



  void printDebug() {

    System.out.println("ObjectLog size = " + transaction.getDataFile(
                             ODBTransaction.TRANSACTION_LOG_KEY, 'r').size());
    System.out.println("ObjectLog entries = " + getSortedLog().size());

    final OrderedSetData alloc_set = getSortedLog();
    Iterator<ByteArray> i = alloc_set.iterator();
    while (i.hasNext()) {
      System.out.println(i.next());
    }



  }



  Iterator<KeyAllocation> getKeyAllocIterator() {
    final OrderedSetData alloc_set = getSortedLog().tailSet(RESOURCE_ALLOC_S);
    final Iterator<ByteArray> alloc_set_iterator = alloc_set.iterator();
    return new Iterator<KeyAllocation>() {
      private KeyAllocation cur_resource_ref = null;
      private boolean end_reached = false;
      public boolean hasNext() {
        if (!alloc_set_iterator.hasNext()) {
          return false;
        }
        if (cur_resource_ref == null) {
          if (end_reached) {
            return false;
          }
          try {
            ByteArray barr = alloc_set_iterator.next();
            DataInputStream din = barr.getDataInputStream();
            byte c = din.readByte();
            if (c != 0x004) {
              end_reached = true;
              return false;
            }
            long keyh = din.readLong();
            long keyl = din.readLong();
            long refh = din.readLong();
            long refl = din.readLong();

            // Covert to Key
            short k1 = (short) (keyh >> 32);
            int k2 = (int) (keyh & 0x0FFFFFFFF);
            long k3 = keyl;
            Key key = new Key(k1, k2, k3);
            // Convert to Reference
            Reference ref = new Reference(refh, refl);

            cur_resource_ref = new KeyAllocation(key, ref);
          }
          catch (IOException e) {
            // Should never happen,
            throw new RuntimeException();
          }
        }
        return true;
      }
      public KeyAllocation next() {
        if (cur_resource_ref != null || hasNext()) {
          KeyAllocation ka = cur_resource_ref;
          cur_resource_ref = null;
          return ka;
        }
        throw new NoSuchElementException();
      }
      public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
  }



  Iterator<DictionaryEvent> getDictionaryAddIterator() {
    final OrderedSetData change_set = getSortedLog().tailSet(DICTIONARY_ADD_S);
    final Iterator<ByteArray> change_set_iterator = change_set.iterator();
    return new Iterator<DictionaryEvent>() {
      private Reference dictionary_ref = null;
      private boolean end_reached = false;
      public boolean hasNext() {
        if (!change_set_iterator.hasNext()) {
          return false;
        }
        if (dictionary_ref == null) {
          if (end_reached) {
            return false;
          }
          try {
            ByteArray barr = change_set_iterator.next();
            DataInputStream din = barr.getDataInputStream();
            byte c = din.readByte();
            if (c != 0x013) {
              end_reached = true;
              return false;
            }
            long drh = din.readLong();
            long drl = din.readLong();
            dictionary_ref = new Reference(drh, drl);
          }
          catch (IOException e) {
            // Should never happen,
            throw new RuntimeException();
          }
        }
        return true;
      }
      public DictionaryEvent next() {
        if (dictionary_ref != null || hasNext()) {
          DictionaryEvent evt = new DictionaryEvent(dictionary_ref);
          dictionary_ref = null;
          return evt;
        }
        throw new NoSuchElementException();
      }
      public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
  }



  Iterator<ObjectChangeEvent> getObjectChangeIterator() {
    final OrderedSetData change_set = getSortedLog().tailSet(OBJECT_CHANGE_S);
    final Iterator<ByteArray> change_set_iterator = change_set.iterator();
    return new Iterator<ObjectChangeEvent>() {
      private Reference cur_class_ref = null;
      private Reference cur_obj_ref = null;
      private boolean end_reached = false;
      public boolean hasNext() {
        if (!change_set_iterator.hasNext()) {
          return false;
        }
        if (cur_class_ref == null) {
          if (end_reached) {
            return false;
          }
          try {
            ByteArray barr = change_set_iterator.next();
            DataInputStream din = barr.getDataInputStream();
            byte c = din.readByte();
            if (c != 0x00f) {
              end_reached = true;
              return false;
            }
            long crh = din.readLong();
            long crl = din.readLong();
            long orh = din.readLong();
            long orl = din.readLong();
            cur_class_ref = new Reference(crh, crl);
            cur_obj_ref = new Reference(orh, orl);
          }
          catch (IOException e) {
            // Should never happen,
            throw new RuntimeException();
          }
        }
        return true;
      }
      public ObjectChangeEvent next() {
        if (cur_class_ref != null || hasNext()) {
          ObjectChangeEvent evt =
                             new ObjectChangeEvent(cur_class_ref, cur_obj_ref);
          cur_class_ref = null;
          cur_obj_ref = null;
          return evt;
        }
        throw new NoSuchElementException();
      }
      public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
  }



  Iterator<ListItemChangeEvent> getListItemAddIterator() {
    final OrderedSetData change_set = getSortedLog().tailSet(LIST_ITEM_ADD_S);
    final Iterator<ByteArray> change_set_iterator = change_set.iterator();
    return new Iterator<ListItemChangeEvent>() {
      private Reference cur_list_ref = null;
      private Reference cur_obj_ref = null;
      private Reference cur_class_ref = null;
      private boolean end_reached = false;
      public boolean hasNext() {
        if (!change_set_iterator.hasNext()) {
          return false;
        }
        if (cur_list_ref == null) {
          if (end_reached) {
            return false;
          }
          try {
            ByteArray barr = change_set_iterator.next();
            DataInputStream din = barr.getDataInputStream();
            byte c = din.readByte();
            if (c != 0x009) {
              end_reached = true;
              return false;
            }
            long lrh = din.readLong();
            long lrl = din.readLong();
            long orh = din.readLong();
            long orl = din.readLong();
            long crh = din.readLong();
            long crl = din.readLong();
            cur_list_ref = new Reference(lrh, lrl);
            cur_obj_ref = new Reference(orh, orl);
            cur_class_ref = new Reference(crh, crl);
          }
          catch (IOException e) {
            // Should never happen,
            throw new RuntimeException();
          }
        }
        return true;
      }
      public ListItemChangeEvent next() {
        if (cur_list_ref != null || hasNext()) {
          ListItemChangeEvent evt = new ListItemChangeEvent(
                                     cur_list_ref, cur_obj_ref, cur_class_ref);
          cur_list_ref = null;
          cur_obj_ref = null;
          cur_class_ref = null;
          return evt;
        }
        throw new NoSuchElementException();
      }
      public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
  }



  Iterator<ListItemChangeEvent> getListItemRemoveIterator() {
    final OrderedSetData change_set = getSortedLog().tailSet(LIST_ITEM_REMOVE_S);
    final Iterator<ByteArray> change_set_iterator = change_set.iterator();
    return new Iterator<ListItemChangeEvent>() {
      private Reference cur_list_ref = null;
      private Reference cur_obj_ref = null;
      private Reference cur_class_ref = null;
      private boolean end_reached = false;
      public boolean hasNext() {
        if (!change_set_iterator.hasNext()) {
          return false;
        }
        if (cur_list_ref == null) {
          if (end_reached) {
            return false;
          }
          try {
            ByteArray barr = change_set_iterator.next();
            DataInputStream din = barr.getDataInputStream();
            byte c = din.readByte();
            if (c != 0x00a) {
              end_reached = true;
              return false;
            }
            long lrh = din.readLong();
            long lrl = din.readLong();
            long orh = din.readLong();
            long orl = din.readLong();
            long crh = din.readLong();
            long crl = din.readLong();
            cur_list_ref = new Reference(lrh, lrl);
            cur_obj_ref = new Reference(orh, orl);
            cur_class_ref = new Reference(crh, crl);
          }
          catch (IOException e) {
            // Should never happen,
            throw new RuntimeException();
          }
        }
        return true;
      }
      public ListItemChangeEvent next() {
        if (cur_list_ref != null || hasNext()) {
          ListItemChangeEvent evt = new ListItemChangeEvent(
                                     cur_list_ref, cur_obj_ref, cur_class_ref);
          cur_list_ref = null;
          cur_obj_ref = null;
          cur_class_ref = null;
          return evt;
        }
        throw new NoSuchElementException();
      }
      public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
  }


  Iterator<ListChangeEvent> getListChangeIterator() {
    final OrderedSetData change_set = getSortedLog().tailSet(LIST_CHANGE_S);
    final Iterator<ByteArray> change_set_iterator = change_set.iterator();
    return new Iterator<ListChangeEvent>() {
      private Reference list_ref = null;
      private boolean end_reached = false;
      public boolean hasNext() {
        if (!change_set_iterator.hasNext()) {
          return false;
        }
        if (list_ref == null) {
          if (end_reached) {
            return false;
          }
          try {
            ByteArray barr = change_set_iterator.next();
            DataInputStream din = barr.getDataInputStream();
            byte c = din.readByte();
            if (c != 0x00b) {
              end_reached = true;
              return false;
            }
            long drh = din.readLong();
            long drl = din.readLong();
            list_ref = new Reference(drh, drl);
          }
          catch (IOException e) {
            // Should never happen,
            throw new RuntimeException();
          }
        }
        return true;
      }
      public ListChangeEvent next() {
        if (list_ref != null || hasNext()) {
          ListChangeEvent evt = new ListChangeEvent(list_ref);
          list_ref = null;
          return evt;
        }
        throw new NoSuchElementException();
      }
      public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
  }


  Iterator<DataChangeEvent> getDataChangeIterator() {
    final OrderedSetData change_set = getSortedLog().tailSet(DATA_CHANGE_S);
    final Iterator<ByteArray> change_set_iterator = change_set.iterator();
    return new Iterator<DataChangeEvent>() {
      private Reference data_ref = null;
      private boolean end_reached = false;
      public boolean hasNext() {
        if (!change_set_iterator.hasNext()) {
          return false;
        }
        if (data_ref == null) {
          if (end_reached) {
            return false;
          }
          try {
            ByteArray barr = change_set_iterator.next();
            DataInputStream din = barr.getDataInputStream();
            byte c = din.readByte();
            if (c != 0x007) {
              end_reached = true;
              return false;
            }
            long drh = din.readLong();
            long drl = din.readLong();
            data_ref = new Reference(drh, drl);
          }
          catch (IOException e) {
            // Should never happen,
            throw new RuntimeException();
          }
        }
        return true;
      }
      public DataChangeEvent next() {
        if (data_ref != null || hasNext()) {
          DataChangeEvent evt = new DataChangeEvent(data_ref);
          data_ref = null;
          return evt;
        }
        throw new NoSuchElementException();
      }
      public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
  }





  private static Integer128Bit readInt128(DataInputStream in)
                                                          throws IOException {

    long vh = in.readLong();
    long vl = in.readLong();
    return new Integer128Bit(vh, vl);

  }

  private static final Comparator<ByteArray> LOG_COLLATOR =
                                              new Comparator<ByteArray>() {

    public int compare(ByteArray o1, ByteArray o2) {

      DataInputStream din1 = o1.getDataInputStream();
      DataInputStream din2 = o2.getDataInputStream();

      try {
        final byte c1 = din1.readByte();
        final byte c2 = din2.readByte();

        if (c1 < c2) {
          return -1;
        }
        else if (c1 > c2) {
          return 1;
        }
        else {
          // Compare the none primary components of the entry,

          // object construct/free and change, and key/reference assignment,
          if (c1 == 0x002 || c1 == 0x003 || c1 == 0x004 || c1 == 0x00f) {
            Integer128Bit l1 = readInt128(din1);
            Integer128Bit l2 = readInt128(din2);
            int comp1 = l1.compareTo(l2);
            if (comp1 != 0) {
              return comp1;
            }
            Integer128Bit ob1 = readInt128(din1);
            Integer128Bit ob2 = readInt128(din2);
            return ob1.compareTo(ob2);
          }
          // list addition and list removal,
          else if (c1 == 0x009 || c1 == 0x00a) {
            Integer128Bit l1 = readInt128(din1);
            Integer128Bit l2 = readInt128(din2);
            int comp1 = l1.compareTo(l2);
            if (comp1 != 0) {
              return comp1;
            }
            Integer128Bit ob1 = readInt128(din1);
            Integer128Bit ob2 = readInt128(din2);
            return ob1.compareTo(ob2);
          }
          // object change, list change and dictionary addition,
          else if (c1 == 0x007 || c1 == 0x00b || c1 == 0x013) {
            Integer128Bit r1 = readInt128(din1);
            Integer128Bit r2 = readInt128(din2);
            return r1.compareTo(r2);
          }

          else {
            throw new RuntimeException("Unknown log entry code");
          }

        }

      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }

    }

  };

  private static final JavaByteArray OBJECT_CHANGE_S;
  private static final JavaByteArray RESOURCE_ALLOC_S;

  private static final JavaByteArray LIST_ITEM_ADD_S;
  private static final JavaByteArray LIST_ITEM_REMOVE_S;
  private static final JavaByteArray LIST_CHANGE_S;

  private static final JavaByteArray DATA_CHANGE_S;

  private static final JavaByteArray DICTIONARY_ADD_S;

  static {
    ByteArrayOutputStream bout1 = new ByteArrayOutputStream();
    DataOutputStream dout1 = new DataOutputStream(bout1);
    ByteArrayOutputStream bout2 = new ByteArrayOutputStream();
    DataOutputStream dout2 = new DataOutputStream(bout2);
    ByteArrayOutputStream bout3 = new ByteArrayOutputStream();
    DataOutputStream dout3 = new DataOutputStream(bout3);
    ByteArrayOutputStream bout4 = new ByteArrayOutputStream();
    DataOutputStream dout4 = new DataOutputStream(bout4);
    ByteArrayOutputStream bout5 = new ByteArrayOutputStream();
    DataOutputStream dout5 = new DataOutputStream(bout5);
    ByteArrayOutputStream bout6 = new ByteArrayOutputStream();
    DataOutputStream dout6 = new DataOutputStream(bout6);
    ByteArrayOutputStream bout7 = new ByteArrayOutputStream();
    DataOutputStream dout7 = new DataOutputStream(bout7);
    try {

      dout1.writeByte((byte) 0x00f);
      dout1.writeLong(0);
      dout1.writeLong(0);
      dout1.writeLong(0);
      dout1.writeLong(0);
      OBJECT_CHANGE_S = new JavaByteArray(bout1.toByteArray());

      dout2.writeByte((byte) 0x004);
      dout2.writeLong(0);
      dout2.writeLong(0);
      RESOURCE_ALLOC_S = new JavaByteArray(bout2.toByteArray());

      dout3.writeByte((byte) 0x009);
      dout3.writeLong(0);
      dout3.writeLong(0);
      dout3.writeLong(0);
      dout3.writeLong(0);
      LIST_ITEM_ADD_S = new JavaByteArray(bout3.toByteArray());

      dout4.writeByte((byte) 0x00a);
      dout4.writeLong(0);
      dout4.writeLong(0);
      dout4.writeLong(0);
      dout4.writeLong(0);
      LIST_ITEM_REMOVE_S = new JavaByteArray(bout4.toByteArray());

      dout5.writeByte((byte) 0x00b);
      dout5.writeLong(0);
      dout5.writeLong(0);
      LIST_CHANGE_S = new JavaByteArray(bout5.toByteArray());

      dout6.writeByte((byte) 0x013);
      dout6.writeLong(0);
      dout6.writeLong(0);
      DICTIONARY_ADD_S = new JavaByteArray(bout6.toByteArray());

      dout7.writeByte((byte) 0x007);
      dout7.writeLong(0);
      dout7.writeLong(0);
      DATA_CHANGE_S = new JavaByteArray(bout7.toByteArray());

    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
