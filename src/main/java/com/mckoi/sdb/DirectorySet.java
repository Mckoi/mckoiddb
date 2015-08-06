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

package com.mckoi.sdb;

import com.mckoi.data.*;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * A structure that manages a directory set (a name space of strings mapped to
 * a unique 64-bit identifier). This structure assumes that the referenced
 * items can self resolve their name. The DirectorySet implements a
 * PropertySet for fast lookup of named items, and an ordered index of ids
 * for fast cursor traversal and element count calculation.
 *
 * @author Tobias Downer
 */

class DirectorySet {

  /**
   * The backed transaction.
   */
  private final KeyObjectTransaction transaction;

  /**
   * The Key of the PropertySet object that contains information about the
   * structure.
   */
  private final Key directory_properties_key;

  /**
   * The Key of the PropertySet.
   */
  private final Key property_set_key;

  /**
   * The key of the OrderedSet64Bit.
   */
  private final Key index_set_key;

  /**
   * The keyspace of the item keys to generate.
   */
  private final short item_key_type;
  private final int item_key_primary;

  private final IndexObjectCollator collator;

  /**
   * The current version of this directory set, incremented each time a
   * modification is made.
   */
  private long directory_version = 0;



  DirectorySet(KeyObjectTransaction transaction,
               Key directory_properties_key,
               Key property_set_key, Key index_set_key,
               short item_key_type, int item_key_primary) {
    this.transaction = transaction;
    this.directory_properties_key = directory_properties_key;
    this.index_set_key = index_set_key;
    this.property_set_key = property_set_key;
    this.item_key_type = item_key_type;
    this.item_key_primary = item_key_primary;

    this.collator = new ItemCollator();
  }

  private AddressableDataFile getDataFile(Key k) {
    return transaction.getDataFile(k, 'w');
  }

  private Key getItemKey(long id) {
    return new Key(item_key_type, item_key_primary, id);
  }

  /**
   * Generates a new unique id for an item.
   */
  private long generateId() {
    DataFile df = getDataFile(directory_properties_key);
    PropertySet pset = new PropertySet(df);
    long v = pset.getLongProperty("v", 16);
    pset.setLongProperty("v", v + 1);
    return v;
  }

  /**
   * Given an item id, returns the name of it.
   */
  String getItemName(long id) {
    Key k = getItemKey(id);
    DataFile df = getDataFile(k);
    try {
      DataInputStream din = DataFileUtils.asDataInputStream(df);
      return din.readUTF();
    }
    catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /**
   * Adds a new item into the list and returns a Key that points to the
   * DataFile of the object. The DataFile will be populated with the name of
   * the object.
   */
  Key addItem(String name) {
    ++directory_version;

    PropertySet pset = new PropertySet(getDataFile(property_set_key));
    // Assert the item isn't already stored,
    if (pset.getLongProperty(name, -1) != -1) {
      throw new RuntimeException("Item already exists: " + name);
    }

    // Generate a unique identifier for the name,
    long id = generateId();

    pset.setLongProperty(name, id);
    OrderedList64Bit iset = new OrderedList64Bit(getDataFile(index_set_key));
    iset.insert(name, id, collator);

    Key item_key = getItemKey(id);

    DataFile df = getDataFile(item_key);
    try {
      DataOutputStream dout = DataFileUtils.asDataOutputStream(df);
      dout.writeUTF(name);
    }
    catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }

    return item_key;
  }

  /**
   * Returns the key for the named item, or null if there is no item with the
   * given name.
   */
  Key getItem(String name) {
    PropertySet pset = new PropertySet(getDataFile(property_set_key));
    long id = pset.getLongProperty(name, -1);
    if (id == -1) {
      return null;
    }
    else {
      return getItemKey(id);
    }
  }

  /**
   * Removes the named item from the structure. Generates an exception if the
   * item isn't found.
   */
  Key removeItem(String name) {
    ++directory_version;

    PropertySet pset = new PropertySet(getDataFile(property_set_key));
    long id = pset.getLongProperty(name, -1);
    // Assert the item is stored,
    if (id == -1) {
      throw new RuntimeException("Item not found: " + name);
    }

    pset.setProperty(name, null);
    OrderedList64Bit iset = new OrderedList64Bit(getDataFile(index_set_key));
    iset.remove(name, id, collator);

    // Delete the associated datafile
    Key k = getItemKey(id);
    DataFile df = getDataFile(k);
    df.delete();

    return k;
  }

  /**
   * Returns a List<String> of all items in the set, sorted in lexicographical
   * order and will not contain any duplicates.
   */
  List<String> itemSet() {
    OrderedList64Bit iset = new OrderedList64Bit(getDataFile(index_set_key));
    return new ItemList(iset);
  }

  /**
   * Returns a DataFile object for the given item name ready to be updated
   * with content.
   */
  AddressableDataFile getItemDataFile(String name) {
    PropertySet pset = new PropertySet(getDataFile(property_set_key));
    long id = pset.getLongProperty(name, -1);
    // Assert the item is stored,
    if (id == -1) {
      throw new RuntimeException("Item not found: " + name);
    }

    Key k = getItemKey(id);
    AddressableDataFile df = getDataFile(k);

    // Find out how large the header is, without actually reading it. This is
    // an optimization to improve queries that want to only find the size of
    // the file without touching the data.
    int header_size = 0;
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream(64);
      DataOutputStream dout = new DataOutputStream(bout);
      dout.writeUTF(name);
      dout.flush();
      dout.close();
      header_size = bout.size();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    df.position(header_size);
    return new SubDataFile(df, header_size);
//    DataInputStream din = DataFileUtils.asDataInputStream(df);
//    try {
//      String str = din.readUTF();
//      long start = df.position();
//      return new SubDataFile(df, start);
//    }
//    catch (IOException e) {
//      throw new RuntimeException(e.getMessage());
//    }
  }

  /**
   * Number of elements in the set.
   */
  long size() {
    OrderedList64Bit iset = new OrderedList64Bit(getDataFile(index_set_key));
    return iset.size();
  }

  /**
   * Copies an item from this directory set to another.
   */
  void copyTo(String name, DirectorySet destination) {
    ++destination.directory_version;

    PropertySet pset = new PropertySet(getDataFile(property_set_key));
    long id = pset.getLongProperty(name, -1);
    // Assert the item is stored,
    if (id == -1) {
      throw new RuntimeException("Item not found: " + name);
    }

    // Get the source data file item,
    Key source_k = getItemKey(id);
    DataFile source_df = getDataFile(source_k);

    // Get the item from the destination. Throw an error if the item not
    // already found in the destination file set.
    Key dest_k = destination.getItem(name);
    if (dest_k == null) {
      throw new RuntimeException("Item not in destination: " + name);
    }
    DataFile destination_df = destination.getDataFile(dest_k);

    // Copy the data,
//    destination_df.delete();
//    source_df.copyTo(destination_df, source_df.size());
    source_df.replicateTo(destination_df);

  }

  // ----- Inner classes

  /**
   * A lexicographical string collator for item names.
   * Uses the Java String.compareTo method for the ordering method.
   */
  private class ItemCollator implements IndexObjectCollator {

    public ItemCollator() {
    }

    @Override
    public int compare(long ref, Object val) {
      // Nulls are ordered at the beginning
      String v = getItemName(ref);
      if (val == null && v == null) {
        return 0;
      }
      else if (val == null) {
        return 1;
      }
      else if (v == null) {
        return -1;
      }
      else {
        return v.compareTo((String) val);
      }
    }

  }

  // This is a trusted object,
  private class ItemList extends AbstractList<String> implements RandomAccess {

    private OrderedList64Bit list;

    private long local_dir_version;

    ItemList(OrderedList64Bit list) {
      this.local_dir_version = directory_version;
      this.list = list;
    }

    @Override
    public String get(int index) {
      // If the directory set changed while this list in use, generate an
      // error.
      if (this.local_dir_version != directory_version) {
        throw new ConcurrentModificationException(
                                    "Directory changed while iterator in use");
      }

      long id = list.get(index);
      return getItemName(id);
    }

    @Override
    public int indexOf(Object o) {
      // Since we know the list is sorted and there are no duplicate entries,
      // we can resolve this one quickly
      int i = Collections.binarySearch(this, (String) o);
      if (i < 0) {
        return -1;
      }
      else {
        return i;
      }
    }

    @Override
    public int lastIndexOf(Object o) {
      // We know there are no duplicates so the result will be the same as a
      // call to 'indexOf'
      return indexOf(o);
    }

    @Override
    public boolean contains(Object o) {
      return indexOf(o) >= 0;
    }

    @Override
    public int size() {
      return (int) Math.min(list.size(), Integer.MAX_VALUE);
    }

  }


  private static class SubDataFile implements AddressableDataFile {

    private final AddressableDataFile df;
    private final long start;

    SubDataFile(AddressableDataFile df, long start) {
      this.df = df;
      this.start = start;
    }





    @Override
    public void copyFrom(DataFile from, long size) {
      df.copyFrom(from, size);
    }

    @Override
    public void replicateFrom(DataFile from) {
      // We must fall back to a copy implementation because the header
      // may be different between replications of the data.
      delete();
      position(0);
      copyFrom(from, from.size());
    }

    @Override
    public Object getBlockLocationMeta(long start_position, long end_position) {
      // Transform,
      return df.getBlockLocationMeta(
                                 start_position + start, end_position - start);
    }

    // Legacy
    @Override
    public void copyTo(DataFile target, long size) {
      target.copyFrom(this, size);
    }

    // Legacy
    @Override
    public void replicateTo(DataFile target) {
      target.replicateFrom(this);
    }

//    public void copyTo(DataFile target, long size) {
//      df.copyTo(target, size);
//    }
//
//    public void replicateTo(DataFile target) {
//
//      // This is a little complex. If 'target' is an instance of SubDataFile
//      // we use the raw 'replicateTo' method on the data files and preserve
//      // the header on the target by making a copy of it before the replicateTo
//      // function.
//      // Otherwise, we use a 'copyTo' implementation.
//
//      // If replicating to a SubDataFile
//      if (target instanceof SubDataFile) {
//        // Preserve the header of the target
//        SubDataFile target_file = (SubDataFile) target;
//        long header_size = target_file.start;
//        if (header_size <= 8192) {
//          DataFile target_df = target_file.df;
//          // Make a copy of the header in the target,
//          int ihead_size = (int) header_size;
//          byte[] header = new byte[ihead_size];
//          target_df.position(0);
//          target_df.get(header, 0, ihead_size);
//
//          // Replicate the bases
//          df.replicateTo(target_df);
//          // Now 'target_df' will be a replica of this, so we need to copy
//          // the previous header back on the target.
//          // Remove the replicated header on the target and copy the old one
//          // back.
//          target_df.position(start);
//          target_df.shift(ihead_size - start);
//          target_df.position(0);
//          target_df.put(header, 0, ihead_size);
//          // Set position per spec
//          target_df.position(target_df.size());
//          // Done.
//          return;
//        }
//      }
//      // Fall back to a copy-to implementation
//      target.delete();
//      target.position(0);
//      df.position(start);
//      df.copyTo(target, df.size() - start);
//
//    }

    @Override
    public void delete() {
      df.setSize(start);
    }

    @Override
    public byte get() {
      return df.get();
    }

    @Override
    public void get(byte[] buf, int off, int len) {
      df.get(buf, off, len);
    }

    @Override
    public char getChar() {
      return df.getChar();
    }

    @Override
    public int getInt() {
      return df.getInt();
    }

    @Override
    public long getLong() {
      return df.getLong();
    }

    @Override
    public short getShort() {
      return df.getShort();
    }

    @Override
    public void position(long position) {
      if (position < 0) {
        throw new java.lang.IndexOutOfBoundsException();
      }
      df.position(start + position);
    }

    @Override
    public long position() {
      return df.position() - start;
    }

    @Override
    public void put(byte b) {
      df.put(b);
    }

    @Override
    public void put(byte[] buf, int off, int len) {
      df.put(buf, off, len);
    }

    @Override
    public void put(byte[] buf) {
      df.put(buf);
    }

    @Override
    public void putChar(char c) {
      df.putChar(c);
    }

    @Override
    public void putInt(int i) {
      df.putInt(i);
    }

    @Override
    public void putLong(long l) {
      df.putLong(l);
    }

    @Override
    public void putShort(short s) {
      df.putShort(s);
    }

    @Override
    public void setSize(long size) {
      if (size < 0) {
        throw new java.lang.IndexOutOfBoundsException();
      }
      df.setSize(size + start);
    }

    @Override
    public void shift(long offset) {
      df.shift(offset);
    }

    @Override
    public long size() {
      return df.size() - start;
    }

  }

}
