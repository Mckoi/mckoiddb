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

import com.mckoi.data.DataFile;
import java.util.Comparator;
import java.util.NoSuchElementException;

/**
 * Wraps a DataFile to provide an ordered list of 128-bit references, where
 * the list maintains a sort order of either the reference value itself or
 * on a key in the referenced object.
 * <p>
 * This maintains the sorted order of the list inline by using the backed
 * DataFile shift functions.
 * <p>
 * OrderedReferenceList supports random access positional queries, and an
 * efficient size query.
 *
 * @author Tobias Downer
 */

class OrderedReferenceList implements ODBList {

  /**
   * The backed transaction.
   */
  private final ODBTransactionImpl transaction;

  /**
   * The list resource reference.
   */
  private final Reference list_reference;

  /**
   * The class that describes this list.
   */
  private final ODBClass list_class;

  /**
   * The backed DataFile.
   */
  private final DataFile data;

  /**
   * A description of the order specification of this list.
   */
  private final OrderSpec order_spec;

  /**
   * Cache for the comparator.
   */
  private Comparator<Reference> cached_comparator;


  /**
   * The position of the first element in the list.
   */
  private long start_pos = -1;

  /**
   * The position after the end element in the list.
   */
  private long end_pos = -1;

  /**
   * The version of this set (incremented each time a modification made).
   */
  private long version;

  /**
   * This boolean is set to true if this object is a root object and
   * a change has been made to a subset that has caused the state
   * information in the root object to become dirty.
   */
  private boolean root_state_dirty = false;

  /**
   * The set object that is the root.
   */
  private OrderedReferenceList root_set;

  /**
   * The lower bounds of the view or null if no lower bound.
   */
  private Reference lower_bound;

  /**
   * The upper bounds of the view or null if no upper bound.
   */
  private Reference upper_bound;


  /**
   * View constructor.
   */
  private OrderedReferenceList(ODBTransactionImpl transaction,
                               Reference list_reference,
                               ODBClass list_class,
                               DataFile data,
                               OrderSpec order_spec,
                               OrderedReferenceList root_set,
                               Reference lower_bound, Reference upper_bound) {

    this.transaction = transaction;
    this.list_reference = list_reference;
    this.list_class = list_class;
    this.data = data;
    this.order_spec = order_spec;

    this.root_set = root_set;

    this.lower_bound = lower_bound;
    this.upper_bound = upper_bound;

    // Set initial version to -1
    this.version = -1;
  }

  /**
   * Constructor.
   */
  OrderedReferenceList(ODBTransactionImpl transaction,
                       Reference list_reference,
                       ODBClass list_class,
                       DataFile data,
                       OrderSpec order_spec) {

    this(transaction, list_reference,
         list_class, data, order_spec, null, null, null);

    this.root_set = this;

    // This constructor always generates root objects, so we set the version
    // to 0 and root_state_dirty to true.

    // The version of new list is 0
    this.version = 0;

    // This forces a state update
    this.root_state_dirty = true;

  }

  /**
   * Updates the internal state of this object (the start_pos and end_pos
   * objects) if the subset is determined to be dirty (this version is less
   * than the version of the root). This is necessary for when the list
   * changes.
   */
  private void updateInternalState() {

    if (this.version < root_set.version || this.root_state_dirty) {
      // Reset the root state dirty boolean
      this.root_state_dirty = false;

      // Read the size
      final long sz = data.size() / 16;

      // The empty states,
      if (sz == 0) {
        start_pos = 0;
        end_pos = 0;
      }
      // The none empty state
      else {

        // If there is no lower bound we use start of the list
        if (lower_bound == null) {
          start_pos = 0;
        }
        // If there is a lower bound we search for the string and use it
        else {
          long pos = indexOf(lower_bound, 0, sz);
          if (pos < 0) pos = -(pos + 1);
          start_pos = pos;
        }

        // If there is no upper bound we use end of the list
        if (upper_bound == null) {
          end_pos = sz;
        }
        // Otherwise there is an upper bound so search for the string and use it
        else {
          long pos = indexOf(upper_bound, 0, sz);
          if (pos < 0) pos = -(pos + 1);
          end_pos = pos;
        }
      }

      // Update the version of this to the parent.
      this.version = root_set.version;
    }
  }

  /**
   * Returns true if the given reference is within the lower and upper
   * bounds.
   */
  private boolean isWithinBounds(Reference ref) {
    Comparator<Reference> c = getOrderSpecComparator();
    // If it's outside the bounds, return false,
    if (upper_bound != null && c.compare(ref, upper_bound) >= 0) {
      return false;
    }
    if (lower_bound != null && c.compare(ref, lower_bound) < 0) {
      return false;
    }
    return true;
  }

  /**
   * Returns true if the given value is within the lower and upper bounds.
   */
  private boolean isWithinBounds(String key_value) {
    if (order_spec.orderedByReferenceValue()) {
      throw new RuntimeException("Reference order only");
    }
    Comparator<Reference> c = getExternalOrderSpecComparator(key_value);
    // If it's outside the bounds, return false,
    if (upper_bound != null && c.compare(null, upper_bound) >= 0) {
      return false;
    }
    if (lower_bound != null && c.compare(null, lower_bound) < 0) {
      return false;
    }
    return true;
  }

  /**
   * Bounds the given reference within the upper and lower boundary defined by
   * this set.
   */
  private Reference bounded(Reference str) {
    if (str == null) {
      throw new NullPointerException();
    }
    Comparator<Reference> c = getOrderSpecComparator();
    // If str is less than lower bound then return lower bound
    if (lower_bound != null && c.compare(str, lower_bound) < 0) {
      return lower_bound;
    }
    // If str is greater than upper bound then return upper bound
    if (upper_bound != null && c.compare(str, upper_bound) >= 0) {
      return upper_bound;
    }
    return str;
  }


  private Reference getLowerReference(String key_value) {
//    System.out.println("^^ getLowerReference " + key_value);
//    System.out.println("^^ start_pos = " + start_pos);
//    System.out.println("^^ end_pos = " + end_pos);
//    for (long i = start_pos; i < end_pos; ++i) {
//      System.out.println("^^ " + transaction.getObject(this.getElementClass(), this.getReferenceAt(i)));
//    }

    long i = indexOf(key_value, start_pos, end_pos);
    if (i < 0) {
      i = -(i + 1);
    }
    if (i < end_pos) {
      return this.getReferenceAt(i);
    }
    return null;
  }

  private Reference getUpperReference(String key_value) {
    long i = indexOf(key_value, start_pos, end_pos);
    if (i < 0) {
      i = -(i + 1);
    }
    if (i < end_pos) {
      return this.getReferenceAt(i);
    }
    return null;
  }

  /**
   * Returns the size of the list (the total number of references stored).
   */
  @Override
  public long size() {
    updateInternalState();
    return end_pos - start_pos;
  }

  @Override
  public boolean isEmpty() {
    updateInternalState();
    return end_pos == start_pos;
  }

  public ODBClass getODBClass() {
    return list_class;
  }

  @Override
  public ODBClass getElementClass() {
    return order_spec.getODBClass();
  }

  @Override
  public void add(Reference ref) throws ConstraintViolationException {

    updateInternalState();
    // Check the reference is within the bounds,
    if (!isWithinBounds(ref)) {
      throw new RuntimeException("Reference is out of bounds of the view");
    }

    // Check the reference is of the correct type for the list,
    transaction.checkObjectType(getElementClass(), ref);
    // Insert into the list,
    insert(ref);
  }

  @Override
  public void add(ODBObject value) throws ConstraintViolationException {
    add(value.getReference());
  }

  @Override
  public Reference first() {
    updateInternalState();
    if (start_pos >= end_pos) {
      throw new NoSuchElementException();
    }
    return getReferenceAt(start_pos);
  }

  @Override
  public Reference last() {
    updateInternalState();
    if (start_pos >= end_pos) {
      throw new NoSuchElementException();
    }
    return getReferenceAt(end_pos - 1);
  }

  @Override
  public ODBListIterator iterator() {
    updateInternalState();

    return new ODBListIterator() {

      long pos = start_pos - 1;

      @Override
      public boolean hasNext() {
        return pos < (end_pos - 1);
      }

      @Override
      public boolean hasPrevious() {
        return pos > start_pos;
      }

      @Override
      public ODBObject next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        ++pos;
        return getObject(pos - start_pos);
      }

      @Override
      public int nextIndex() {
        return (int) (pos - start_pos) + 1;
      }

      @Override
      public ODBObject previous() {
        if (!hasPrevious()) {
          throw new NoSuchElementException();
        }
        --pos;
        return getObject(pos - start_pos);
      }

      @Override
      public int previousIndex() {
        return (int) (pos - start_pos);
      }

      @Override
      public void add(ODBObject e) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void set(ODBObject e) {
        throw new UnsupportedOperationException();
      }

    };

  }



  @Override
  public boolean contains(Reference ref) {
    updateInternalState();

    // If it's outside the bounds, return false,
    if (!isWithinBounds(ref)) {
      return false;
    }
    long pos = indexOf(ref, start_pos, end_pos);
    if (pos < 0) {
      return false;
    }
    return true;
  }

  @Override
  public Reference get(long index) {
    updateInternalState();
    long sz = end_pos - start_pos;
    if (index < 0 || index >= sz) {
      throw new IndexOutOfBoundsException();
    }
    return getReferenceAt(start_pos + index);
  }

  @Override
  public ODBObject getObject(long index) {
    return transaction.getObject(getElementClass(), get(index));
  }

  @Override
  public long indexOf(Reference ref) {
    updateInternalState();

    long pos = indexOf(ref, start_pos, end_pos);
    return (pos - start_pos);
  }

  @Override
  public long lastIndexOf(Reference ref) {
    updateInternalState();

    long pos = lastIndexOf(ref, start_pos, end_pos);
    return (pos - start_pos);
  }

  @Override
  public boolean remove(Reference ref) {
    updateInternalState();
    if (!isWithinBounds(ref)) {
      return false;
    }
    return remove(ref, start_pos, end_pos);
  }

  @Override
  public boolean removeAll(Reference ref) {
    updateInternalState();
    if (!isWithinBounds(ref)) {
      return false;
    }
    return removeAll(ref, start_pos, end_pos);
  }

  @Override
  public ODBList sub(Reference from_ref, Reference to_ref) {
    updateInternalState();

    // check the bounds not out of range of the parent bounds
    from_ref = bounded(from_ref);
    to_ref = bounded(to_ref);
    return new OrderedReferenceList(transaction, list_reference,
                     list_class, data, order_spec, root_set, from_ref, to_ref);

  }

  @Override
  public ODBList head(Reference to_ref) {
    updateInternalState();

    to_ref = bounded(to_ref);
    return new OrderedReferenceList(transaction, list_reference,
                  list_class, data, order_spec, root_set, lower_bound, to_ref);

  }

  @Override
  public ODBList tail(Reference from_ref) {
    updateInternalState();

    from_ref = bounded(from_ref);
    return new OrderedReferenceList(transaction, list_reference,
                list_class, data, order_spec, root_set, from_ref, upper_bound);

  }




  @Override
  public boolean contains(String key_value) {
    updateInternalState();

    // If it's outside the bounds, return false,
    if (!isWithinBounds(key_value)) {
      return false;
    }
    long pos = indexOf(key_value, start_pos, end_pos);
    if (pos < 0) {
      return false;
    }
    return true;
  }

  @Override
  public Reference get(String key_value) {
    updateInternalState();
    // Error if outside the bounds,
    if (!isWithinBounds(key_value)) {
      throw new RuntimeException("The value is out of bounds of the view");
    }
    long pos = indexOf(key_value, start_pos, end_pos);
    if (pos < 0) {
      // NOTE; Is it correct to return null if a value isn't found?
      return null;
    }
    return getReferenceAt(pos);
  }

  @Override
  public ODBObject getObject(String key_value) {
    Reference ref = get(key_value);
    if (ref == null) {
      return null;
    }
    return transaction.getObject(getElementClass(), ref);
  }

  @Override
  public long indexOf(String key_value) {
    updateInternalState();

    long pos = indexOf(key_value, start_pos, end_pos);
    return pos - start_pos;
  }

  @Override
  public long lastIndexOf(String key_value) {
    updateInternalState();

    long pos = lastIndexOf(key_value, start_pos, end_pos);
    return pos - start_pos;
  }

  @Override
  public boolean remove(String key_value) {
    updateInternalState();
    if (!isWithinBounds(key_value)) {
      return false;
    }
    return remove(key_value, start_pos, end_pos);
  }

  @Override
  public boolean removeAll(String key_value) {
    updateInternalState();
    if (!isWithinBounds(key_value)) {
      return false;
    }
    return removeAll(key_value, start_pos, end_pos);
  }

  @Override
  public ODBList sub(String from_key, String to_key) {
    updateInternalState();

    Reference from_ref = getLowerReference(from_key);
    Reference to_ref = getUpperReference(to_key);
    return new OrderedReferenceList(transaction, list_reference,
                     list_class, data, order_spec, root_set, from_ref, to_ref);
  }

  @Override
  public ODBList head(String to_key) {
    updateInternalState();

    Reference to_ref = getUpperReference(to_key);
    return new OrderedReferenceList(transaction, list_reference,
                  list_class, data, order_spec, root_set, lower_bound, to_ref);
  }

  @Override
  public ODBList tail(String from_key) {
    updateInternalState();

    Reference from_ref = getLowerReference(from_key);
    return new OrderedReferenceList(transaction, list_reference,
                list_class, data, order_spec, root_set, from_ref, upper_bound);
  }








  /**
   * Returns the Reference object for the value at the given position in the
   * list.
   */
  Reference getReferenceAt(long position) {
    data.position(position * 16);
    long high = data.getLong();
    long low = data.getLong();
    return new Reference(high, low);
  }

  /**
   * Finds the first index of the given key value in the list between the
   * given bounds. If the order spec of this list is by reference value then
   * this method generates a runtime exception.
   * <p>
   * Note that the bounds positions are inclusive. Returns -(pos + 1) if the
   * value isn't found where pos is the location the value would be found.
   */
  long indexOf(String key_value, long start, long end) {
    if (order_spec.orderedByReferenceValue()) {
      throw new RuntimeException("Reference order only");
    }
    Comparator<Reference> c = getExternalOrderSpecComparator(key_value);
    return searchFirst(null, c, start, end - 1);
  }

  /**
   * Finds the last index of the given key value in the list between the
   * given bounds. If the order spec of this list is by reference value then
   * this method generates a runtime exception.
   * <p>
   * Note that the bounds positions are inclusive. Returns -(pos + 1) if the
   * value isn't found where pos is the location the value would be found.
   */
  long lastIndexOf(String key_value, long start, long end) {
    if (order_spec.orderedByReferenceValue()) {
      throw new RuntimeException("Reference order only");
    }
    Comparator<Reference> c = getExternalOrderSpecComparator(key_value);
    return searchLast(null, c, start, end - 1);
  }

  /**
   * Finds the first index of the given reference value in the list between
   * the given bounds. This is an exact search that will find the position
   * of the reference.
   * <p>
   * Note that the bounds positions are inclusive. Returns -(pos + 1) if the
   * value isn't found where pos is the location the value would be found.
   */
  long indexOf(Reference ref, long start, long end) {
    Comparator<Reference> c = getOrderSpecComparator();
    return searchFirst(ref, c, start, end - 1);
  }

  /**
   * Finds the last index of the given reference value in the list between
   * the given bounds. This is an exact search that will find the position
   * of the last reference.
   * <p>
   * Note that the bounds positions are inclusive. Returns -(pos + 1) if the
   * value isn't found where pos is the location the value would be found.
   */
  long lastIndexOf(Reference ref, long start, long end) {
    Comparator<Reference> c = getOrderSpecComparator();
    return searchLast(ref, c, start, end - 1);
  }

  /**
   * Removes the reference stored at the given index.
   */
  void removeReferenceAt(long pos) {

    // Tell the root set that any child subsets may be dirty
    if (root_set != this) {
      root_set.version += 1;
      root_set.root_state_dirty = true;
    }
    version += 1;

    data.position(pos * 16);
    // The reference being removed from the list,
    long ref_high = data.getLong();
    long ref_low = data.getLong();
    Reference ref = new Reference(ref_high, ref_low);
    data.shift(-16);

    // Force an internal state update on this object
    root_state_dirty = true;

    transaction.getObjectLog().logListRemoval(list_reference, ref,
                                              list_class.getReference());
  }

  /**
   * Removes all the references stored between the given indexes (inclusive).
   */
  void removeReferenceRange(long pos_start, long pos_end) {

    // Tell the root set that any child subsets may be dirty
    if (root_set != this) {
      root_set.version += 1;
      root_set.root_state_dirty = true;
    }
    version += 1;

    ++pos_end;
    long size_to_delete = (pos_end - pos_start) * 16;
    data.position(pos_end * 16);
    data.shift(-size_to_delete);

    // Force an internal state update on this object
    root_state_dirty = true;

    // We need to go through and add all the removed referenced to the
    // transaction log,
    throw new RuntimeException("PENDING - log all removed items");

  }

  /**
   * Removes the first entry from the list with the given key value. If the
   * order spec of this list is by reference value then this method generates
   * a runtime exception.
   * <p>
   * Returns true if a value was removed.
   */
  boolean remove(String key_value, long start, long end) {
    if (order_spec.orderedByReferenceValue()) {
      throw new RuntimeException("Reference order only");
    }
    Comparator<Reference> c = getExternalOrderSpecComparator(key_value);
    long pos = searchFirst(null, c, start, end - 1);
    if (pos < 0) {
      return false;
    }
    removeReferenceAt(pos);
    return true;
  }

  /**
   * Removes all the entries from the list that match the given key value. If
   * the order spec of this list is by reference value then this method
   * generates a runtime exception.
   * <p>
   * Returns true if any value was removed.
   */
  boolean removeAll(String key_value, long start, long end) {
    if (order_spec.orderedByReferenceValue()) {
      throw new RuntimeException("Reference order only");
    }
    Comparator<Reference> c = getExternalOrderSpecComparator(key_value);
    long[] bounds = searchFirstAndLast(null, c, start, end - 1);
    if (bounds[0] < 0) {
      return false;
    }
    removeReferenceRange(bounds[0], bounds[1]);
    return true;
  }

  /**
   * Removes the first entry from the list with the given reference.
   * <p>
   * Returns true if a value was removed.
   */
  boolean remove(Reference ref, long start, long end) {
    Comparator<Reference> c = getOrderSpecComparator();
    long pos = searchFirst(ref, c, start, end - 1);
    if (pos < 0) {
      return false;
    }
    removeReferenceAt(pos);
    return true;
  }

  /**
   * Removes all entries from the list with the given reference.
   * <p>
   * Returns true if a value was removed.
   */
  boolean removeAll(Reference ref, long start, long end) {
    Comparator<Reference> c = getOrderSpecComparator();
    long[] bounds = searchFirstAndLast(ref, c, start, end - 1);
    if (bounds[0] < 0) {
      return false;
    }
    removeReferenceRange(bounds[0], bounds[1]);
    return true;
  }





  /**
   * Returns a Comparator that is used to order values in the list as per the
   * order specification.
   */
  private Comparator<Reference> getOrderSpecComparator() {
    if (cached_comparator == null) {
      // If the values are ordered by reference value,
      if (order_spec.orderedByReferenceValue()) {
        cached_comparator = reference_value_comparator;
      }
      else {
        Comparator<Reference> c = getExternalOrderSpecComparator(null);
        if (order_spec.allowsDuplicates()) {
          c = new ExactPositionComparator(c);
        }
        cached_comparator = c;
      }
    }
    return cached_comparator;
  }

  /**
   * Returns a Comparator that is used to order values in the list as per the
   * order specification with an external key value.
   */
  private Comparator<Reference>
                             getExternalOrderSpecComparator(String key_value) {

    String desc = order_spec.getKeyCollationDescription();
    int field_num = order_spec.getKeyFieldIndex();
    ODBClass os_list_class = order_spec.getODBClass();

    if (desc.equals("lexi")) {
      // Lexicographical order,
      return new LexiFieldComparator(os_list_class, field_num, key_value);
    }
    else {
      throw new RuntimeException("Pending collators");
    }
  }




  // -----


//  /**
//   * Inserts a reference into this list at the ordered position as determined
//   * by the order specification. Throws a ConstraintViolationException if
//   * the list does not allow duplicate entries and a duplicate was inserted.
//   */
//  void insert(Reference ref) throws ConstraintViolationException {
//    insert(ref, getOrderSpecComparator());
//  }



  /**
   * Inserts the reference value into the list in ordered position as per
   * the order spec. Throws ConstraintViolationException if a value is given
   * that is a duplicate of a value already in the list and duplicates are
   * not allowed.
   */
  void insert(Reference ref) throws ConstraintViolationException {

//    System.out.println("-- INSERTING: " + ref);
//    System.out.println("  " + (size() - 1));

    Comparator<Reference> c = getOrderSpecComparator();

    long pos;

    pos = searchLast(ref, c, 0, size() - 1);
    // If found, check the spec permits duplicates,
    if (pos >= 0 && !order_spec.allowsDuplicates()) {
      throw new ConstraintViolationException("Duplicate key not permitted");
    }

//    // NOTE: The following code is a bit subtle. If we are ordered by key value
//    //   and allow duplicates then we find the position to insert using an
//    //   ExactPositionComparator that searches by the key value as primary
//    //   and the reference value as secondary.
//    //   Otherwise we use a regular search since either duplicates are not
//    //   allowed and we don't need to worry about a secondary index, or we
//    //   are searching by the reference value which also means we don't need
//    //   to worry about a secondary index.
//
//    // If we are ordering by a key value and allowing duplicates
//    if (!order_spec.orderedByReferenceValue() &&
//           order_spec.allowsDuplicates()) {
//      // Use an exact position comparator to find the place to insert
//      pos = searchLast(ref, new ExactPositionComparator(c), 0, size() - 1);
//    }
//    // Otherwise, either no duplicates or ordered by reference value
//    else {
//      // Find the last value that matches 'ref',
//      pos = searchLast(ref, c, 0, size() - 1);
//
//      // If found, check the spec permits duplicates,
//      if (pos >= 0 && !order_spec.allowsDuplicates()) {
//        throw new ConstraintViolationException("Duplicate key not permitted");
//      }
//    }

    // Convert to the position to insert
    if (pos < 0) {
      pos = -(pos + 1);
    }
    else {
      pos = pos + 1;
    }

    // Tell the root set that any child subsets may be dirty
    if (root_set != this) {
      root_set.version += 1;
      root_set.root_state_dirty = true;
    }
    version += 1;

    // Shift and add,
    data.position(pos * 16);
    data.shift(16);
    data.putLong(ref.getHighLong());
    data.putLong(ref.getLowLong());

    // Force an internal state update on this object
    root_state_dirty = true;

    transaction.getObjectLog().logListAddition(list_reference, ref,
                                               list_class.getReference());

  }

  /**
   * Binary search for the first value in the set that matches the given value.
   */
  private long searchFirst(Reference value, Comparator<Reference> c,
                           long low, long high) {

    if (low > high) {
      return -(low + 1);
    }

    while (true) {
      // If low is the same as high, we are either at the first value or at
      // the position to insert the value,
      if ((high - low) <= 4) {
        for (long i = low; i <= high; ++i) {
          Reference val = getReferenceAt(i);
          int res = c.compare(val, value);
          if (res == 0) {
            return i;
          }
          if (res > 0) {
            return -(i + 1);
          }
        }
        return -(high + 2);
      }

      // The index half way between the low and high point
      long mid = (low + high) >> 1;
      // Read the middle value from the data file,
      Reference mid_val = getReferenceAt(mid);

      // Compare it with the value
      int res = c.compare(mid_val, value);
      if (res < 0) {
        low = mid + 1;
      }
      else if (res > 0) {
        high = mid - 1;
      }
      else {  // if (res == 0)
        high = mid;
      }
    }
  }

  /**
   * Binary search for the last value in the set that matches the given value.
   */
  private long searchLast(Reference value, Comparator<Reference> c,
                          long low, long high) {

    if (low > high) {
      return -(low + 1);
    }

    while (true) {
      // If low is the same as high, we are either at the last value or at
      // the position to insert the value,
      if ((high - low) <= 4) {
        for (long i = high; i >= low; --i) {
          Reference val = getReferenceAt(i);
          int res = c.compare(val, value);
          if (res == 0) {
            return i;
          }
          if (res < 0) {
            return -(i + 2);
          }
        }
        return -(low + 1);
      }

      // The index half way between the low and high point
      long mid = (low + high) >> 1;
      // Read the middle value from the data file,
      Reference mid_val = getReferenceAt(mid);

      // Compare it with the value
      int res = c.compare(mid_val, value);
      if (res < 0) {
        low = mid + 1;
      }
      else if (res > 0) {
        high = mid - 1;
      }
      else {  // if (res == 0)
        low = mid;
      }
    }

  }

  /**
   * Searches for the first and last positions of the given value in the
   * set over the given comparator.
   */
  private long[] searchFirstAndLast(Reference value, Comparator<Reference> c,
                                    long low, long high) {

    if (low > high) {
      // The not found position,
      long nf_pos = -(low + 1);
      return new long[] { nf_pos, nf_pos };
    }

    while (true) {
      // If low is the same as high, we are either at the first value or at
      // the position to insert the value,
      if ((high - low) <= 4) {
        long r0 = searchFirst(value, c, low, high);
        long r1 = searchLast(value, c, low, high);
        return new long[] { r0, r1 };
      }

      // The index half way between the low and high point
      long mid = (low + high) >> 1;
      // Read the middle value from the data file,
      Reference mid_val = getReferenceAt(mid);

      // Compare it with the value
      int res = c.compare(mid_val, value);
      if (res < 0) {
        low = mid + 1;
      }
      else if (res > 0) {
        high = mid - 1;
      }
      else {  // if (res == 0)
        long r0 = searchFirst(value, c, low, high);
        long r1 = searchLast(value, c, low, high);
        return new long[] { r0, r1 };
      }
    }

  }



  // -----

  /**
   * Wraps a comparator to include the reference id as the secondary component
   * of the order. This means searching for a particular Reference in a list
   * containing the same key is still a low complexity operation.
   */
  private static class ExactPositionComparator
                                            implements Comparator<Reference> {
    private final Comparator<Reference> backed;
    ExactPositionComparator(Comparator<Reference> backed) {
      this.backed = backed;
    }
    @Override
    public int compare(Reference r1, Reference r2) {
      int c = backed.compare(r1, r2);
      // If the backed comparator found the values to be equal, we use the
      // reference value itself as the secondary search criteria.
      if (c == 0) {
        return reference_value_comparator.compare(r1, r2);
      }
      return c;
    }
  }

  /**
   * A comparator that orders by the reference value itself.
   */
  private static Comparator<Reference> reference_value_comparator =
                                                 new Comparator<Reference>() {
    @Override
    public int compare(Reference r1, Reference r2) {
      return r1.compareTo(r2);
    }
  };

  /**
   * A Collator that looks up a reference to a string field and compares
   * references lexicographically.
   */
  private class LexiFieldComparator implements Comparator<Reference> {

    /**
     * The class.
     */
    private final ODBClass clazz;

    /**
     * The field number in the referenced object which is the inline string.
     */
    private final int field_num;

    /**
     * If we are comparing against an external string.
     */
    private final String external_key;

    /**
     * Constructor.
     */
    LexiFieldComparator(ODBClass clazz, int field_num, String external_key) {
      this.clazz = clazz;
      this.field_num = field_num;
      this.external_key = external_key;
    }

    @Override
    public int compare(Reference o1, Reference o2) {
      String str1, str2;

      // The inline string elements of the object,

      // If o1 or o2 is null we use the external key,
      if (o1 != null) {
        ODBObject obj1 = transaction.getObject(clazz, o1);
        str1 = obj1.getString(field_num);
      }
      else {
        str1 = external_key;
      }
      if (o2 != null) {
        ODBObject obj2 = transaction.getObject(clazz, o2);
        str2 = obj2.getString(field_num);
      }
      else {
        str2 = external_key;
      }

      // Handle nulls,
      // Nulls are ordered to the start,
      if (str1 == null) {
        if (str2 == null) {
          return 0;
        }
        else {
          return -1;
        }
      }
      else if (str2 == null) {
        return 1;
      }

      // Both not null, so use the internal lexicographical string comparison
      return str1.compareTo(str2);
    }

  }

}
