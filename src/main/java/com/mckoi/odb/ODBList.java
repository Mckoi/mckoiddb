/**
 * com.mckoi.sdb.ODBList  Aug 2, 2010
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

/**
 * An ordered set of items, either sorted by the 128-bit reference value of
 * the items in the list, or by an order specification based on a key value
 * of the item. An ODBList is a useful element to create a database index,
 * or to reference a variable set of associated items in an object.
 * <p>
 * ODBList must support fast iteration and index addressing. ODBList must also
 * support views via the 'sub', 'head' and 'tail' methods.
 * <p>
 * An implementation of ODBList may put constraints on items stored in the
 * list, such as not allowing duplicate values of the same item or key value
 * to be inserted into the list. Such constraints are enforced both when
 * the list is immediately mutated by the client and also when a transaction
 * is committed, at which time the activity of concurrent transactions
 * are considered.
 *
 * @author Tobias Downer
 */

public interface ODBList extends Iterable<ODBObject> {

  /**
   * Returns the class of the elements stored in this list.
   */
  ODBClass getElementClass();

  /**
   * Returns the size of the list.
   */
  long size();

  /**
   * Returns true if the list is empty.
   */
  boolean isEmpty();

  /**
   * Returns the first reference stored in this list.
   */
  Reference first();

  /**
   * Returns the last reference stored in this list.
   */
  Reference last();

  /**
   * Returns an iterator over references in this list.
   */
  ODBListIterator iterator();

  /**
   * Adds a new element to the list at the ordered position in the list. The
   * referenced element must match the class returned by 'getElementClass()'.
   * Throws a constraint violation exception if the entry could not be added.
   */
  void add(Reference ref) throws ConstraintViolationException;

  /**
   * Adds the object element to the list at the ordered position in the list.
   * The object must match the class returned by 'getElementClass()'.
   * Throws a constraint violation exception if the entry could not be added.
   */
  void add(ODBObject value) throws ConstraintViolationException;



  /**
   * Removes the first element from the list that matches the reference. The
   * referenced object must be stored in this list.
   * <p>
   * If an entry with the given reference is not found returns false,
   * otherwise returns true. Note that if the list contains duplicates,
   * this will only remove the first. Use 'removeAll' to remove all the
   * entries with the given reference.
   */
  boolean remove(Reference ref);

  /**
   * Removes all the elements from the list of the given reference.
   * <p>
   * If there are no entries in the list with the given reference then
   * false is returned. This is equivalent to the 'remove' function for
   * lists that do not allow duplicates.
   */
  boolean removeAll(Reference ref);

  /**
   * Returns true if the list contains at least one element with the given
   * reference.
   */
  boolean contains(Reference ref);

  /**
   * Returns the reference stored at the given position in this list.
   */
  Reference get(long index);

  /**
   * Returns the reference stored at the given position as an ODBObject.
   */
  ODBObject getObject(long index);

  /**
   * Returns the index of the first reference that matches 'ref' in the
   * list, or -(pos + 1) if no entries with the reference are found where
   * pos is index the entry with that reference would be found.
   */
  long indexOf(Reference ref);

  /**
   * Returns the index of the last reference that matches 'ref' in the
   * list, or -(pos + 1) if no entries with the reference are found where
   * pos is index the entry with that reference would be found.
   * <p>
   * This is equivalent to 'indexOf' for lists that do not allow duplicates.
   */
  long lastIndexOf(Reference ref);

  /**
   * Returns an ODBList that is a subset of this list between the given
   * positions.
   *
   * @param from_ref the minimum reference of the subset (inclusive)
   * @param to_ref the maximum reference of the subset (exclusive)
   */
  ODBList sub(Reference from_ref, Reference to_ref);

  /**
   * Returns an ODBList that is a subset of this list between the given
   * position and the end of the list.
   *
   * @param from_ref the reference of the start of the subset (inclusive)
   */
  ODBList tail(Reference from_ref);

  /**
   * Returns an ODBList that is a subset of this list between the start
   * of the list and the given position.
   *
   * @param to_ref the reference of the end of the subset (exclusive)
   */
  ODBList head(Reference to_ref);

  // ----- Key value query methods

  // These methods only supported by lists with an order specification on
  // an object's key value.

  /**
   * Removes the first element from the list that has a key that matches the
   * given string. This only works if the order specification is on the key
   * of stored objects.
   * <p>
   * If an entry with the given key is not found returns false, otherwise
   * returns true. Note that if the list contains duplicates,
   * this will only remove the first. Use 'removeAll' to remove all the
   * entries with the given key value.
   */
  boolean remove(String key_value);

  /**
   * Removes all the elements from the list that have the given key. This
   * only works if the order specification is on the key of stored objects.
   * <p>
   * If there are no entries in the list with the key value then false is
   * returned. This is equivalent to the 'remove' function for lists that do
   * not allow duplicates.
   */
  boolean removeAll(String key_value);

  /**
   * Returns true if the list contains at least one element with the given
   * key value. This only works if the order specification is on the key of
   * stored objects.
   */
  boolean contains(String key_value);

  /**
   * Returns a reference to the first entry in the list that contains the
   * given key value. This only works if the order specification is on the
   * key of stored objects.
   * <p>
   * Returns null if the key value is not present in the list.
   */
  Reference get(String key_value);

  /**
   * Returns the first entry in the list that contains the given key value.
   * This only works if the order specification is on the key of stored
   * objects.
   * <p>
   * Returns null if the key value is not present in the list.
   */
  ODBObject getObject(String key_value);

  /**
   * Returns the index of the first element that contains the key value
   * in the list, or -(pos + 1) if no entries with the key value are found
   * where pos is the index the entry with that key value would be found.
   */
  long indexOf(String key_value);

  /**
   * Returns the index of the last element that contains the key value
   * in the list, or -(pos + 1) if no entries with the key value are found
   * where pos is the index the entry with that key value would be found.
   */
  long lastIndexOf(String key_value);

  /**
   * Returns an ODBList that is a subset of this list between the given
   * positions.
   *
   * @param from_key the minimum key value of the subset (inclusive)
   * @param to_key the maximum key value of the subset (exclusive)
   */
  ODBList sub(String from_key, String to_key);

  /**
   * Returns an ODBList that is a subset of this list between the given
   * position and the end of the list.
   *
   * @param from_key the key value of the start of the subset (inclusive)
   */
  ODBList tail(String from_key);

  /**
   * Returns an ODBList that is a subset of this list between the start
   * of the list and the given position.
   *
   * @param to_key the key value of the end of the subset (exclusive)
   */
  ODBList head(String to_key);

}
