/**
 * com.mckoi.treestore.Iterator64Bit  01 Nov 2008
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

package com.mckoi.data;

/**
 * An iterator interface for forward and backward traversal of a sequence of
 * 64-bit values. Iterator64Bit also supports removal of values from the
 * backed collection, and free location assignment of the iterator pointer.
 *
 * @author Tobias Downer
 */

public interface Iterator64Bit {

  /**
   * Returns the total number of 64-bit values in the group of values
   * defined by the scope of this iterator.
   */
  long size();

  /**
   * Moves the position location to the given offset in the iterator address
   * space where 0 will position the iterator to the first location, and -1 to
   * before the first location.  Note that for the 'next' method to return the
   * first value in the collection, the position should be set to -1.  For the
   * 'previous' method to return the last value in the set, the position must
   * be set to size().
   *
   * @param position the position to move to.
   */
  void position(long p);

  /**
   * Returns the current position of the iterator within the address space as
   * initially defined when the iterator was created and after any removal
   * operations (from calls to 'remove') have been performed.  A new iterator
   * will always return -1 (before the first entry).
   */
  long position();

  /**
   * Returns true if there is a value that can be read from a call to the
   * 'next' method.  Returns false if the iterator is at the end of the
   * collection and a value may not be read.
   */
  boolean hasNext();

  /**
   * Returns the next 64-bit from the backed collection and moves the position
   * location to the next value in the set.
   */
  long next();

  /**
   * Returns true if there is a value that can be read by a call to the
   * 'previous' method.  Returns false if the iterator is at the start of the
   * collection and a value may not be read.
   */
  boolean hasPrevious();

  /**
   * Returns the previous 64-bit from the backed collection and moves the
   * position location to the previous value in the collection.
   */
  long previous();

  /**
   * Returns a copy of this iterator over an identical collection of values but
   * with a positional pointer that can be moved independently of this
   * iterator.
   */
  Iterator64Bit copy();


  /**
   * Removes a value from the set at the current position.  This should be used
   * immediately after a 'next' or 'previous' call to remove the value that
   * was last accessed.  Note that the call itself acts as a 'next' operation -
   * when this method returns the interator position will point to the next
   * value in the collection.
   */
  long remove();

}

