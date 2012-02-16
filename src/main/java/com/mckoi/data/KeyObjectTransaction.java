/**
 * com.mckoi.treestore.KeyObjectTransaction  Dec 29, 2007
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
 * KeyObjectTransaction is an isolated snapshot of a database that can be
 * modified locally and the changes published in a commit operation. Any
 * changes made to a data structure that implements KeyObjectTransaction must
 * be immediately reflected in the view. All changes made to a transaction
 * must also be fully isolated from the system as a whole (a change in
 * one transaction will not change the content of any other transaction
 * that is concurrently being used).
 * <p>
 * NOTE: Objects that implement KeyObjectTransaction are <b>NOT</b> intended
 *   to be thread safe, nor are the DataFile or DataRange objects that are
 *   created either exclusive or inclusive of other DataFile and DataRange
 *   objects created by this object. For example, one DataFile object may not
 *   be changed while another is being written to at the same time by a
 *   different thread on the same transaction object. However, a transaction
 *   object is isolated from all other transaction objects in the system, so
 *   a multi-threaded system may implement concurrent access to the data
 *   source provided it ensures that multiple threads do not share the same
 *   transaction object instance.
 * <p>
 * KeyObjectTransaction implementations are intended to be lightweight and
 * cheap to create and dispose.
 *
 * @author Tobias Downer
 */

public interface KeyObjectTransaction {

  /**
   * Returns true if a DataFile with the given key exists.  This is exactly
   * the same as (getDataFile(key, 'r').size() > 0).
   */
  public boolean dataFileExists(Key key);

  /**
   * Returns a DataFile for the given key.  This method can be called multiple
   * times to create multiple DataFile objects with their own positional state.
   * Modifying the contents of a DataFile will mirror the change over all the
   * DataFiles created on the same key.
   * <p>
   * If mode is 'r', the DataFile is read-only and generates an exception if
   * it is written to.
   * <p>
   * The returned object must provide efficient implementations for all the
   * operations specified in DataFile, including the resize, shift and
   * copy operations.
   */
  public AddressableDataFile getDataFile(Key key, char mode);

  /**
   * Returns a DataRange object that describes all the keys stored in the
   * database between and including the minimum and maximum key range given.
   * This method may be called multiple times to create multiple DataRange
   * objects. If a DataFile that is part of the key range is modified, the
   * change is reflected immediately in all the DataRange objects created that
   * have coverage on that key.
   * <p>
   * The returned object must provide efficient implementations for all the
   * operations in the interface.
   */
  public DataRange getDataRange(Key min_key, Key max_key);

  /**
   * Returns a DataRange object that describes the entire set of keys stored
   * in this database. If a DataFile that is part of the key range is modified,
   * the change is reflected immediately in all the DataRange objects created
   * that have coverage on that key.
   * <p>
   * The returned object must provide efficient implementations for all the
   * operations in the interface.
   */
  public DataRange getDataRange();

  /**
   * Provides a hint that DataFile objects with the given keys may be accessed
   * shortly and their content or meta-data should be prefetched in
   * preparation. Prefetch is intended to reduce latency when it's known
   * the likelyhood of the order of objects being accessed.
   * <p>
   * How this implementation decides to preload the data is up to the
   * implementation specifics. The implementation can decide to ignore this
   * hint altogether.
   */
  public void prefetchKeys(Key[] keys);

}
