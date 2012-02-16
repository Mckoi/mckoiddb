/**
 * com.mckoi.data.AddressableDataFile  Feb 11, 2011
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
 * A DataFile that is addressable in a KeyObjectDatabase.
 *
 * @author Tobias Downer
 */

public interface AddressableDataFile extends DataFile {

  /**
   * Returns an object that describes a fully qualified addressable location
   * of the data representing this DataFile from the start position
   * (inclusive) to the end position (exclusive). The object returned is
   * entirely implementation specific, and is used by the 'copyFrom' and
   * 'replicateFrom' methods to optimize the block replication methods.
   * Calling 'df.getBlockLocationMeta(0, df.size())' will return a meta object
   * describing the complete file content.
   * <p>
   * This method may return null, which signifies that the content of the
   * file is not eligible to be replicated using a block replication method.
   * For example, a DataFile that has content entirely on the Java Heap
   * should return null for this method.
   * <p>
   * For security reasons, the object returned should not expose information
   * about the data being addressed.
   */
  Object getBlockLocationMeta(long start_position, long end_position);

}
