/**
 * com.mckoi.network.BlockStore  Jul 17, 2009
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

package com.mckoi.network;

import java.io.IOException;

/**
 * An interface for fetching and writing the nodes stored in a block storage
 * component.
 *
 * @author Tobias Downer
 */

interface BlockStore {

  /**
   * Opens the block store. Returns true if a new object has to be created,
   * false if we opened an existing archive.
   */
  boolean open() throws IOException;

  /**
   * Closes the block store.
   */
  void close() throws IOException;

  /**
   * Stores node data in the backed store with the given data_id. The data
   * may be a maximum of 65535 bytes in length. The 'data_id' may be between 0
   * and 16383 (the maximum number of nodes that can be stored per block).
   * <p>
   * This will generate an exception if the store implementation doesn't
   * support writing new data items, or if an attempt is made to overwrite a
   * data item.
   */
  void putData(int data_id, byte[] buf, int off, int len) throws IOException;

  /**
   * Fetches the data stored with the given data_id stored within this block.
   * Generates an exception if the data id is outside the permitted range of
   * ids that can be stored in this block (0 to 16383). Returns a zero length
   * node item for node data that was allocated but not written to.
   * <p>
   * The data_id will be found within the set of nodes returned in the NodeSet.
   * It will not necessarily be found in a predictable position in the returned
   * set. The items in the node set must be iterated through to find the
   * requested data item.
   * <p>
   * If the node is empty, this method should generate a BlockReadException
   * exception but it is not required to do so.
   */
  NodeSet getData(int data_id) throws IOException;

  /**
   * Returns the maximum extent of data_ids stored in this block store. If
   * this is block store that can be written to, and therefore the extent
   * can not be known, an exception is generated.
   */
  int getMaxDataId() throws IOException;

  /**
   * Removes the data with the given data_id stored within this block. This
   * only removes the pointer to the data, not the actual data itself which is
   * left remaining in the block container. To reclaim the resources for
   * deleted nodes, the block container needs to be rewritten.
   * <p>
   * Generates an exception if data_id is out of range (valid range is 0 to
   * 16383), or if the implementing store does not permit deleting data.
   */
  boolean removeData(int data_id) throws IOException;

  /**
   * Creates a 64-bit checksum from all the node data recorded in this block
   * store. Uses Adler32 to generate the checksum value.
   */
  long createChecksumValue() throws IOException;

  /**
   * Performs a file synchronize on this block store, ensuring that any data
   * is flushed onto the disk.
   */
  void fsync() throws IOException;

}
