/**
 * com.mckoi.network.MutableBlockStore  Dec 14, 2008
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

import com.mckoi.util.ByteArrayUtil;
import com.mckoi.util.StrongPagedAccess;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.Adler32;

/**
 * A storage component that stores a series of uncompressed nodes in a block
 * using the standard java IO facilities to store the information.
 * <p>
 * There is a one-to-one mapping between a block and the file that is the
 * storage component in the file system. The file contains two parts, the
 * table that points to the node stored data, and the stored data itself. The
 * stored data is appended to the end of the file as allocation requests are
 * made.
 * <p>
 * This node storage device is intended for storing node information during
 * the earlier stage of its lifespan when it is being written to the network.
 *
 * @author Tobias Downer
 */

class MutableBlockStore implements BlockStore {

  /**
   * The size of the header area in the store.
   */
  static final int HEADER = 6 * 16384;

  /**
   * The block id value.
   */
  private final BlockId block_id;

  /**
   * The File in the local filesystem of the stored data.
   */
  private final File store;

  /**
   * The random access file that represents the block store.
   */
  private RandomAccessFile content;

  /**
   * The length of the file content.
   */
  private int content_len;

  /**
   * A buffer for accesses to parts of the underlying file.
   */
  private StrongPagedAccess paged_content;

  /**
   * Constructs the block store.
   */
  MutableBlockStore(BlockId block_id, File f) {
    this.block_id = block_id;
    this.store = f;
  }

  /**
   * Returns the file object,
   */
  File getFile() {
    return store;
  }

  /**
   * Opens the block store.
   */
  @Override
  public boolean open() throws IOException {
    // If the store file doesn't exist, create it
    if (!store.exists()) {
      store.createNewFile();
      // Set the header table in the newly created file,
      content = new RandomAccessFile(store, "rw");
      content.setLength(HEADER);
      content_len = HEADER;
      paged_content = new StrongPagedAccess(content, 2048);
      return true;
    }
    else {
      content = new RandomAccessFile(store, "rw");
      content_len = (int) content.length();
      paged_content = new StrongPagedAccess(content, 2048);
      return false;
    }
  }

  /**
   * Closes the block store.
   */
  @Override
  public void close() throws IOException {
//    System.out.print("[H:" + paged_content.getCacheHits() + " M:" + paged_content.getCacheMiss() + "]");
    content.close();
    content = null;
    content_len = 0;
    paged_content = null;
  }

  /**
   * Stores node data in the backed store with the given data_id. The data
   * may be a maximum of 65535 bytes in length limiting node length to this
   * size. 'data_id' may be between 0 and 16383 (the maximum number of nodes
   * that can be stored per block).
   */
  @Override
  public void putData(int data_id, byte[] buf, int off, int len)
                                                          throws IOException {
    // Arg checks
    if (len < 0 || len >= 65536) {
      throw new IllegalArgumentException("len < 0 || len > 65535");
    }
    if (len + off > buf.length) {
      throw new IllegalArgumentException();
    }
    if (off < 0) {
      throw new IllegalArgumentException();
    }
    if (data_id < 0 || data_id >= 16384) {
      throw new IllegalArgumentException("data_id out of range");
    }

    byte[] tmp_area = new byte[6];

    // Seek to the position of this data id in the table,
    final int pos = data_id * 6;
    int did_pos = paged_content.readInt(pos);
    int did_len = ((int) paged_content.readShort(pos + 4)) & 0x0FFFF;
//    content.seek(pos);
//    content.readFully(tmp_area, 0, 6);
//    int did_pos = ByteArrayUtil.getInt(tmp_area, 0);
//    int did_len = ((int) ByteArrayUtil.getShort(tmp_area, 4)) & 0x0FFFF;
    // These values should be 0, if not we've already written data here,
    if (did_pos != 0 || did_len != 0) {
      throw new RuntimeException("data_id previously written");
    }
    // Write the content to the end of the file,
    content.seek(content_len);
    content.write(buf, off, len);
    paged_content.invalidateSection(content_len, len);
    // Write the table entry,
    ByteArrayUtil.setInt(content_len, tmp_area, 0);
    ByteArrayUtil.setShort((short) len, tmp_area, 4);
    content.seek(pos);
    content.write(tmp_area, 0, 6);
    paged_content.invalidateSection(pos, 6);
    // Set the new content length
    content_len = content_len + len;
  }

  /**
   * Fetches the data stored with the given data_id stored within this block.
   * If no data is stored with the given data_id, a runtime exception is
   * generated.
   */
  @Override
  public NodeSet getData(int data_id) throws IOException {
    if (data_id < 0 || data_id >= 16384) {
      throw new IllegalArgumentException("data_id out of range");
    }

//    byte[] tmp_area = new byte[6];

    // Seek to the position of this data id in the table,
    final int pos = data_id * 6;
    int did_pos = paged_content.readInt(pos);
    int did_len = ((int) paged_content.readShort(pos + 4)) & 0x0FFFF;
//    content.seek(pos);
//    content.readFully(tmp_area, 0, 6);
//    int did_pos = ByteArrayUtil.getInt(tmp_area, 0);
//    int did_len = ((int) ByteArrayUtil.getShort(tmp_area, 4)) & 0x0FFFF;
    // If position for the data_id is 0, the data hasn't been written,
    byte[] buf = new byte[did_len];
    if (did_pos > 0) {
      // Fetch the content,
      content.seek(did_pos);
      content.readFully(buf, 0, did_len);
    }
    else {
      throw new BlockReadException("Data id " + data_id +
                                   " is empty (block " + block_id + ")");
    }

    // Return as a nodeset object,
    return new SingleUncompressedNodeSet(block_id, data_id, buf);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getMaxDataId() throws IOException {
    throw new BlockReadException(
                          "MutableBlockStore does not support 'getMaxDataId'");
  }

  /**
   * Removes the data with the given data_id stored within this block. This
   * only removes the pointer to the data, not the actual data itself which is
   * left remaining in the block container. To reclaim the resources for
   * deleted nodes, the block container needs to be rewritten.
   */
  @Override
  public boolean removeData(int data_id) throws IOException {
    if (data_id < 0 || data_id >= 16384) {
      throw new IllegalArgumentException("data_id out of range");
    }

    byte[] tmp_area = new byte[6];

    // Seek to the position of this data id in the table,
    final int pos = data_id * 6;
    int did_pos = paged_content.readInt(pos);
    int did_len = ((int) paged_content.readShort(pos + 4)) & 0x0FFFF;
//    content.seek(pos);
//    content.readFully(tmp_area, 0, 6);
//    int did_pos = ByteArrayUtil.getInt(tmp_area, 0);
//    int did_len = ((int) ByteArrayUtil.getShort(tmp_area, 4)) & 0x0FFFF;
    // Clear it,
    for (int i = 0; i < tmp_area.length; ++i) {
      tmp_area[i] = 0;
    }
    // Write the cleared entry in,
    content.seek(pos);
    content.write(tmp_area, 0, 6);
    paged_content.invalidateSection(pos, 6);
    // Return true if we deleted something,
    return did_pos != 0;
  }

  /**
   * Creates a 64-bit checksum from all the node data recorded in this block
   * store. Uses Adler32 to generate the checksum value.
   */
  @Override
  public long createChecksumValue() throws IOException {
    Adler32 alder1 = new Adler32();
    Adler32 alder2 = new Adler32();

    byte[] header_value = new byte[HEADER];
    content.seek(0);
    content.readFully(header_value, 0, HEADER);
    for (int i = 0; i < HEADER; i += 6) {
      int pos = ByteArrayUtil.getInt(header_value, i);
      int len = ((int) ByteArrayUtil.getShort(header_value, i + 4)) & 0x0FFFF;

      byte[] node = new byte[len];
      content.seek(pos);
      content.readFully(node, 0, len);

      Adler32 a;
      if ((i & 0x01) == 0) {
        a = alder1;
      }
      else {
        a = alder2;
      }
      a.update(node, 0, len);

    }

    // Return the 64 bit value checksum,
    return (((long) alder1.getValue()) << 32) | alder2.getValue();
  }

//  /**
//   * Returns the list of all data_ids stored in this block store.
//   */
//  public int[] getDataList() throws IOException {
//    ArrayList<Integer> list = new ArrayList();
//
//    // Fetch the header area of the store,
//    byte[] header_value = new byte[HEADER];
//    content.seek(0);
//    content.readFully(header_value, 0, HEADER);
//    for (int i = 0; i < HEADER; i += 6) {
//      int pos = ByteArrayUtil.getInt(header_value, i);
//      int len = ((int) ByteArrayUtil.getShort(header_value, i + 4)) & 0x0FFFF;
//
//      int n = (i / 6);
//      list.add(n);
//    }
//
//    // Turn 'list' into an int array.
//    int sz = list.size();
//    int[] ret_val = new int[sz];
//    int i = 0;
//    for (Integer val : list) {
//      ret_val[i] = val;
//      ++i;
//    }
//
//    // Return the list
//    return ret_val;
//  }

  /**
   * Performs a file synchronize on this block store, ensuring that any data
   * is flushed onto the disk.
   */
  @Override
  public void fsync() throws IOException {
    if (content != null) {
      content.getFD().sync();
    }
  }

  @Override
  public String toString() {
    return store.toString();
  }

}
