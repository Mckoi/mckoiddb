/**
 * com.mckoi.network.CompressedBlockStore  Jul 16, 2009
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

import com.mckoi.data.NodeReference;
import com.mckoi.util.StrongPagedAccess;
import java.io.*;
import java.util.ArrayList;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * A block store containing compressed encoded nodes.
 *
 * @author Tobias Downer
 */

public class CompressedBlockStore implements BlockStore {

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
   * The size of the store.
   */
  private long content_size;

  /**
   * A buffer for accesses to parts of the underlying file.
   */
  private StrongPagedAccess paged_content;

  /**
   * Constructs the block store.
   */
  CompressedBlockStore(BlockId block_id, File f) {
    this.block_id = block_id;
    this.store = f;
  }

  /**
   * Opens the block store.
   */
  @Override
  public boolean open() throws IOException {
    // If the store file doesn't exist, throw an error. We can't create
    // compressed files, they are made by calling the 'compress'.
    if (!store.exists()) {
      throw new RuntimeException("Compressed file doesn't exist: " + store);
    }
    else {
      content = new RandomAccessFile(store, "r");
      content_size = content.length();
      paged_content = new StrongPagedAccess(content, 2048);
      return false;
    }
  }

  /**
   * Closes the block store.
   */
  @Override
  public void close() throws IOException {
    content.close();
    content = null;
    paged_content = null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getLastModified() {
    return store.lastModified();
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

    throw new RuntimeException("Not supported in compressed store.");

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

    try {
      int data_p = data_id;
      int pos = data_p * 6;
      int did_pos = paged_content.readInt(pos);
      int did_len = ((int) paged_content.readShort(pos + 4)) & 0x0FFFF;

      if (did_pos < 0) {
        data_p = -(did_pos + 1);
        pos = data_p * 6;
        did_pos = paged_content.readInt(pos);
        did_len = ((int) paged_content.readShort(pos + 4)) & 0x0FFFF;
      }

      // Fetch the node set,
      ArrayList<Integer> node_ids = new ArrayList(24);
      node_ids.add(data_p);
      while (true) {
        ++data_p;
        pos += 6;
        int check_v = paged_content.readInt(pos);
        if (check_v < 0) {
          node_ids.add(data_p);
        }
        else {
          break;
        }
      }

      // Turn it into a node array,
      int sz = node_ids.size();
      NodeReference[] lnode_ids = new NodeReference[sz];
      for (int i = 0; i < sz; ++i) {
        DataAddress daddr = new DataAddress(block_id, node_ids.get(i));
        lnode_ids[i] = daddr.getValue();
      }

      // Read the encoded form into a byte[] array,
      byte[] buf = new byte[did_len];
      content.seek(did_pos);
      content.readFully(buf, 0, did_len);

      // Return it,
      return new CompressedNodeSet(lnode_ids, buf);

    }
    catch (IOException e) {
      // We wrap this IOException around a BlockReadException. This can only
      // indicate a corrupt compressed block file or access to a data_id that
      // is out of range of the nodes stored in this file.
      throw new BlockReadException("IOError reading data from block file", e);
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getMaxDataId() throws IOException {

    // Read the header from the file until we hit 0/0 entry.
    int data_p = 0;

    while (true) {
      int pos = data_p * 6;
      int did_pos = paged_content.readInt(pos);
      int did_len = ((int) paged_content.readShort(pos + 4)) & 0x0FFFF;

      // Did we hit 0/0 entry?
      if (did_pos == 0 && did_len == 0) {
        return data_p - 1;
      }

      // Go to next,
      ++data_p;
    }

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

    throw new RuntimeException("Not supported in compressed store.");
  }

  /**
   * Creates a 64-bit checksum from all the node data recorded in this block
   * store. Uses Adler32 to generate the checksum value.
   */
  @Override
  public long createChecksumValue() throws IOException {
    // PENDING,
    // We need to go through and decompress all the data in the block to
    // generate the checksum.
    throw new RuntimeException("PENDING");
  }

  /**
   * Performs a file synchronize on this block store, ensuring that any data
   * is flushed onto the disk.
   */
  @Override
  public void fsync() throws IOException {
    // Not implemented in a CompressedBlockStore,
  }








  /**
   * Compresses a source file (formatted as a MutableBlockStore) and writes a
   * destination file containing the compressed data in the format necessary
   * for CompressedBlockStore.
   */
  static void compress(
                 File source_file, File destination_file) throws IOException {
    // Set up the input streams,
    FileInputStream fin = new FileInputStream(source_file);
    DataInputStream din = new DataInputStream(new BufferedInputStream(fin));

    int[] pos = new int[16384];
    short[] lens = new short[16384];
    boolean[] empty = new boolean[16384];

    // Read the header,
    int last_header_item = 0;
    for (int n = 0; n < 16384; ++n) {
      pos[n] = din.readInt();
      lens[n] = din.readShort();
      if (pos[n] != 0) {
        last_header_item = n + 1;
      }
      else {
        empty[n] = true;
      }
    }

    fin.close();

    // Create the compressed file,
    if (destination_file.exists()) {
      throw new RuntimeException(
                              "Destination file exists: " + destination_file);
    }
    destination_file.createNewFile();

    // Input file
    int header_size = (last_header_item + 1) * 6;
    ByteArrayOutputStream header_out = new ByteArrayOutputStream(header_size);
    DataOutputStream dheader_out = new DataOutputStream(header_out);

    {
      RandomAccessFile output_file =
                                 new RandomAccessFile(destination_file, "rw");
      output_file.setLength(header_size);
      output_file.seek(header_size);
      output_file.close();
    }
    FileOutputStream file_out = new FileOutputStream(destination_file, true);
    BufferedOutputStream file_bout = new BufferedOutputStream(file_out);

    // Input file,
    RandomAccessFile contents = new RandomAccessFile(source_file, "r");

    // The compression algorithm works as follows;

    Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION);

    ByteArrayOutputStream bout;

    int f_pos = header_size;

    for (int i = 0; i < last_header_item; ++i) {

      ByteArrayOutputStream bout_to_write = null;
      int compress_start;
      int compress_end;
      // For each node,
      int n = i + 1;
      while (true) {
        bout = new ByteArrayOutputStream(16384);
        deflater.reset();
        DeflaterOutputStream compress_out =
                                     new DeflaterOutputStream(bout, deflater);
        DataOutputStream data_compress_out =
                                           new DataOutputStream(compress_out);
        for (int p = i; p < n; ++p) {
          int node_pos = pos[p];
          if (node_pos > 0) {
            short node_len = lens[p];
            contents.seek(node_pos);
            byte[] node_buf = new byte[node_len];
            contents.readFully(node_buf);

            data_compress_out.write(node_buf);
          }
          else {
            // Make sure to handle the empty node,
            data_compress_out.writeShort(0);
          }
        }
        data_compress_out.flush();
        compress_out.finish();

        int compress_size = bout.size();
        if (n == last_header_item) {
          compress_start = i;
          compress_end = n;
          bout_to_write = bout;
          break;
        }
        // The compressed size can not go over 4096 bytes, or 24 nodes.
        else if (compress_size > 4096 || (n - i) > 24) {
          compress_start = i;
          compress_end = Math.max(i + 1, n - 1);
          if (n == i + 1) {
            bout_to_write = bout;
          }
          break;
        }
        bout_to_write = bout;
        ++n;
      }

      // Write the compressed packet out to the file
      bout_to_write.writeTo(file_bout);

      int entry_count = (compress_end - compress_start);

      dheader_out.writeInt(f_pos);
      dheader_out.writeShort(bout_to_write.size());
      for (int p = 1; p < entry_count; ++p) {
        dheader_out.writeInt(-(i + 1));
        dheader_out.writeShort(0);
      }

      f_pos += bout_to_write.size();

      i = compress_end - 1;
    }

    // The final header element
    dheader_out.writeInt(0);
    dheader_out.writeShort(0);

    dheader_out.flush();
    dheader_out.close();
    file_bout.flush();
    file_bout.close();

    // Close the contents file,
    contents.close();

    // Write out the header,
    {
      RandomAccessFile output_file =
                                 new RandomAccessFile(destination_file, "rw");
      output_file.seek(0);
      output_file.write(header_out.toByteArray());
      // Sync the changes,
      try {
        output_file.getFD().sync();
      }
      catch (SyncFailedException e) {
        // Sync failed exception is ignored,
      }
      output_file.close();
    }

    // Done.
  }

  /**
   * Procedure that compresses a MutableBlockStore.
   */
  static void compress(MutableBlockStore source, File dest_file)
                                                          throws IOException {
    compress(source.getFile(), dest_file);
  }

//  public static void main(String[] args) {
//    try {
//      String file_name = args[0];
//
//      File in_file = new File(file_name);
//      File out_file = new File(in_file.getParent(), in_file.getName() + ".mcd");
//
//      out_file.delete();
////      out_file.createNewFile();
//
//      compress(in_file, out_file);
//    }
//    catch (IOException e) {
//      e.printStackTrace();
//    }
//  }

}
