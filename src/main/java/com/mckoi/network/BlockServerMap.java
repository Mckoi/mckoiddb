/**
 * com.mckoi.network.BlockServerMap  Nov 25, 2008
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

import com.mckoi.data.*;
import java.util.ArrayList;

/**
 * A DataFile wrapper implementation of FixedSizeSerialSet that implements a
 * sorted list of block_id (long) to server_id (long) mappings for all
 * block items stored in the network.
 *
 * @author Tobias Downer
 */

public class BlockServerMap extends FixedSizeSerialSet {

  /**
   * Constructor.
   */
  public BlockServerMap(DataFile data) {
    super(data, 24);
  }

  /**
   * Put a block_id -> server_id association in the map. Returns true if the
   * record was added, false if the record couldn't be added because the
   * association already exists.
   */
  public boolean put(BlockId block_id, long server_id) {
    // Assert the server_id is positive
    if (server_id < 0) {
      throw new RuntimeException("Negative server_id");
    }

    // Search for the record
    RecordItem item = new RecordItem(block_id, server_id);
    long p = searchForRecord(item);

    // If the record was found,
    if (p >= 0) {
      return false;
    }
    // If the record wasn't found, insert it
    p = -(p + 1);
    insertEmpty(p);
    positionOn(p);
    getDataFile().putLong(block_id.getHighLong());
    getDataFile().putLong(block_id.getLowLong());
    getDataFile().putLong(server_id);

    return true;
  }

  /**
   * Put a block_id -> server_id association in the map. Returns true if the
   * record was added, false if the record couldn't be added because the
   * association already exists.
   */
  public boolean put(BlockId block_id, long[] server_ids) {
    boolean b = true;
    for (long server_id : server_ids) {
      b &= put(block_id, server_id);
    }
    return b;
  }

  /**
   * Returns the list of all servers that currently hold the block, or an
   * empty array if there are no servers recorded in the database for the
   * block.
   */
  public long[] get(BlockId block_id) {
    // Search for the first record item
    RecordItem item = new RecordItem(block_id, 0);
    long p = searchForRecord(item);
    if (p < 0) {
      // If the record wasn't found, we set p to the insert location
      p = -(p + 1);
    }
//    System.out.println("Search for " + block_id + " found at " +p);
    
    // The list of servers,
    ArrayList<Long> server_id_list = new ArrayList();
    // Fetch the records,
    DataFile dfile = getDataFile();
    long size = dfile.size();
    long loc = p * getRecordSize();
    dfile.position(loc);
    while (loc < size) {
      long read_block_id_h = dfile.getLong();
      long read_block_id_l = dfile.getLong();
      BlockId read_block_id = new BlockId(read_block_id_h, read_block_id_l);
      long read_server_id = dfile.getLong();
      // If we've read a record that isn't the block id we are searching for,
      // break the loop
      if (!read_block_id.equals(block_id)) {
        break;
      }
      // Add the server id of matching blocks
      server_id_list.add(read_server_id);
      loc += getRecordSize();
    }

    // Populate the result array,
    int sz = server_id_list.size();
    long[] result_arr = new long[sz];
    for (int i = 0; i < sz; ++i) {
      result_arr[i] = server_id_list.get(i);
    }
    // And return it
    return result_arr;
  }

  /**
   * Removes all the block_id pairs from the map, and returns the number of
   * entries removed.
   */
  public int remove(BlockId block_id) {
    // Search for the first record item
    RecordItem item = new RecordItem(block_id, 0);
    long p = searchForRecord(item);
    if (p < 0) {
      // If the record wasn't found, we set p to the insert location
      p = -(p + 1);
    }

    // Fetch the records,
    DataFile dfile = getDataFile();
    final long size = dfile.size();
    final long start_loc = p * getRecordSize();
    long loc = start_loc;
    int count = 0;
    dfile.position(loc);
    while (loc < size) {
      long read_block_id_h = dfile.getLong();
      long read_block_id_l = dfile.getLong();
      BlockId read_block_id = new BlockId(read_block_id_h, read_block_id_l);
      long read_server_id = dfile.getLong();
      // If we've read a record that isn't the block id we are searching for,
      // break the loop

      if (!read_block_id.equals(block_id)) {
        break;
      }
      // Add this record to the area being deleted,
      loc += getRecordSize();
      ++count;
    }

    // Remove the area
    if ((start_loc - loc) != 0) {
      dfile.position(loc);
      dfile.shift(start_loc - loc);
    }

    // Return the count
    return count;
  }

  /**
   * Removes the block_id / server_id pair from the map. Returns true if
   * the pair was found and removed, false if the pair wasn't found.
   */
  public boolean remove(BlockId block_id, long server_id) {
    // Search for the first record item
    RecordItem item = new RecordItem(block_id, server_id);
    long p = searchForRecord(item);
    if (p < 0) {
      // Not found, return false
      return false;
    }
    // Remove the record,
    removeRecordAt(p);
    // Return true,
    return true;
  }

  /**
   * Returns the last block id stored in the map.
   */
  public BlockId lastBlockId() {
    long p = size() - 1;
    RecordItem item = (RecordItem) getRecordKey(p);
    return item.block_id;
  }

  /**
   * Returns the range of maps as block_id -> server_id pairs as a String array
   * where each entry is formatted as '[block_id]=[service_id]'
   */
  public String[] getRange(long p1, long p2) {
    if ((p2 - p1) > Integer.MAX_VALUE) {
      throw new RuntimeException("Overflow; p2 - p1 > Integer.MAX_VALUE");
    }

    int sz = (int) (p2 - p1);
    String[] arr = new String[sz];
    for (int p = 0; p < sz; ++p) {
      RecordItem item = (RecordItem) getRecordKey(p1 + p);
      arr[p] = item.block_id.toString() + "=" +
               Long.toHexString(item.server_id);
    }
    return arr;
  }

  /**
   * Returns a consecutive range of BlockId key and values stored in this map
   * from the first block id key given (inclusive). Returns an Object[] that
   * contains an ArrayList of BlockId keys, and an ArrayList of Long values,
   * the same size representing the mappings,
   */
  public Object[] getKeyValueChunk(BlockId min, final int range_size) {

    ArrayList<BlockId> keys = new ArrayList();
    ArrayList<Long> values = new ArrayList();

    // Search for the first record item
    RecordItem item = new RecordItem(min, 0);
    long p = searchForRecord(item);
    if (p < 0) {
      // If the record wasn't found, we set p to the insert location
      p = -(p + 1);
    }
    // Fetch the records,
    DataFile dfile = getDataFile();
    long size = dfile.size();
    long start_loc = p * getRecordSize();
    long loc = start_loc;
    int count = 0;
    dfile.position(loc);

    BlockId last_block_id = null;

    while (count < range_size && loc < size) {
      long read_block_id_h = dfile.getLong();
      long read_block_id_l = dfile.getLong();
      BlockId read_block_id = new BlockId(read_block_id_h, read_block_id_l);
      long read_server_id = dfile.getLong();

      // Count each time we go to a new block,
      if (last_block_id == null || !last_block_id.equals(read_block_id)) {
        last_block_id = read_block_id;
        ++count;
      }

      keys.add(read_block_id);
      values.add(read_server_id);

      // Add this record to the area being deleted,
      loc += getRecordSize();
    }

    return new Object[] { keys, values };
  }

  /**
   * For debugging.
   */
  public String debugString() {
    StringBuffer buf = new StringBuffer();
    long sz = size();
    for (int i = 0; i < sz; ++i) {
      RecordItem item = (RecordItem) getRecordKey(i);
      buf.append(i);
      buf.append("> ");
      buf.append(item.block_id);
      buf.append(" ");
      buf.append(item.server_id);
      buf.append("\n");
    }
    return buf.toString();
  }
  
  
  // ---------- Implemented from FixedSizeSerialSet ----------

  protected Object getRecordKey(long record_pos) {
    positionOn(record_pos);
    long block_id_h = getDataFile().getLong();
    long block_id_l = getDataFile().getLong();
    BlockId block_id = new BlockId(block_id_h, block_id_l);
    long server_id = getDataFile().getLong();
    return new RecordItem(block_id, server_id);
  }

  protected int compareRecordTo(long record_pos, Object record_key) {
    positionOn(record_pos);
    long src_block_id_h = getDataFile().getLong();
    long src_block_id_l = getDataFile().getLong();
    BlockId src_block_id = new BlockId(src_block_id_h, src_block_id_l);
    long src_server_id = getDataFile().getLong();
    RecordItem dst_item = (RecordItem) record_key;
    BlockId dst_block_id = dst_item.block_id;

    int cmp = src_block_id.compareTo(dst_block_id);
    if (cmp > 0) {
      return 1;
    }
    else if (cmp < 0) {
      return -1;
    }
    else {
      // If identical block items, sort by the server identifier
      long dst_server_id = dst_item.server_id;
      if (src_server_id > dst_server_id) {
        return 1;
      }
      else if (src_server_id < dst_server_id) {
        return -1;
      }
      else {
        // Equal,
        return 0;
      }
    }
  }

  // ---------- key/value pair record ----------
  
  private static class RecordItem {
    private final BlockId block_id;
    private final long server_id;
    private RecordItem(BlockId block_id, long server_id) {
      this.block_id = block_id;
      this.server_id = server_id;
    }
    @Override
    public int hashCode() {
      return (int) (block_id.hashCode() + server_id);
    }
    @Override
    public boolean equals(Object ob) {
      if (this == ob) {
        return true;
      }
      RecordItem dest_ob = (RecordItem) ob;
      return (dest_ob.block_id.equals(block_id) &&
              dest_ob.server_id == server_id);
    }
  }

}
