/**
 * com.mckoi.odb.ReferenceKeyLookup  Jan 1, 2011
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

import com.mckoi.data.DataFile;
import com.mckoi.data.FixedSizeSerialSet;
import com.mckoi.data.Key;
import com.mckoi.network.BlockId;
import java.util.ArrayList;

/**
 * A fixed size record structure that is a one way map of 16-byte Reference
 * values to 16-byte Key objects.
 *
 * @author Tobias Downer
 */

class ReferenceKeyLookup extends FixedSizeSerialSet {

  /**
   * Constructor.
   */
  public ReferenceKeyLookup(DataFile data) {
    super(data, 32);
  }

  /**
   * Put a reference -> key association in the map. Returns true if the
   * record was added, false if the record couldn't be added because the
   * association already exists.
   */
  public boolean put(Reference reference, Key key) {

    // Search for the record
    RecordItem item = new RecordItem(reference, key);
    long p = searchForRecord(item);

    // If the record was found,
    if (p >= 0) {
      return false;
    }
    // If the record wasn't found, insert it
    p = -(p + 1);
    insertEmpty(p);
    positionOn(p);
    getDataFile().putLong(reference.getHighLong());
    getDataFile().putLong(reference.getLowLong());
    getDataFile().putShort(key.getType());
    getDataFile().putInt(key.getSecondary());
    getDataFile().putLong(key.getPrimary());

    return true;
  }

  /**
   * Returns a key given a reference.
   */
  public Key get(Reference ref) {
    // Search for the first record item
    RecordItem item = new RecordItem(ref, null);
    long p = searchForRecord(item);
    if (p < 0) {
      // If the record wasn't found, we set p to the insert location
      p = -(p + 1);
    }
//    System.out.println("Search for " + block_id + " found at " +p);

    // Fetch the records,
    DataFile dfile = getDataFile();
    long size = dfile.size();
    long loc = p * getRecordSize();
    dfile.position(loc);
    if (loc < size) {
      long read_ref_h = dfile.getLong();
      long read_ref_l = dfile.getLong();
      Reference read_ref = new Reference(read_ref_h, read_ref_l);
      // If we've read a record that isn't the reference, return null (not
      // found).
      if (!read_ref.equals(ref)) {
        return null;
      }
      // Otherwise read the key,
      short key_1 = dfile.getShort();
      int key_2 = dfile.getInt();
      long key_3 = dfile.getLong();
      Key read_key = new Key(key_1, key_2, key_3);
      // And return it,
      return read_key;
    }

    // End reached, so return null (not found).
    return null;
  }

  /**
   * Removes all the reference pairs from the map, and returns the number of
   * entries removed.
   */
  public int remove(Reference ref) {
    // Search for the first record item
    RecordItem item = new RecordItem(ref, null);
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
    while (loc < size) {
      dfile.position(loc);
      long read_ref_h = dfile.getLong();
      long read_ref_l = dfile.getLong();
      Reference read_ref = new Reference(read_ref_h, read_ref_l);
      // If we've read a record that isn't the ref we are searching for,
      // break the loop
      if (!read_ref.equals(ref)) {
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
   * For debugging.
   */
  public String debugString() {
    StringBuffer buf = new StringBuffer();
    long sz = size();
    for (int i = 0; i < sz; ++i) {
      RecordItem item = (RecordItem) getRecordKey(i);
      buf.append(i);
      buf.append("> ");
      buf.append(item.ref);
      buf.append(" ");
      buf.append(item.key);
      buf.append("\n");
    }
    return buf.toString();
  }


  // ---------- Implemented from FixedSizeSerialSet ----------

  protected Object getRecordKey(long record_pos) {
    positionOn(record_pos);
    long ref_h = getDataFile().getLong();
    long ref_l = getDataFile().getLong();
    Reference ref = new Reference(ref_h, ref_l);
    short key_1 = getDataFile().getShort();
    int key_2 = getDataFile().getInt();
    long key_3 = getDataFile().getLong();
    return new RecordItem(ref, new Key(key_1, key_2, key_3));
  }

  protected int compareRecordTo(long record_pos, Object record_key) {
    positionOn(record_pos);
    long src_ref_h = getDataFile().getLong();
    long src_ref_l = getDataFile().getLong();
    Reference src_ref = new Reference(src_ref_h, src_ref_l);
//    short src_key_1 = getDataFile().getShort();
//    int src_key_2 = getDataFile().getInt();
//    long src_key_3 = getDataFile().getLong();
//    Key src_key = new Key(src_key_1, src_key_2, src_key_3);

    RecordItem dst_item = (RecordItem) record_key;
    Reference dst_ref = dst_item.ref;

    int cmp = src_ref.compareTo(dst_ref);
    if (cmp > 0) {
      return 1;
    }
    else if (cmp < 0) {
      return -1;
    }
    else {
      return 0;
//      // If identical ref, sort by key
//      Key dst_key = dst_item.key;
//      cmp = src_key.compareTo(dst_key);
//      if (cmp > 0) {
//        return 1;
//      }
//      else if (cmp < 0) {
//        return -1;
//      }
//      else {
//        return 0;
//      }
    }
  }

  // ---------- key/value pair record ----------

  private static class RecordItem {
    private final Reference ref;
    private final Key key;
    private RecordItem(Reference ref, Key key) {
      this.ref = ref;
      this.key = key;
    }
    @Override
    public int hashCode() {
      if (key != null) {
        return (int) (ref.hashCode() + key.hashCode());
      }
      else {
        return ref.hashCode();
      }
    }
    @Override
    public boolean equals(Object ob) {
      if (this == ob) {
        return true;
      }
      RecordItem dest_ob = (RecordItem) ob;
      
      if (dest_ob.ref.equals(ref)) {
        return true;
      }
      
      // If there's a null
      if (key == null || dest_ob.key == null) {
        // If both null,
        if (key == null && dest_ob.key == null) {
          return dest_ob.ref.equals(ref);
        }
        // If one is null and one is not null, can't be equal,
        else {
          return false;
        }
      }
      
      // No nulls,
      return (dest_ob.key.equals(key));
    }
  }

}
