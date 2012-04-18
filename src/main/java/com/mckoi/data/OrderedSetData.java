/**
 * com.mckoi.data.OrderedSetData  Aug 4, 2010
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

import com.mckoi.util.ByteArrayBuilder;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * An ordered set of variable length data strings mapped over a single
 * DataFile object.  Set modifications are immediately reflected in the
 * underlying data file. This object grows and shrinks the size of the
 * underlying data file as values are inserted and removed from the set.
 * This is a convenient way to manage a set of arbitrary variable length
 * data objects in a single DataFile.
 * <p>
 * For value look up, this class implements a binary search algorithm over the
 * address space of all bytes of all strings stored in the file. The byte
 * string items are encoded in the DataFile such that each string item is
 * followed by an 0x0FFF8 sequence. 0x0FFF8 in the data item is encoded as
 * a double 0x0FFF8 sequence.
 * <p>
 * Note that every array read and written must go through an encoding/decoding
 * process that ensures the data is escaped correctly. The worst case
 * scenario (where the string contains all 0x0FFF8 sequences) the encoded size
 * will be double the input size. The '0x0FFF8' sequence was chosen because
 * it will never appear in valid utf-8 encoded form.
 * <p>
 * While this structure is able to store data strings of any length, it should
 * be noted that the search algorithm will read and store an entire string in
 * memory for each item it visits during a query. It is therefore recommended
 * that excessively large data strings should not be stored in this structure
 * if good search performance and low memory usage is desired.
 * <p>
 * OrderedSetData stores 64 bits of meta information (a static magic value)
 * at the start of the DataFile on any set that contains a none zero quantity
 * of strings. This meta information is intended to help identify
 * DataFile structures that are formatted by this object.
 * <p>
 * This provides an iterator implementation for traversing the set, however it
 * should be noted that when an OrderedSetData object is mutated (items
 * added/removed/updated) any existing iterators created by the object are
 * invalidated.
 * <p>
 * This object implements java.lang.SortedSet&lt;ByteArray&gt;.
 * <p>
 * <b>PERFORMANCE</b>: While the data string search and iteration functions
 * are efficient, the size() query requires a full scan of all the data in
 * file to compute.
 *
 * @author Tobias Downer
 */

public class OrderedSetData extends AbstractSet<ByteArray>
                                              implements SortedSet<ByteArray> {

  /**
   * The magic value for this file format.
   */
  private static final long OSS_MAGIC = 0x0BE0220F;

  /**
   * A static Comparator object that compares the data string values using the
   * default Java string compare call.
   */
  private static final Comparator<ByteArray> LEXI_COLLATOR;

  static {
    LEXI_COLLATOR = new Comparator<ByteArray>() {
      @Override
      public int compare(ByteArray ob1, ByteArray ob2) {
        return ob1.compareTo(ob2);
      }
    };
  }


  // ----- Members -----

  /**
   * The DataFile object that maps to the set.
   */
  private final DataFile data;

  /**
   * The Comparator under which the strings in the set are sorted.
   */
  private final Comparator<ByteArray> string_collator;

  /**
   * The position of the first element in the list.
   */
  private long start_pos = -1;

  /**
   * The position after the end element in the list.
   */
  private long end_pos = -1;

  /**
   * The end position of the first element in the list if known, or -1 if not
   * known.
   */
  private long start_element_endpos = -1;

  /**
   * The start position of the last element in the list if known, or -1 if not
   * known.
   */
  private long end_element_startpos = -1;

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
  private OrderedSetData root_set;

  /**
   * The lower bound of the subset, or null if there is no lower bound.
   */
  private final ByteArray lower_bound;

  /**
   * The upper bound of the subset, or null if there is no upper bound.
   */
  private final ByteArray upper_bound;


  // ------ Temporary members set in the search method -----

//  private long found_item_start = 0, found_item_end = 0;
  private DataSectionByteArray found_item;



  /**
   * General private constructor.
   */
  private OrderedSetData(DataFile data,
                         Comparator<ByteArray> collator,
                         ByteArray l_bound, ByteArray u_bound) {

    // data and handler may not be null
    if (data == null) {
      throw new NullPointerException("data is null");
    }

    this.data = data;
    this.string_collator = (collator != null) ? collator : LEXI_COLLATOR;
    // This constructor has unlimited bounds.
    this.upper_bound = u_bound;
    this.lower_bound = l_bound;

  }

  /**
   * Subset of a set constructor.
   */
  private OrderedSetData(OrderedSetData root_set,
                         ByteArray l_bound, ByteArray u_bound) {

    this(root_set.data, root_set.string_collator,
         l_bound, u_bound);

    // Set the root set
    this.root_set = root_set;
    // Set the version to -1 (will auto update internal state when the list is
    // accessed).
    this.version = -1;

  }

  /**
   * Creates this structure mapped over the given DataFile object. 'collator'
   * describes the collation of strings in the set, or null if the order of
   * strings should be lexicographical.
   * <p>
   * Note that the collator object behavior must be consistent over all
   * use of instances of this object on a DataFile object.  An
   * OrderedSetData that has managed a backed DataFile under one collation
   * will not work correctly if the collation is changed.  If such a situation
   * happens, the class function behavior is undefined.
   *
   * @param data the DataFile object that backs the list.
   * @param collator how strings in the set are ordered or null for
   *    lexicographical ordering.
   */
  public OrderedSetData(DataFile data, Comparator<ByteArray> collator) {
    this(data, collator, null, null);

    this.root_set = this;
    // The version of new list is 0
    this.version = 0;

    // This forces a state update
    this.root_state_dirty = true;
  }

  /**
   * Creates this structure mapped over the given DataFile object. The order of
   * strings in this string set is lexicographical.
   *
   * @param data the DataFile object that backs the list.
   */
  public OrderedSetData(DataFile data) {
    this(data, null);
  }

  /**
   * Updates the internal state of this object (the start_pos and end_pos
   * objects) if the subset is determined to be dirty (this version is less
   * than the version of the root). This is necessary for when the list
   * changes.
   */
  private void updateInternalState() {

//    System.out.println("this.version = " + this.version);
//    System.out.println("root_set.version = " + root_set.version);
//    System.out.println("this.root_state_dirty = " + this.root_state_dirty);

    if (this.version < root_set.version || this.root_state_dirty) {
      // Reset the root state dirty boolean
      this.root_state_dirty = false;

      // Read the size
      final long sz = data.size();

      // The empty states,
      if (sz < 8) {
        start_pos = 0;
        end_pos = 0;
        start_element_endpos = -1;
        end_element_startpos = -1;
      }
      else if (sz == 8) {
        start_pos = 8;
        end_pos = 8;
        start_element_endpos = -1;
        end_element_startpos = -1;
      }
      // The none empty state
      else {

        // If there is no lower bound we use start of the list
        if (lower_bound == null) {
          start_pos = 8;
          start_element_endpos = -1;
        }
        // If there is a lower bound we search for the string and use it
        else {
          boolean found = searchFor(lower_bound, 8, sz);
          start_pos = data.position();
          if (found) {
            start_element_endpos = found_item.end_pos;
          }
        }

        // If there is no upper bound we use end of the list
        if (upper_bound == null) {
          end_pos = sz;
          end_element_startpos = -1;
        }
        // Otherwise there is an upper bound so search for the string and use it
        else {
          boolean found = searchFor(upper_bound, 8, sz);
          end_pos = data.position();
          if (found) {
            end_element_startpos = found_item.start_pos;
          }
        }
      }

      // Update the version of this to the parent.
      this.version = root_set.version;
    }
  }

//  /**
//   * Returns the byte array at the given position.
//   *
//   * @param s the start of the string in the file.
//   * @param e the end of the string (including the 0x0FFF8 deliminator).
//   * @return the String at the given position.
//   * @throws java.io.IOException
//   */
//  private ByteArray byteArrayAtPosition(final long s, final long e) {
//
//    final long to_read = (e - s) - 4;
//    // If it's too large
//    if (to_read > Integer.MAX_VALUE) {
//      throw new RuntimeException("String too large to read.");
//    }
//    data.position(s + 2);
//
//    // Decode to a buffer,
//    int left_to_read = (int) to_read;
//    byte[] buf = new byte[left_to_read];
//    int pos = 0;
//    boolean last_was_escape = false;
//
//    while (left_to_read > 0) {
//      int read = Math.min(128, left_to_read);
//      data.get(buf, pos, read);
//
//      // Decode,
//      int read_to = pos + read;
//      for (; pos < read_to; pos += 2) {
//        if (buf[pos] == (byte) 0x0FF &&
//            buf[pos + 1] == (byte) 0x0F8) {
//          // This is the escape sequence,
//          if (last_was_escape) {
//            // Ok, we convert this to a single,
////            System.out.println("pos = " + pos);
////            System.out.println("read_to - pos = " + (read_to - pos));
//            System.arraycopy(buf, pos + 2, buf, pos, read_to - pos - 2);
//            pos -= 2;
//            read_to -= 2;
//            last_was_escape = false;
//          }
//          else {
//            last_was_escape = true;
//            continue;
//          }
//        }
//        // This is illegal. The string contained 0x0FFF8 that was not followed
//        // by 0x0FFF8.
//        if (last_was_escape) {
//          throw new RuntimeException("Encoding error");
//        }
//      }
//
//      left_to_read -= read;
//    }
//
//    // Illegal, must end with 'last_was_escape' at false.
//    if (last_was_escape) {
//      throw new RuntimeException("Encoding error");
//    }
//
//    // 'buf' now contains the decoded sequence. Turn it into a ByteArray
//    // object.
//
//    byte last_byte = buf[pos - 1];
//    if (last_byte == 0x00) {
//      // Last byte is 0 indicate we lose 1 byte from the end,
//      pos -= 1;
//    }
//    else if (last_byte == 0x01) {
//      // Last byte of 1 indicates we lose 2 bytes from the end.
//      pos -= 2;
//    }
//    else {
//      throw new RuntimeException("Encoding error");
//    }
//
//    return new JavaByteArray(buf, 0, pos);
//  }

  /**
   * Removes the byte array at the position of the element set in 'found_item'.
   */
  private void removeByteArrayAtPosition(DataSectionByteArray item) {
    // Tell the root set that any child subsets may be dirty
    if (root_set != this) {
      root_set.version += 1;
      root_set.root_state_dirty = true;
    }
    version += 1;

    long ba_start_pos = item.getStartPosition();
    long ba_end_pos = item.getEndPosition();

    // The number of byte entries to remove
    final long str_remove_size = ba_start_pos - ba_end_pos;
    data.position(ba_end_pos);
    data.shift(str_remove_size);
    this.end_pos = this.end_pos + str_remove_size;
    this.start_element_endpos = -1;
    this.end_element_startpos = -1;

    // If this removal leaves the set empty, we delete the file and update the
    // internal state as necessary.
    if (this.start_pos == 8 && this.end_pos == 8) {
      data.delete();
      this.start_pos = 0;
      this.end_pos = 0;
    }

  }

  /**
   * Inserts a string into the data file at the current position the DataFile
   * object is at.
   *
   * @param value the string to insert into the set.
   */
  private void insertValue(ByteArray value) {
    // This encodes the value and stores it at the position. The encoding
    // process looks for 0x0FFF8 sequences and encodes it as a pair. This
    // allows to distinguish between a 0x0FFF8 seqence in the binary data and
    // a record deliminator.

    // Tell the root set that any child subsets may be dirty
    if (root_set != this) {
      root_set.version += 1;
      root_set.root_state_dirty = true;
    }
    version += 1;

    this.start_element_endpos = -1;
    this.end_element_startpos = -1;

    // If the set is empty, we insert the magic value to the start of the
    // data file and update the internal vars as appropriate
    if (data.size() < 8) {
      data.setSize(8);
      data.position(0);
      data.putLong(OSS_MAGIC);
      this.start_pos = 8;
      this.end_pos = 8;
    }

    // Encode the value,
    int len = value.length();
    // Make enough room to store the value and round up to the nearest word
    int act_len;
    if (len % 2 == 0) act_len = len + 2;
    else act_len = len + 1;

    // Note that this is an estimate. Any 'FFF8' sequences found will expand
    // the file when found.
    data.shift(act_len + 4);
    data.putShort((short) 0);
    int i = 0;
    int read_len = (len / 2) * 2;
    for (; i < read_len; i += 2) {
      byte b1 = value.getByteAt(i);
      byte b2 = value.getByteAt(i + 1);
      data.put(b1);
      data.put(b2);
      // 'FFF8' sequence will write itself again.
      if (b1 == (byte) 0x0FF && b2 == (byte) 0x0F8) {
        data.shift(2);
        data.put((byte) 0x0FF);
        data.put((byte) 0x0F8);
        act_len += 2;
      }
    }

    // Put tail characters,
    if (i == len) {
      data.put((byte) 0x00);
      data.put((byte) 0x01);
    }
    else {
      data.put(value.getByteAt(i));
      data.put((byte) 0x00);
    }

    // Write the deliminator
    data.putShort((short) 0x0FFF8);

    // Adjust end_pos
    end_pos = end_pos + (act_len + 4);

//    System.out.println("---");
//    data.position(0);
//    long sz = data.size();
//    int n = 0;
//    for (; n < sz; ++n) {
//      int b = ((int) data.get()) & 0x0FF;
//      System.out.print(Integer.toString(b, 16));
//      System.out.print(" ");
//      if ((n % 2) == 1) {
//        System.out.println();
//      }
//    }
//    System.out.println("---");

  }


  /**
   * Given a position aligned to the nearest 16-bit and start and end boundary
   * positions, discovers the end of the byte array at the given position.
   */
  private long scanForEndPosition(long pos,
                                  final long start, final long end) {
    data.position(pos);

    long init_pos = pos;

    // Did we land on a deliminator sequence?
    if (pos < end && data.getShort() == (short) 0x0FFF8) {
      // If there's not an escape char after or before then we did,
      if (pos + 2 >= end || data.getShort() != (short) 0x0FFF8) {
        data.position(pos - 2);
        if (pos - 2 < start || data.getShort() != (short) 0x0FFF8) {
          // Ok, this is deliminator. The char before and after are not 0x0FFF8
          return pos + 2;
        }
      }
      // If we are here we landed on a deliminator that is paired, so now
      // scan forward until we reach the last,
      pos = pos + 2;
      data.position(pos);
      while (pos < end) {
        pos = pos + 2;
        if (data.getShort() != (short) 0x0FFF8) {
          break;
        }
      }
    }

    data.position(pos);

    while (pos < end) {
      short c = data.getShort();
      pos = pos + 2;
      if (c == (short) 0x0FFF8) {
        // This is the end of the string if the 0x0FFF8 sequence is on its own.
        if (pos >= end || data.getShort() != (short) 0x0FFF8) {
          // This is the end of the string, break the while loop
          return pos;
        }
        else {
          // Not end because 0x0FFF8 is repeated,
          pos = pos + 2;
        }
      }
    }

    // All strings must end with 0x0FFF8.  If this character isn't found before
    // the end is reached then the format of the data is in error.

    // Output debugging information to System.err
    System.err.println("start = " + start);
    System.err.println("end = " + end);
    System.err.println("start_pos = " + start_pos);
    System.err.println("end_pos = " + end_pos);
    System.err.println("data.size() = " + data.size());
    System.err.println("init_pos = " + init_pos);

    data.position(start);
    for (long i = start; i < end; ++i) {
      byte b = data.get();
      String hex_str = Integer.toHexString(((int) b) & 0x0FF);
      System.err.print(hex_str + " ");
    }
    System.err.println();

    throw new RuntimeException("Set data error.");
  }

  /**
   * Given a position aligned to a 16-bit word between the given bounds, scans
   * for the start position of the byte array at the given position.
   */
  private long scanForStartPosition(long pos,
                                    final long start, final long end) {

    data.position(pos);

    // Did we land on a deliminator sequence?
    if (pos < end && data.getShort() == (short) 0x0FFF8) {
      // If there's not an escape char after or before then we did,
      if (pos + 2 >= end || data.getShort() != (short) 0x0FFF8) {
        data.position(pos - 2);
        if (pos - 2 < start || data.getShort() != (short) 0x0FFF8) {
          // Ok, this is deliminator. The char before and after are not 0x0FFF8
          return pos + 2;
        }
      }
      // If we are here we landed on a deliminator that is paired, so now
      // scan backward until we reach the first none deliminated entry,
      pos = pos - 2;
      while (pos >= start) {
        data.position(pos);
        if (data.getShort() != (short) 0x0FFF8) {
          break;
        }
        pos = pos - 2;
      }
    }

    while (pos >= start) {
      data.position(pos);
      short c = data.getShort();
      pos = pos - 2;
      if (c == (short) 0x0FFF8) {
        // This is the end of the string if the 0x0FFF8 sequence is on its own.
        data.position(pos);
        if (pos < start || data.getShort() != (short) 0x0FFF8) {
          // This is the end of the string, break the while loop
          return pos + 4;
        }
        else {
          // Not end because 0x0FFF8 is repeated,
          pos = pos - 2;
        }
      }
    }

    // We hit the start of the bounded area,
    return pos + 2;

  }

//  /**
//   * Search for the string value in the DataFile and return true if found.
//   * When this method returns, the DataFile (data) object will be positioned at
//   * either the location to insert the string into the correct
//   * order or at the location of the value in the set.
//   * <p>
//   * We recursively divide up the ordered list to search for the value.
//   *
//   * @param value the value to search for.
//   * @param start the start of the file to search for the string.
//   * @param end the end of the file to search for the string.
//   * @return true if the string was found, false otherwise.
//   */
//  private boolean searchFor(final ByteArray value,
//                            final long start, final long end) {
//
//    // If start is end, the list is empty,
//    if (start == end) {
//      data.position(start);
//      return false;
//    }
//
//    // How large is the area we are searching in characters?
//    long search_len = (end - start) / 2;
//    // Read the string from the middle of the area
//    final long mid_pos = start + ((search_len / 2) * 2);
//
//    // Search to the end of the string
//    long str_end = scanForEndPosition(mid_pos, start, end);
//    long str_start = scanForStartPosition(mid_pos - 2, start, end);
//
////    System.out.println("mid_pos = " + mid_pos);
////    System.out.println("  start = " + start + ", end = " + end);
////    System.out.println("  s = " + str_start + ", e = " + str_end);
//
//    // Now str_start will point to the start of the string and str_end to the
//    // end (the char immediately after 0x0FFFF).
//    // Read the midpoint string,
//    ByteArray mid_value = byteArrayAtPosition(str_start, str_end);
//
//    // Compare the values
//    int v = string_collator.compare(value, mid_value);
//    // If str_start and str_end are the same as start and end, then the area
//    // we are searching represents only 1 string, which is a return state
//    final boolean last_str = (str_start == start && str_end == end);
//
//    if (v < 0) {  // if value < mid_value
//      if (last_str) {
//        // Position at the start if last str and value < this value
//        data.position(str_start);
//        return false;
//      }
//      // We search the head
//      return searchFor(value, start, str_start);
//    }
//    else if (v > 0) {  // if value > mid_value
//      if (last_str) {
//        // Position at the end if last str and value > this value
//        data.position(str_end);
//        return false;
//      }
//      // We search the tail
//      return searchFor(value, str_end, end);
//    }
//    else {  // if value == mid_value
//      data.position(str_start);
//      // Update internal state variables
//      found_item_start = str_start;
//      found_item_end = str_end;
//      return true;
//    }
//  }

  /**
   * Search for the string value in the DataFile and return true if found.
   * When this method returns, the DataFile (data) object will be positioned at
   * either the location to insert the string into the correct
   * order or at the location of the value in the set.
   * <p>
   * We recursively divide up the ordered list to search for the value.
   *
   * @param value the value to search for.
   * @param start the start of the file to search for the string.
   * @param end the end of the file to search for the string.
   * @return true if the string was found, false otherwise.
   */
  private boolean searchFor(final ByteArray value,
                            final long start, final long end) {

    // If start is end, the list is empty,
    if (start == end) {
      data.position(start);
      found_item = null;
      return false;
    }

    // How large is the area we are searching in characters?
    long search_len = (end - start) / 2;
    // Read the string from the middle of the area
    final long mid_pos = start + ((search_len / 2) * 2);

    // Search to the start of the string we landed on,
//    long str_end_TO = scanForEndPosition(mid_pos, start, end);
    long str_start = scanForStartPosition(mid_pos - 2, start, end);

    // Get the byte array at the given position where the start of the array
    // is at 'str_start'. This byte array does not immediately know where the
    // end of the string is.
//    ByteArray mid_value = byteArrayAtPosition(str_start, str_end_TO);
//    DataSectionByteArray mid_value_test =
//                             new DataSectionByteArray(str_start, end, mid_pos);
    DataSectionByteArray mid_value =
                             new DataSectionByteArray(str_start, end, mid_pos);

//    // -- TESTING : compare the two objects --
//
//    if (mid_value.length() != mid_value_test.length()) {
//      System.out.println("LENGTH MISMATCH");
//      System.out.println("correct: " + mid_value.length());
//      System.out.println("actual: " + mid_value_test.length());
//      throw new RuntimeException("Assert failed.");
//    }
//
//    // -- TESTING : END --

    // Compare the values
    int v = string_collator.compare(value, mid_value);

    if (v < 0) {  // if value < mid_value
      // If we are at the final string then we know we won't find the
      // value.
//      if (str_start == start && str_end_TO == end) {
      if (str_start == start && mid_value.getEndPosition() == end) {
        // Position at the start if last str and value < this value
        data.position(str_start);
        found_item = null;
        return false;
      }
      // We search the head
      return searchFor(value, start, str_start);
    }
    else if (v > 0) {  // if value > mid_value
      // If str_start and str_end are the same as start and end, then the area
      // we are searching represents only 1 string, which is a return state
//      final long str_end = str_end_TO;
      final long str_end = mid_value.getEndPosition();
      if (str_start == start && str_end == end) {
        // Position at the end if last str and value > this value
        data.position(str_end);
        found_item = null;
        return false;
      }
      // We search the tail
      return searchFor(value, str_end, end);
    }
    else {  // if value == mid_value
//      final long str_end = str_end_TO;
//      final long str_end = mid_value.getEndPosition();
      data.position(str_start);
      // Update internal state variables
      found_item = mid_value;
      return true;
    }
  }

  // ----------- Implemented from AbstractSet<String> ------------

  /**
   * Returns the total number of elements in the set or Integer.MAX_VALUE if
   * the set contains Integer.MAX_VALUE or more values.
   * <p>
   * <b>PERFORMANCE</b>: This operation will scan the entire set to
   * determine the number of elements. Avoid using this operation to scale
   * for large sets.
   * <p>
   * Performance is O(n)
   */
  @Override
  public int size() {
    updateInternalState();
    long p = this.start_pos;
    long end = this.end_pos;
    int count = 0;
    while (p < end && count < Integer.MAX_VALUE) {
      ++count;
      p = scanForEndPosition(p, p, end);
    }

    return count;
  }

  /**
   * Returns true if the set is empty. This is a low complexity query.
   */
  @Override
  public boolean isEmpty() {
    updateInternalState();
    // If start_pos == end_pos then the list is empty
    if (this.start_pos == this.end_pos) {
      return true;
    }
    return false;
  }

  /**
   * Returns an Iterator over all the strings stored in this set in collation
   * order.
   */
  @Override
  public Iterator<ByteArray> iterator() {
    // Note - important we update internal state here because start_pos and
    //   end_pos used by the inner class.
    updateInternalState();
    return new ByteArraySetIterator();
  }

  /**
   * Returns an iterator over all the strings stored in this set in reverse
   * collation order.
   */
  public Iterator<ByteArray> reverseIterator() {
    // Note - important we update internal state here because start_pos and
    //   end_pos used by the inner class.
    updateInternalState();
    return new ReverseByteArraySetIterator();
  }

  /**
   * Returns true if the set contains the given string.  Assumes the set is
   * ordered by the collator.
   *
   * @param str the value to search for.
   * @return true if the set contains the string.
   */
  @Override
  public boolean contains(Object str) {
    if (str == null) throw new NullPointerException();
    updateInternalState();
    // Look for the string in the file.
    return searchFor((ByteArray) str, this.start_pos, this.end_pos);
  }

  /**
   * Adds a string to the set in sorted order as defined by the collator
   * defined when the object is created.  Returns true if the set does not
   * contain the string and the string was added, false if the set already
   * contains the value.
   */
  @Override
  public boolean add(ByteArray value) {

    if (value == null) throw new NullPointerException();
    // As per the contract, this method can not add values that compare below
    // the lower bound or compare equal or greater to the upper bound.
    if (lower_bound != null &&
        string_collator.compare(value, lower_bound) < 0) {
      throw new IllegalArgumentException("value < lower_bound");
    }
    if (upper_bound != null &&
        string_collator.compare(value, upper_bound) >= 0) {
      throw new IllegalArgumentException("value >= upper_bound");
    }

    updateInternalState();

    // Find the index in the list of the value either equal to the given value
    // or the first value in the set comparatively more than the given value.
//    System.out.println("sf: " + this.start_pos + " - " + this.end_pos);
    boolean found = searchFor(value, this.start_pos, this.end_pos);
    // If the value was found,
    if (found) {
      // Return false
      return false;
    }
    else {
      // Not found, so insert into the set at the position we previously
      // discovered.
      insertValue(value);
      // And return true
      return true;
    }

  }

  /**
   * Finds a data string in the set that the comparator compares as equal
   * with the given value and replaces the content with the given value. This
   * is used with custom comparators that only consider a small part of the
   * data element when determining order. For example, a record may contain
   * a key and a variable value - the key is used for collation ordering and
   * the variable value may be changed using this method.
   * <p>
   * Returns true if a value was found and replaced. Returns false if the
   * value was not found and nothing was replaced.
   */
  public boolean replace(ByteArray value) {

    if (value == null) throw new NullPointerException();
    // As per the contract, this method can not replace values that compare
    // below the lower bound or compare equal or greater to the upper bound.
    if (lower_bound != null &&
        string_collator.compare(value, lower_bound) < 0) {
      throw new IllegalArgumentException("value < lower_bound");
    }
    if (upper_bound != null &&
        string_collator.compare(value, upper_bound) >= 0) {
      throw new IllegalArgumentException("value >= upper_bound");
    }

    updateInternalState();

    // Find the index in the list of the value either equal to the given value
    // or the first value in the set comparatively more than the given value.
    boolean found = searchFor(value, this.start_pos, this.end_pos);
    // If the value was not found,
    if (!found) {
      // Return false
      return false;
    }
    else {
      // The position in the data file after this operation,
      long final_pos = found_item.getStartPosition();
      // Found, so remove and then insert a new value,
      removeByteArrayAtPosition(found_item);
      // Reposition to the start of the delete area
      if (data.size() == 0) {
        // Removed last entry so set the position at the start,
        data.position(0);
      }
      else {
        // Otherwise set position to the start of the item,
        data.position(final_pos);
      }
      insertValue(value);

      // And return true
      return true;
    }

  }

  /**
   * Replaces or adds a data string to the set depending on whether an entry
   * is found in the set. If an entry that compares equally (as determined by
   * the comparator) is found in the set then it is replaced. If no entry is
   * found that compares equally then the entry is added in the correct
   * sorted location in the set.
   */
  public void replaceOrAdd(ByteArray value) {

    if (value == null) throw new NullPointerException();
    // As per the contract, this method can not replace values that compare
    // below the lower bound or compare equal or greater to the upper bound.
    if (lower_bound != null &&
        string_collator.compare(value, lower_bound) < 0) {
      throw new IllegalArgumentException("value < lower_bound");
    }
    if (upper_bound != null &&
        string_collator.compare(value, upper_bound) >= 0) {
      throw new IllegalArgumentException("value >= upper_bound");
    }

    updateInternalState();

    // Find the index in the list of the value either equal to the given value
    // or the first value in the set comparatively more than the given value.
    boolean found = searchFor(value, this.start_pos, this.end_pos);
    // If the value was not found,
    if (!found) {
      // Add to the list,
      insertValue(value);
    }
    else {
      // The position in the data file after this operation,
      long final_pos = found_item.getStartPosition();
      // Found, so remove and then insert a new value,
      removeByteArrayAtPosition(found_item);
      // Reposition to the start of the delete area
      if (data.size() == 0) {
        // Removed last entry so set the position at the start,
        data.position(0);
      }
      else {
        // Otherwise set position to the start of the item,
        data.position(final_pos);
      }
      insertValue(value);
    }

  }


  /**
   * Removes the value from the set if it is present.  Assumes the set is
   * ordered by the collator.
   *
   * @param value the String to remove.
   * @return true if the value was removed.
   */
  @Override
  public boolean remove(Object value) {

//    System.out.println("remove: " + value);

    if (value == null) throw new NullPointerException();
    updateInternalState();
    // Find the index in the list of the value either equal to the given value
    // or the first value in the set comparatively more than the given value.
    boolean found = searchFor((ByteArray) value, this.start_pos, this.end_pos);
    // If the value was found,
    if (found) {
      // Remove it
      removeByteArrayAtPosition(found_item);
    }
    return found;
  }

  /**
   * Clears the set of all string items.
   */
  @Override
  public void clear() {

//    System.out.println("clear");

    updateInternalState();

    // Tell the root set that any child subsets may be dirty
    if (root_set != this) {
      root_set.version += 1;
      root_set.root_state_dirty = true;
    }
    version += 1;

    this.start_element_endpos = -1;
    this.end_element_startpos = -1;

    // Clear the list between the start and end,
    long to_clear = this.start_pos - this.end_pos;
    data.position(this.end_pos);
    data.shift(to_clear);
    this.end_pos = this.start_pos;

    // If it's completely empty, we delete the file,
    if (this.start_pos == 8 && this.end_pos == 8) {
      data.delete();
      this.start_pos = 0;
      this.end_pos = 0;
    }

  }

  // ---------- Implemented from SortedSet -----------

  /**
   * Bounds the given string within the upper and lower boundary defined by
   * this set.
   */
  private ByteArray bounded(ByteArray str) {
    if (str == null) {
      throw new NullPointerException();
    }
    // If str is less than lower bound then return lower bound
    if (lower_bound != null &&
        comparator().compare(str, lower_bound) < 0) {
      return lower_bound;
    }
    // If str is greater than upper bound then return upper bound
    if (upper_bound != null &&
        comparator().compare(str, upper_bound) >= 0) {
      return upper_bound;
    }
    return str;
  }

  /**
   * The comparator for this set.
   *
   * @return the comparator for this set.
   */
  @Override
  public Comparator<ByteArray> comparator() {
    return string_collator;
  }

  /**
   * Returns the sorted subset of string items from this set between the string
   * 'from_element' (inclusive) and 'to_element' (exclusive), as ordered by the
   * collation definition.  The behavior of this method follows the contract
   * as defined by java.util.AbstractSet.
   *
   * @param from_element the lowest string in the subset.
   * @param to_element the highest string in the subset
   * @return the sorted subset of string items.
   */
  @Override
  public OrderedSetData subSet(ByteArray from_element,
                                     ByteArray to_element) {
    // check the bounds not out of range of the parent bounds
    from_element = bounded(from_element);
    to_element = bounded(to_element);
    return new OrderedSetData(root_set, from_element, to_element);
  }

  /**
   * Returns the sorted subset of string items from this set between the start
   * and 'to_element' (exclusive) from this set, as ordered by the collation
   * definition.  The behavior of this method follows the contract
   * as defined by java.util.AbstractSet.
   *
   * @param to_element the highest string in the subset
   * @return the sorted subset of string items.
   */
  @Override
  public OrderedSetData headSet(ByteArray to_element) {
    to_element = bounded(to_element);
    return new OrderedSetData(root_set, lower_bound, to_element);
  }

  /**
   * Returns the sorted subset of string items from this set between the string
   * 'from_element' (inclusive) and the end of the set, as ordered by the
   * collation definition.  The behavior of this method follows the contract
   * as defined by java.util.AbstractSet.
   *
   * @param to_element the highest string in the subset
   * @return the sorted subset of string items.
   */
  @Override
  public OrderedSetData tailSet(ByteArray from_element) {
    from_element = bounded(from_element);
    return new OrderedSetData(root_set, from_element, upper_bound);
  }

  /**
   * Returns the first (lowest) string item currently in this set.
   *
   * @return the first (lowest) element currently in this set
   */
  @Override
  public ByteArray first() {
    updateInternalState();

    if (start_pos >= end_pos) {
      throw new NoSuchElementException();
    }

    // If we know the end position of the first element then given a better
    // hieristic to the data section object,
    long mid_pos = start_pos;
    DataSectionByteArray byte_array;
    byte_array = new DataSectionByteArray(start_pos, end_pos, start_pos);
    if (start_element_endpos >= 0) {
      byte_array.setEndPosition(start_element_endpos);
    }

    // Return the array,
    return byte_array;

//    long found_end = scanForEndPosition(start_pos, start_pos, end_pos);
//    return byteArrayAtPosition(start_pos, found_end);

  }

  /**
   * Returns the last (highest) string item currently in this set.
   *
   * @return the last (highest) element currently in this set
   */
  @Override
  public ByteArray last() {
    updateInternalState();

    if (start_pos >= end_pos) {
      throw new NoSuchElementException();
    }

    // If we know the start position of the last element,
    long found_start;
    if (end_element_startpos < 0) {
      end_element_startpos =
                         scanForStartPosition(end_pos - 2, start_pos, end_pos);
    }
    found_start = end_element_startpos;

    // Return the array,
    DataSectionByteArray byte_array =
                   new DataSectionByteArray(found_start, end_pos, end_pos - 2);
    return byte_array;

//    long found_start = scanForStartPosition(end_pos - 2, start_pos, end_pos);
//    return byteArrayAtPosition(found_start, end_pos);

  }

  // ---------- Inner classes ----------

  /**
   * An implementation of ByteArray that is lazily materialized either as the
   * returned stream is read, or when it is required that the start or end
   * position are required to be known.
   */
  private class DataSectionByteArray implements ByteArray {

    /**
     * An arbitrary mid point in the section.
     */
    private final long mid_pos;

    /**
     * The end of the search section.
     */
    private final long bounds_end;

    /**
     * The start position of the data section.
     */
    private final long start_pos;

    /**
     * The end position of the data section if known, or -1 if not known.
     */
    private long end_pos;

    /**
     * The materialized version of this byte array.
     */
    private byte[] materialized_version;
    private int materialized_length;

    /**
     * Constructor.
     */
    DataSectionByteArray(long start_pos, long bounds_end, long mid_pos) {
      this.mid_pos = mid_pos;
      this.bounds_end = bounds_end;
      this.start_pos = start_pos;
      this.end_pos = -1;
      this.materialized_version = null;
    }

    /**
     * Returns a string representation of this byte array as a list of byte
     * values.
     */
    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("[");
      int len = length();
      for (int i = 0; i < len; ++i) {
        b.append(Byte.toString(getByteAt(i)));
        b.append(" ");
      }
      b.append("]");
      return b.toString();
    }

    private byte[] materializedArray() {
      if (materialized_version == null) {

        // Fully materialize it,

        try {
          ByteArrayBuilder bout = new ByteArrayBuilder(256);
          DataSectionInputStream din =
                                  new DataSectionInputStream(start_pos + 2);
          byte[] buf = new byte[32];
          while (true) {
            int read = din.read(buf, 0, 32);
            if (read == -1) {
              break;
            }
            bout.write(buf, 0, read);
          }

          materialized_version = bout.getBuffer();
          materialized_length = bout.length();
          end_pos = din.getEndPosition();

        }
        catch (IOException e) {
          // Can't happen,
          throw new RuntimeException(e);
        }

      }
      return materialized_version;
    }

    /**
     * Make sure the object is materialized.
     */
    void forceMaterialize() {
      materializedArray();
    }



    @Override
    public int length() {
      materializedArray();
      return materialized_length;
    }

    @Override
    public byte getByteAt(int p) {
      materializedArray();
      if (p < 0 || p >= materialized_length) {
        throw new IndexOutOfBoundsException();
      }
      return materialized_version[p];
    }

    @Override
    public DataInputStream getDataInputStream() {
      InputStream ins;
      // If it's already materialized in memory,
      if (materialized_version != null) {
        ins = new ByteArrayInputStream(
                                materialized_version, 0, materialized_length);
      }
      else {
        ins = new DataSectionInputStream(start_pos + 2);
      }
      return new DataInputStream(ins);
    }

    @Override
    public int compareTo(ByteArray dest) {
      DataInputStream thisin = this.getDataInputStream();
      DataInputStream thatin = dest.getDataInputStream();
      try {
        while (true) {
          int thisv = thisin.read();
          int thatv = thatin.read();
          if (thisv == -1 || thatv == -1) {
            // They must be the same,
            if (thisv == -1 && thatv == -1) {
              return 0;
            }
            // 'dest' is largest
            else if (thisv == -1) {
              return -1;
            }
            // 'this' is largest
            else {
              return 1;
            }
          }
          // Compare the values,
          if (thisv != thatv) {
            if ((byte) thisv > (byte) thatv) {
              return 1;
            }
            else {
              return -1;
            }
          }
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean equals(Object obj) {
      return compareTo((ByteArray) obj) == 0;
    }

    /**
     * Returns the start position of the data section.
     */
    long getStartPosition() {
      return start_pos;
    }

    /**
     * Returns the end position of the data section.
     */
    long getEndPosition() {
      if (end_pos == -1) {
        if (mid_pos >= start_pos + 12) {
          end_pos = scanForEndPosition(mid_pos, start_pos, bounds_end);
        }
        else {
          forceMaterialize();
        }
      }
      return end_pos;
    }

    /**
     * Set the end position of the object, if known.
     */
    void setEndPosition(long end_pos) {
      this.end_pos = end_pos;
    }

    /**
     * An InputStream implementation that reads sequentially over a data
     * section.
     */
    private class DataSectionInputStream extends InputStream {

      private long pos;
      private byte[] buf;
      private int bufi;
      private int size;
      private long stream_end_pos = -1;

      private final static int BUF_SIZE = 128;

      DataSectionInputStream(long pos) {
        this.pos = pos;
        this.buf = new byte[BUF_SIZE];
        this.bufi = 0;
        this.size = 0;
      }

      private void fillBuffer() {

        // Already filled,
        if (bufi < size) {
          return;
        }
        else if (stream_end_pos != -1) {
          return;
        }

        bufi = 0;
        size = 0;

        boolean last_was_escape = false;
        boolean end_found = false;

        data.position(pos);

        while (true) {

          // If the end of the buffer reached,
          if (size >= BUF_SIZE) {
            // Edge case; if the last short is '0x0FFF8' then we know it's
            //   ok to continue from here
            if (buf[size - 2] != (byte) 0x0FF ||
                buf[size - 1] != (byte) 0x0F8) {
              // We always reduce size by 2. This ensures that we don't
              // accidently read half of an array end sequence.
              size -= 2;
              pos -= 2;
            }
            break;
          }

          // If we reached the bounds end,
          if (pos >= bounds_end) {
            end_found = true;
            break;
          }
          // Read 2 bytes,
          data.get(buf, size, 2);
          pos += 2;
          // Are the 2 bytes 0x0FFF8 ?
          if (buf[size] == (byte) 0x0FF &&
              buf[size + 1] == (byte) 0x0F8) {
            // Was the last 2 bytes also 0x0FFF8 ?
            if (last_was_escape) {
              // Yes, so this is a '0x0FFF8' string,
              size += 2;
              last_was_escape = false;
            }
            else {
              // No, so we might have reached the end,
              last_was_escape = true;
            }
          }
          else {
            if (last_was_escape) {
              // Single '0x0FFF8' indicates terminator,
              end_found = true;
              pos -= 2;
              break;
            }
            size += 2;
            last_was_escape = false;
          }
        }

        if (end_found) {
          // The last byte read tells us to skip byte at the end of the array,
          byte last_byte = buf[size - 1];
          if (last_byte == 0x00) {
            // Last byte is 0 indicate we lose 1 byte from the end,
            size -= 1;
          }
          else if (last_byte == 0x01) {
            // Last byte of 1 indicates we lose 2 bytes from the end.
            size -= 2;
          }
          else {
            throw new RuntimeException("Encoding error");
          }
          stream_end_pos = pos;
        }

      }

      /**
       * Returns the stream end position assuming the stream has been read to
       * the end, otherwise returns -1.
       */
      long getEndPosition() {
        return stream_end_pos;
      }

      @Override
      public int available() throws IOException {
        return super.available();
      }

      @Override
      public int read() throws IOException {
        fillBuffer();
        // End reached,
        if (bufi >= size) {
          return -1;
        }
        int v = (((int) buf[bufi]) & 0x0FF);
        ++bufi;
        return v;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
          throw new NullPointerException();
        }
        else if (off < 0 || len < 0 || len > b.length - off) {
          throw new IndexOutOfBoundsException();
        }
        else if (len == 0) {
          return 0;
        }

        int read_count = 0;

        // The number of bytes to read,
        while (len > 0) {

          fillBuffer();
          // Maximum possible we can read,
          int max_read = size - bufi;
          // End reached,
          if (max_read <= 0) {
            break;
          }

          int to_read = Math.min(len, max_read);
          System.arraycopy(buf, bufi, b, off, to_read);

          bufi += to_read;
          off += to_read;
          len -= to_read;
          read_count += to_read;

        }

        if (read_count == 0) {
          return -1;
        }
        return read_count;
      }

    }

  }



  /**
   * An iterator for strings in this set.
   */
  private class ByteArraySetIterator implements Iterator<ByteArray> {

    // The version of which this is derived,
    private long ver;
    // Offset of the iterator
    private long offset;
    // Last string position
    private long last_str_start = -1;

    // The DataSectionByteArray last fetched with 'next'
    private DataSectionByteArray last_next = null;

    /**
     * Constructor.
     */
    ByteArraySetIterator() {
      this.ver = root_set.version;
      this.offset = 0;
    }

    /**
     * Generates an exception if this version doesn't match the root
     * (concurrent list modification occurred).
     */
    private void versionCheck() {
      if (ver < root_set.version) {
        throw new IllegalStateException("Concurrent set update");
      }
    }

    @Override
    public boolean hasNext() {
      versionCheck();
      return start_pos + offset < end_pos;
    }

    @Override
    public ByteArray next() {
      versionCheck();
      long p = start_pos + offset;
      last_str_start = p;

      DataSectionByteArray barr = new DataSectionByteArray(p, end_pos, p);
      // Get the end position,
      long found_end_pos = barr.getEndPosition();
      offset += found_end_pos - p;
      // Set 'last_next' then return,
      last_next = barr;
      return barr;
    }

    @Override
    public void remove() {
      versionCheck();
      if (last_str_start == -1) {
        throw new IllegalStateException();
      }
//      found_item_start = last_str_start;
//      found_item_end = start_pos + offset;
      // Remove the byte array of the element last accessed by 'next'
      removeByteArrayAtPosition(last_next);
      // Update internal state
      offset = last_str_start - start_pos;
      last_str_start = -1;
      last_next = null;
      // Update the version of this iterator
      ver = ver + 1;
    }

  }

  /**
   * A reverse iterator for strings in this set.
   */
  private class ReverseByteArraySetIterator implements Iterator<ByteArray> {

    // The version of which this is derived,
    private long ver;
    // Offset of the iterator
    private long offset;

    /**
     * Constructor.
     */
    ReverseByteArraySetIterator() {
      this.ver = root_set.version;
      this.offset = 0;
    }

    /**
     * Generates an exception if this version doesn't match the root
     * (concurrent list modification occurred).
     */
    private void versionCheck() {
      if (ver < root_set.version) {
        throw new IllegalStateException("Concurrent set update");
      }
    }

    @Override
    public boolean hasNext() {
      versionCheck();
      return end_pos - offset > start_pos;
    }

    @Override
    public ByteArray next() {
      versionCheck();
      long p = end_pos - offset;

      long item_start_pos = scanForStartPosition(p - 4, start_pos, end_pos);
//      System.out.println("p - 4 = " + (p - 4));
//      System.out.println("start_pos = " + start_pos);
//      System.out.println("end_pos = " + end_pos);
//      System.out.println("item_start_pos = " + item_start_pos);

      DataSectionByteArray barr =
                      new DataSectionByteArray(item_start_pos, end_pos, p);
      offset = (end_pos - item_start_pos);
      return barr;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

}
