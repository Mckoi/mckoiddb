/**
 * com.mckoi.treestore.OrderedSetString  Dec 9, 2007
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

import java.util.Comparator;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

/**
 * An ordered set of variable length string values mapped over a DataFile
 * object.  Set modifications are immediately reflected in the underlying data
 * file.  This object grows and shrinks the size of the underlying data file as
 * values are inserted and removed from the set.
 * <p>
 * For value look up, this class implements a binary search algorithm over the
 * address space of all characters of all strings stored in the file.  The
 * string items are encoded in the DataFile such that each string item is
 * followed by an 0x0FFFF character.  While this structure is able to store
 * strings of any length, it should be noted that the search algorithm will
 * read and store an entire string in memory for each item it visits during the
 * search.  It is therefore recommended that excessively large strings should
 * not be stored in this structure if good search performance is wanted.
 * <p>
 * OrderedSetString stores 64 bits of meta information (a static magic value)
 * at the start of the DataFile on any set that contains a none zero quantity
 * of strings.  This meta information is intended to help identify
 * DataFile structures that are formatted by this object.
 * <p>
 * This object implements java.lang.SortedSet&lt;String&gt;.
 * <p>
 * <b>PERFORMANCE</b>: While the string search and iteration functions are
 * efficient, the size() query requires a full scan of all the strings in
 * the set to compute.
 * 
 * @author Tobias Downer
 */

public class OrderedSetString extends AbstractSet<String>
                                                implements SortedSet<String> {

  /**
   * The magic value for ordered set strings.
   */
  private static final long OSS_MAGIC = 0x0BE0110F;

//  private final Object track_ob = new Object();

  /**
   * A static Comparator object that compares the string values using the
   * default Java string compare call.
   */
  private static final Comparator<String> LEXI_COLLATOR;

  static {
    LEXI_COLLATOR = new Comparator<String>() {
      public int compare(String ob1, String ob2) {
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
  private final Comparator<String> string_collator;
  
  /**
   * The position of the first element in the list.
   */
  private long start_pos = -1;
  
  /**
   * The position after the end element in the list.
   */
  private long end_pos = -1;

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
  private OrderedSetString root_set;

  /**
   * The lower bound of the subset, or null if there is no lower bound.
   */
  private final String lower_bound;

  /**
   * The upper bound of the subset, or null if there is no upper bound.
   */
  private final String upper_bound;


  // ------ Temporary members set in the search method -----
  
  private long found_item_start = 0, found_item_end = 0;
  
  

  /**
   * General private constructor.
   */
  private OrderedSetString(DataFile data,
                           Comparator<String> collator,
                           String l_bound, String u_bound) {

    // data and handler may not be null
    if (data == null) {
      throw new NullPointerException("data is null");
    }

    this.data = data;
    this.string_collator = (collator != null) ? collator : LEXI_COLLATOR;
    // This constructor has unlimited bounds.
    this.upper_bound = u_bound;
    this.lower_bound = l_bound;

//    System.out.println("CONSTRUCT " + track_ob.toString());

  }

  /**
   * Subset of a set constructor.
   */
  private OrderedSetString(OrderedSetString root_set,
                           String l_bound, String u_bound) {

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
   * OrderedSetString that has managed a backed DataFile under one collation
   * will not work correctly if the collation is changed.  If such a situation
   * happens, the class function behavior is undefined.
   * 
   * @param data the DataFile object that backs the list.
   * @param collator how strings in the set are ordered or null for
   *    lexicographical ordering.
   */
  public OrderedSetString(DataFile data, Comparator<String> collator) {
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
  public OrderedSetString(DataFile data) {
    this(data, null);
  }
  
  /**
   * Updates the internal state of this object (the start_pos and end_pos
   * objects) if the subset is determined to be dirty (this version is less
   * than the version of the root).  This is necessary for when the list
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
      }
      else if (sz == 8) {
        start_pos = 8;
        end_pos = 8;
      }
      // The none empty state
      else {

        // If there is no lower bound we use start of the list
        if (lower_bound == null) {
          start_pos = 8;
        }
        // If there is a lower bound we search for the string and use it
        else {
          searchFor(lower_bound, 8, sz);
          start_pos = data.position();
        }

        // If there is no upper bound we use end of the list
        if (upper_bound == null) {
          end_pos = sz;
        }
        // Otherwise there is an upper bound so search for the string and use it
        else {
          searchFor(upper_bound, 8, sz);
          end_pos = data.position();
        }
      }

      // Update the version of this to the parent.
      this.version = root_set.version;
    }
  }
  
  /**
   * Returns the string at the given position.
   * 
   * @param s the start of the string in the file.
   * @param e the end of the string (including the 0x0FFFF deliminator).
   * @return the String at the given position.
   * @throws java.io.IOException
   */
  private String stringAtPosition(final long s, final long e) {
    final long to_read = ((e - s) - 2) / 2;
    // If it's too large
    if (to_read > Integer.MAX_VALUE) {
      throw new RuntimeException("String too large to read.");
    }
    data.position(s);
    final int sz = (int) to_read;
    StringBuilder buf = new StringBuilder(sz);
    for (int i = 0; i < sz; ++i) {
      buf.append(data.getChar());
    }
    // Returns the string
    return buf.toString();
  }
  
  /**
   * Removes the string at the position of the data file object.
   */
  private void removeStringAtPosition() {
    // Tell the root set that any child subsets may be dirty
    if (root_set != this) {
      root_set.version += 1;
      root_set.root_state_dirty = true;
    }
    version += 1;
    
    // The number of byte entries to remove
    final long str_remove_size = found_item_start - found_item_end;
    data.position(found_item_end);
    data.shift(str_remove_size);
    this.end_pos = this.end_pos + str_remove_size;
    
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
  private void insertValue(String value) {
    // The string encoding is fairly simple.  Each short represents a
    // UTF-16 encoded character of the string.  We use 0x0FFFF to represent
    // the string record separator (an invalid UTF-16 character) at the end
    // of the string.  This method will not permit a string to be inserted
    // that contains a 0x0FFFF character.

    // Tell the root set that any child subsets may be dirty
    if (root_set != this) {
      root_set.version += 1;
      root_set.root_state_dirty = true;
    }
    version += 1;

    // If the set is empty, we insert the magic value to the start of the
    // data file and update the internal vars as appropriate
    if (data.size() < 8) {
      data.setSize(8);
      data.position(0);
      data.putLong(OSS_MAGIC);
      this.start_pos = 8;
      this.end_pos = 8;
    }

    int len = value.length();
    // Make room in the file for the value being inserted
    final long str_insert_size = ((long) len * 2) + 2;
    final long cur_position = data.position();
    data.shift(str_insert_size);
    // Change the end position
    this.end_pos = this.end_pos + str_insert_size;

    // Insert the characters
    for (int i = 0; i < len; ++i) {
      char c = value.charAt(i);
      // Check if the character is 0x0FFFF, if it is then generate an error
      if (c == (char) 0x0FFFF) {
        // Revert any changes we made
        data.position(cur_position + str_insert_size);
        data.shift(-str_insert_size);
        this.end_pos = this.end_pos - str_insert_size;
        // Throw a runtime exception (don't throw an IO exception because
        // this will cause a critical stop condition).
        throw new RuntimeException(
                "Can not encode invalid UTF-16 character 0x0FFFF");
      }
      data.putChar(c);
    }
    // Write the string deliminator
    data.putChar((char) 0x0FFFF);

  }
  
  /**
   * Search for the string value in the DataFile and return true if found.  
   * When this method returns, the DataFile (data) object will be positioned at
   * either the location to insert the string into the correct
   * order or at the location of the value in the set.
   * <p>
   * We recursively divide up the ordered string list to search for the value.
   * 
   * @param value the value to search for.
   * @param start the start of the file to search for the string.
   * @param end the end of the file to search for the string.
   * @return true if the string was found, false otherwise.
   */
  private boolean searchFor(final String value,
                            final long start, final long end) {
    // If start is end, the list is empty,
    if (start == end) {
      data.position(start);
      return false;
    }
    // How large is the area we are searching in characters?
    long search_len = (end - start) / 2;
    // Read the string from the middle of the area
    final long mid_pos = start + ((search_len / 2) * 2);
    // Search to the end of the string
    long str_end = -1;
    long pos = mid_pos;
    data.position(pos);
    while (pos < end) {
      char c = data.getChar();
      pos = pos + 2;
      if (c == (char) 0x0FFFF) {
        // This is the end of the string, break the while loop
        str_end = pos;
        break;
      }
    }
    // All strings must end with 0x0FFFF.  If this character isn't found before
    // the end is reached then the format of the data is in error.
    if (str_end == -1) {
//      data.position(0);
//      for (int i = 0; i < data.size(); ++i) {
//        byte b = data.get();
//        System.out.println(i + ": " + b + "("+(char)b+"),");
//      }
//      System.out.println();
      throw new RuntimeException("Set data error.");
    }
    // Search for the start of the string
    long str_start = mid_pos - 2;
    while (str_start >= start) {
      data.position(str_start);
      char c = data.getChar();
      if (c == (char) 0x0FFFF) {
        // This means we found the end of the previous string
        // so the start is the next char.
        break;
      }
      str_start = str_start - 2;
    }
    str_start = str_start + 2;
    
    // Now str_start will point to the start of the string and str_end to the
    // end (the char immediately after 0x0FFFF).
    // Read the midpoint string,
    String mid_value = stringAtPosition(str_start, str_end);

    // Compare the values
    int v = string_collator.compare(value, mid_value);
    // If str_start and str_end are the same as start and end, then the area
    // we are searching represents only 1 string, which is a return state
    final boolean last_str = (str_start == start && str_end == end);
    
    if (v < 0) {  // if value < mid_value
      if (last_str) {
        // Position at the start if last str and value < this value
        data.position(str_start);
        return false;
      }
      // We search the head
      return searchFor(value, start, str_start);
    }
    else if (v > 0) {  // if value > mid_value
      if (last_str) {
        // Position at the end if last str and value > this value
        data.position(str_end);
        return false;
      }
      // We search the tail
      return searchFor(value, str_end, end);
    }
    else {  // if value == mid_value
      data.position(str_start);
      // Update internal state variables
      found_item_start = str_start;
      found_item_end = str_end;
      return true;
    }
  }

  // ----------- Implemented from AbstractSet<String> ------------

  /**
   * Returns the total number of elements in the set or Integer.MAX_VALUE if
   * the set contains more values than Integer.MAX_VALUE.
   * <p>
   * PERFORMANCE: This operation will scan the entire set to
   * determine the number of elements, therefore this operation does not
   * scale with large sets.
   */
  public int size() {
    updateInternalState();
    // Iterate through the entire data file counting the number of
    // deliminators
    int list_size = 0;
    long p = this.start_pos;
    long size = this.end_pos;
    data.position(p);
    while (list_size < Integer.MAX_VALUE && p < size) {
      char c = data.getChar();
      if (c == (char) 0x0FFFF) {
        ++list_size;
      }
      p += 2;
    }
    return list_size;
  }
  
  /**
   * Returns true if the set is empty.
   */
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
  public Iterator<String> iterator() {
    // Note - important we update internal state here because start_pos and
    //   end_pos used by the inner class.
    updateInternalState();
    return new StringSetIterator();
  }
  
  /**
   * Returns true if the set contains the given string.  Assumes the set is
   * ordered by the collator.
   * 
   * @param str the value to search for.
   * @return true if the set contains the string.
   */
  public boolean contains(Object str) {
    if (str == null) throw new NullPointerException();
    updateInternalState();
    // Look for the string in the file.
    return searchFor((String) str, this.start_pos, this.end_pos);
  }
  
  /**
   * Adds a string to the set in sorted order as defined by the collator
   * defined when the object is created.  Returns true if the set does not
   * contain the string and the string was added, false if the set already
   * contains the value. 
   */
  public boolean add(String value) {

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
   * Removes the value from the set if it is present.  Assumes the set is
   * ordered by the collator.
   * 
   * @param value the String to remove.
   * @return true if the value was removed.
   */
  public boolean remove(Object value) {

//    System.out.println("remove: " + value);

    if (value == null) throw new NullPointerException();
    updateInternalState();
    // Find the index in the list of the value either equal to the given value
    // or the first value in the set comparatively more than the given value.
    boolean found = searchFor((String) value, this.start_pos, this.end_pos);
    // If the value was found,
    if (found) {
      // Remove it
      removeStringAtPosition();
    }
    return found;
  }

  /**
   * Clears the set of all string items.
   */
  public void clear() {

//    System.out.println("clear");

    updateInternalState();

    // Tell the root set that any child subsets may be dirty
    if (root_set != this) {
      root_set.version += 1;
      root_set.root_state_dirty = true;
    }
    version += 1;

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
  private String bounded(String str) {
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
  public Comparator<String> comparator() {
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
  public SortedSet<String> subSet(String from_element, String to_element) {
    // check the bounds not out of range of the parent bounds
    from_element = bounded(from_element);
    to_element = bounded(to_element);
    return new OrderedSetString(root_set, from_element, to_element);
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
  public SortedSet<String> headSet(String to_element) {
    to_element = bounded(to_element);
    return new OrderedSetString(root_set, lower_bound, to_element);
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
  public SortedSet<String> tailSet(String from_element) {
    from_element = bounded(from_element);
    return new OrderedSetString(root_set, from_element, upper_bound);
  }

  /**
   * Returns the first (lowest) string item currently in this set.
   *
   * @return the first (lowest) element currently in this set
   */
  public String first() {
    updateInternalState();

    if (start_pos >= end_pos) {
      throw new NoSuchElementException();
    }

    // Get the first entry
    data.position(start_pos);
    StringBuilder str_buf = new StringBuilder();
    while (true) {
      char c = data.getChar();
      if (c == (char) 0x0FFFF) {
        break;
      }
      str_buf.append(c);
    }
    return str_buf.toString();
  }

  /**
   * Returns the last (highest) string item currently in this set.
   *
   * @return the last (highest) element currently in this set
   */
  public String last() {
    updateInternalState();

    if (start_pos >= end_pos) {
      throw new NoSuchElementException();
    }

    // Get the last entry
    long p = end_pos - 2;
    while (p > start_pos) {
      data.position(p);
      char c = data.getChar();
      if (c == (char) 0x0FFFF) {
        p = p + 2;
        break;
      }
      p = p - 2;
    }
    return stringAtPosition(p, end_pos);
  }

  // ---------- Inner classes ----------
  
  /**
   * An iterator for strings in this set.
   */
  private class StringSetIterator implements Iterator<String> {
    
    // The version of which this is derived,
    private long ver;
    // Offset of the iterator
    private long offset;
    // Last string position
    private long last_str_start = -1;
    
    /**
     * Constructor.
     */
    StringSetIterator() {
      this.ver = root_set.version;
      this.offset = 0;
    }

    /**
     * Generates an exception if this version doesn't match the root (
     * concurrent list modification occurred).
     */
    private void versionCheck() {
      if (ver < root_set.version) {
        throw new IllegalStateException("Concurrent set update");
      }
    }
    
    
    public boolean hasNext() {
      versionCheck();
      return start_pos + offset < end_pos;
    }

    public String next() {
      versionCheck();
      long p = start_pos + offset;
      last_str_start = p;
      data.position(p);
      StringBuilder buf = new StringBuilder();
      while (true) {
        char c = data.getChar();
        offset += 2;
        if (c == (char)0x0FFFF) {
          break;
        }
        buf.append(c);
      }
      return buf.toString();
    }

    public void remove() {
      versionCheck();
      if (last_str_start == -1) {
        throw new IllegalStateException();
      }
      found_item_start = last_str_start;
      found_item_end = start_pos + offset;
      // Remove the string
      removeStringAtPosition();
      // Update internal state
      offset = last_str_start - start_pos;
      last_str_start = -1;
      // Update the version of this iterator
      ver = ver + 1;
    }

  }
  
}
