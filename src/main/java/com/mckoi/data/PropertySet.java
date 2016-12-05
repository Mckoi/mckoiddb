/*
 * Mckoi Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2015  Diehl and Associates, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mckoi.data;

import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.AbstractSet;
import com.mckoi.util.PropertyRead;
import com.mckoi.util.PropertyWrite;

/**
 * A set of string key to value properties mapped over a DataFile object.  This
 * class provides a convenient way to query and map text and basic
 * numeric properties in a DataFile.  This object stores all key and value
 * data as 16 bit unicode character strings.
 * <p>
 * The format used to store the property map is very simple; Each key/value
 * pair is a variable length string wherein the key and value is delimited
 * with an '=' character.  All properties are stored in an OrderedSetString
 * object in lexicographical order by the property key.  A key lookup is
 * therefore a binary search over the set of all strings.
 * <p>
 * Note that this object inherits the performance characteristics of
 * OrderedSetString.  All operations are efficient regardless of the
 * size of the property set with the exception of the 'size()' query (the
 * number of key/value pairs stored in the map).
 * <p>
 * Limitations: This object can not store 'null' values or keys, or can it
 * store keys that contain an '=' character.  Also, key and value strings may
 * not contain the character code '0x0FFFF' (an illegal unicode character).
 *
 * @author Tobias Downer
 */

public class PropertySet implements PropertyRead, PropertyWrite {

  // ------ Statics -----
  
  private static final Comparator<String> PROPERTY_COLLATOR, KEYS_COLLATOR;
  
  static {
    PROPERTY_COLLATOR = new Comparator<String>() {
      public int compare(String str1, String str2) {
        // Compare the keys of the string
        return keyValuePart(str1).compareTo(keyValuePart(str2));
      }
    };
    KEYS_COLLATOR = new Comparator<String>() {
      public int compare(String str1, String str2) {
        return str1.compareTo(str2);
      }
    };
  }
  
  // ----- Member variables -----
  
  /**
   * The backed OrderedSetString that contains the property keys and values.
   */
  private OrderedSetString strings_set;

  /**
   * Constructs the property set mapped over the given DataFile object.
   */
  public PropertySet(DataFile data) {
    strings_set = new OrderedSetString(data, PROPERTY_COLLATOR);    
  }

  /**
   * Returns the key value part of a string entry in the set.
   */
  private static String keyValuePart(String val) {
    int delim = val.indexOf('=');
    if (delim == -1) {
      return val;
    }
    else {
      return val.substring(0, delim);
    }
  }

  /**
   * Generates an exception if the key contains invalid characters.  Throws
   * NullPointerException is the key is null.
   */
  private void checkValidKey(String key) {
    int sz = key.length();
    for (int i = 0; i < sz; ++i) {
      char c = key.charAt(i);
      if (c == '=') {
        throw new RuntimeException("Invalid character in key.");
      }
    }
  }
  
  /**
   * Sets a property value in the set.  If the property already exists, it
   * is overwritten.  If 'value' is null, the property is removed.
   */
  public void setProperty(final String key, final String value) {
    checkValidKey(key);

    // If the value is being removed, remove the key from the set,
    if (value == null) {
      // If the key isn't found in 'strings_set', then nothing changes,
      strings_set.remove(key);
    }
    else {
      // Get the current value for this key in the set
      String cur_val = getProperty(key);
      // If there's a value currently stored,
      if (cur_val != null) {
        // If we are setting the key to the same value, we exit the function
        // early because nothing needs to be done,
        if (cur_val.equals(value)) {
          return;
        }
        // Otherwise remove the existing key
        strings_set.remove(key);
      }
      // Add the key
      StringBuilder str_buf =
                       new StringBuilder(key.length() + value.length() + 1);
      str_buf.append(key);
      str_buf.append('=');
      str_buf.append(value);
      strings_set.add(str_buf.toString());
    }
  }

  /**
   * Returns a property value from the set given the key.  Returns null if
   * the property doesn't exist.
   */
  public String getProperty(String key) {
    checkValidKey(key);
    SortedSet<String> s1 = strings_set.tailSet(key);
    if (!s1.isEmpty()) {
      String entry = s1.first();
      int delim = entry.indexOf('=');
      if (entry.substring(0, delim).equals(key)) {
        // Found the key, so return the value
        return entry.substring(delim + 1);
      }
    }
    return null;
  }

  public String getProperty(String key, String default_value) {
    String str = getProperty(key);
    if (str == null) {
      return default_value;
    }
    return str;
  }
  
  public void setIntegerProperty(String key, int val) {
    setProperty(key, Integer.toString(val));
  }
  
  public int getIntegerProperty(String key, int default_value) {
    String str = getProperty(key);
    if (str == null) {
      return default_value;
    }
    return Integer.parseInt(str);
  }

  public void setLongProperty(String key, long val) {
    setProperty(key, Long.toString(val));
  }

  public long getLongProperty(String key, long default_value) {
    String str = getProperty(key);
    if (str == null) {
      return default_value;
    }
    return Long.parseLong(str);
  }

  public void setBooleanProperty(String key, boolean val) {
    setProperty(key, val ? "true" : "false");
  }

  public boolean getBooleanProperty(String key, boolean default_value) {
    String str = getProperty(key);
    if (str == null) {
      return default_value;
    }
    return str.equals("true");
  }

  /**
   * Returns a sorted set of all keys in this property set.  The returned
   * object supports removing values from the property set but not adding new
   * keys.  The 'subSet' query provided by the returned SortedSet is
   * backed by the OrderedSetString object.  Note that the 'size' query
   * requires a full scan of the data file to compute so care should be
   * taken when using this query.
   */
  public SortedSet<String> keySet() {
    return new PropertyKeySet(strings_set);
  }

  /**
   * Returns a representation of all key/value properties stored in the
   * DataFile.  This operation will iterate over every key/value pair stored
   * in this object, therefore care should be taken when using this on a
   * property set storing a very large number of items.
   */
  public String toString() {
    StringBuilder str_buf = new StringBuilder();
    Iterator<String> i = strings_set.iterator();
    str_buf.append("[");
    if (i.hasNext()) {
      str_buf.append(i.next());
    }
    while (i.hasNext()) {
      str_buf.append(", ");
      str_buf.append(i.next());
    }
    str_buf.append("]");
    return str_buf.toString();
  }
  
  // ---------- Inner classes ----------
  
  /**
   * Sorted set wrapper for property names.
   */
  private static class PropertyKeySet extends AbstractSet<String>
                                                implements SortedSet<String> {
    
    private SortedSet<String> backed_set;
    
    PropertyKeySet(SortedSet<String> set) {
      backed_set = set;
    }

    public int size() {
      return backed_set.size();
    }

    public boolean isEmpty() {
      return backed_set.isEmpty();
    }
    
    public Iterator<String> iterator() {
      return new KeySetIterator(backed_set.iterator());
    }

    public boolean contains(Object key) {
      return backed_set.contains(key);
    }
    
    public boolean remove(Object key) {
      return backed_set.remove(key);
    }
    
    public void clear() {
      backed_set.clear();
    }
    
    public Comparator<String> comparator() {
      return KEYS_COLLATOR;
    }

    public SortedSet<String> subSet(String from_e, String to_e) {
      return new PropertyKeySet(backed_set.subSet(from_e, to_e));
    }

    public SortedSet<String> headSet(String to_e) {
      return new PropertyKeySet(backed_set.headSet(to_e));
    }

    public SortedSet<String> tailSet(String from_e) {
      return new PropertyKeySet(backed_set.tailSet(from_e));
    }

    public String first() {
      return keyValuePart(backed_set.first());
    }

    public String last() {
      return keyValuePart(backed_set.last());
    }
    
  }
  
  private static class KeySetIterator implements Iterator<String> {
    
    private Iterator<String> backed_iterator;
    
    KeySetIterator(Iterator<String> it) {
      backed_iterator = it;
    }
    
    public String next() {
      return keyValuePart(backed_iterator.next());
    }
    
    public boolean hasNext() {
      return backed_iterator.hasNext();
    }
    
    public void remove() {
      backed_iterator.remove();
    }
  }

}
