/**
 * com.mckoi.util.StringUtil  17 Dec 1999
 *
 * Mckoi Database Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2010  Diehl and Associates, Inc.
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

package com.mckoi.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.io.*;

/**
 * Various String utilities.
 *
 * @author Tobias Downer
 */

public class StringUtil {

  /**
   * Finds the index of the given string in the source string.
   * <p>
   * @return -1 if the 'find' string could not be found.
   */
  public static int find(String source, String find) {
    return source.indexOf(find);

//    int find_index = 0;
//    int len = source.length();
//    int find_len = find.length();
//    int i = 0;
//    for (; i < len; ++i) {
//      if (find_index == find_len) {
//        return i - find_len;
//      }
//      if (find.indexOf(source.charAt(i), find_index) == find_index) {
//        ++find_index;
//      }
//      else {
//        find_index = 0;
//      }
//    }
//    if (find_index == find_len) {
//      return i - find_len;
//    }
//    else {
//      return -1;
//    }
  }

  /**
   * Encodes a string by quoting it and using slash notation for deliminator
   * characters.  For example, the string 'test' will become '"test"'.  The
   * string 'sometests"' will become "sometests\"".
   */
  public static String quote(String str) {
    // Look for characters that need quoting
    int sz = str.length();
    for (int i = 0; i < sz; ++i) {
      char c = str.charAt(i);
      if (c == '"' || c == '\\') {
        str = str.substring(0, i) + "\\" + str.substring(i);
      }
    }
    // Returns the string.
    return '"' + str + '"';
  }

  /**
   * Performs an 'explode' operation on the given source string.  This
   * algorithm finds all instances of the deliminator string, and returns an
   * array of sub-strings of between the deliminator.  For example,
   * <code>
   *   explode("10:30:40:55", ":") = ({"10", "30", "40", "55"})
   * </code>
   */
  public static List explode(String source, String deliminator) {
    ArrayList list = new ArrayList();
    int i = find(source, deliminator);
    while (i != -1) {
      list.add(source.substring(0, i));
      source = source.substring(i + deliminator.length());
      i = find(source, deliminator);
    }
    list.add(source);
    return list;
  }

  /**
   * This is the inverse of 'explode'.  It forms a string by concatinating
   * each string in the list and seperating each with a deliminator string.
   * For example,
   * <code>
   *   implode(({"1", "150", "500"}), ",") = "1,150,500"
   * </code>
   */
  public static String implode(List list, String deliminator) {
    StringBuffer str = new StringBuffer();
    Iterator iter = list.iterator();
    boolean has_next = iter.hasNext();
    while (has_next) {
      str.append(iter.next().toString());
      has_next = iter.hasNext();
      if (has_next) {
        str.append(deliminator);
      }
    }
    return new String(str);
  }

  /**
   * Converts a String[] array into a single, comma deliminated string.
   */
  public static String encodeStringList(List list) {
    // Encode each string into a new list
    List new_list = new ArrayList(list.size());
    for (int i = 0; i < list.size(); ++i) {
      // Any commas in the string are double encoded
      String str = quote(list.get(i).toString());
      // Make the new list
      new_list.add(str);
    }
    return implode(new_list, ",");
    
  }

  /**
   * Parses a comma deliminated string and turns it into a List of strings.
   */
  public static List parseStringList(String str) {
    ArrayList list = new ArrayList();
    int sz = str.length();
    if (sz == 0) {
      return list;
    }
    boolean inside_str_element = false;
    boolean slash = false;
    StringBuilder cur_string = new StringBuilder();
    for (int i = 0; i < sz; ++i) {
      char c = str.charAt(i);
      if (slash) {
        cur_string.append(c);
        slash = false;
      }
      else if (c == '"') {
        inside_str_element = !inside_str_element;
      }
      else if (c == '\\') {
        slash = true;
      }
      else {
        // Inside a string element
        if (inside_str_element) {
          cur_string.append(c);
        }
        else {
          // Outside a string element
          if (c == ',') {
            // Add the string to the list and clear cur_string
            list.add(cur_string.toString());
            cur_string.setLength(0);
          }
        }
      }
    }
    list.add(cur_string.toString());
    return list;
  }
  
  /**
   * Searches for various instances of the 'search' string and replaces them
   * with the 'replace' string.
   */
  public static String searchAndReplace(
                                String source, String search, String replace) {
    return implode(explode(source, search), replace);
  }

  public static void main(String[] args) {
    System.out.println(parseStringList(""));
    System.out.println(parseStringList("\"elem1\""));
    System.out.println(parseStringList("\"elem1\",\"elem2\""));
    System.out.println(parseStringList("\"elem1\",\"elem2\",\"elem3\""));
    System.out.println(parseStringList("\"elem1\\\"\",\"elem2\",\"elem3\""));
  }
  
}
