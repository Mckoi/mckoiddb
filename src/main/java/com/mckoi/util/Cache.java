/**
 * com.mckoi.util.Cache  21 Mar 1998
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

/**
 * Represents a cache of Objects.  A Cache is similar to a Hashtable, in that
 * you can 'add' and 'get' objects from the container given some key.  However
 * a cache may remove objects from the container when it becomes too full.
 * <p>
 * The cache scheme uses a doubly linked-list hashtable.  The most recently
 * accessed objects are moved to the start of the list.  The end elements in
 * the list are wiped if the cache becomes too full.
 *
 * @author Tobias Downer
 */

public class Cache {

  /**
   * The maximum number of DataCell objects that can be stored in the cache
   * at any one time.
   */
  private int max_cache_size;

  /**
   * The current cache size.
   */
  private int current_cache_size;

  /**
   * The number of nodes that should be left available when the cache becomes
   * too full and a clean up operation occurs.
   */
  private int wipe_to;

  /**
   * The array of ListNode objects arranged by hashing value.
   */
  private final ListNode[] node_hash;

  /**
   * A pointer to the start of the list.
   */
  private ListNode list_start;

  /**
   * A pointer to the end of the list.
   */
  private ListNode list_end;

  /**
   * The Constructors.  It takes a maximum size the cache can grow to, and the
   * percentage of the cache that is wiped when it becomes too full.
   */
  public Cache(int hash_size, int max_size, int clean_percentage) {
    if (clean_percentage >= 85) {
      throw new RuntimeException(
                "Can't set to wipe more than 85% of the cache during clean.");
    }
    max_cache_size = max_size;
    current_cache_size = 0;
    wipe_to = max_size - ((clean_percentage * max_size) / 100);

    node_hash = new ListNode[hash_size];

    list_start = null;
    list_end = null;
  }

  public Cache(int max_size, int clean_percentage) {
    this((max_size * 2) + 1, max_size, 20);
  }

  public Cache(int max_size) {
    this(max_size, 20);
  }

  public Cache() {
    this(50);
  }

//  /**
//   * Creates the HashMap object to store objects in this cache.  This is
//   * available to be overwritten.
//   * @deprecated
//   */
//  protected final int getHashSize() {
//    return (int) (max_cache_size * 2) + 1;
//  }

  /**
   * This is called whenever at Object is put into the cache.  This method
   * should determine if the cache should be cleaned and call the clean
   * method if appropriate.
   */
  protected void checkClean() {
    // If we have reached maximum cache size, remove some elements from the
    // end of the list
    if (current_cache_size >= max_cache_size) {
      clean();
    }
  }

  /**
   * Returns true if the clean-up method that periodically cleans up the
   * cache, should clean up more elements from the cache.
   */
  protected boolean shouldWipeMoreNodes() {
    return (current_cache_size >= wipe_to);
  }

  /**
   * Notifies that the given object and key has been added to the cache.
   */
  protected void notifyObjectAdded(Object key, Object val) {
    ++current_cache_size;
  }
  
  /**
   * Notifies that the given object and key has been removed from the cache.
   */
  protected void notifyObjectRemoved(Object key, Object val) {
    --current_cache_size;
  }

  /**
   * Notifies that the cache has been entirely cleared of all elements.
   */
  protected void notifyAllCleared() {
    current_cache_size = 0;
  }
  
  /**
   * Notifies that the given object has been wiped from the cache by the
   * clean up procedure.
   */
  protected void notifyWipingNode(Object ob) {
  }

  /**
   * Notifies that some statistical information about the hash map has
   * updated.  This should be used to compile statistical information about
   * the number of walks a 'get' operation takes to retreive an entry from
   * the hash.
   * <p>
   * This method is called every 8192 gets.
   */
  protected void notifyGetWalks(long total_walks, long total_get_ops) {
  }

  // ---------- Hashing methods ----------

  /**
   * Some statistics about the hashing algorithm.
   */
  private long total_gets = 0;
  private long get_total = 0;

  /**
   * Finds the node with the given key in the hash table and returns it.
   * Returns 'null' if the value isn't in the hash table.
   */
  private ListNode getFromHash(Object key) {
    int hash = key.hashCode();
    int index = (hash & 0x7FFFFFFF) % node_hash.length;
    int get_count = 1;

    for (ListNode e = node_hash[index]; e != null; e = e.next_hash_entry) {
      if (key.equals(e.key)) {
        ++total_gets;
        get_total += get_count;

        // Every 8192 gets, call the 'notifyGetWalks' method with the
        // statistical info.
        if ((total_gets & 0x01FFF) == 0) {
          try {
            notifyGetWalks(get_total, total_gets);
            // Reset stats if we overflow on an int
            if (get_total > (65536 * 65536)) {
              get_total = 0;
              total_gets = 0;
            }
          }
          catch (Throwable except) { /* ignore */ }
        }

        // Bring to head if get_count > 1
        if (get_count > 1) {
          bringToHead(e);
        }
        return e;
      }
      ++get_count;
    }
    return null;
  }

  /**
   * Puts the node with the given key into the hash table.
   */
  private ListNode putIntoHash(ListNode node) {
    // Makes sure the key is not already in the HashMap.
    int hash = node.key.hashCode();
    int index = (hash & 0x7FFFFFFF) % node_hash.length;
    Object key = node.key;
    for (ListNode e = node_hash[index] ; e != null ; e = e.next_hash_entry) {
      if (key.equals(e.key)) {
        throw new Error(
                "ListNode with same key already in the hash - remove first.");
      }
    }

    // Stick it in the hash list.
    node.next_hash_entry = node_hash[index];
    node_hash[index] = node;

    return node;
  }

  /**
   * Removes the given node from the hash table.  Returns 'null' if the entry
   * wasn't found in the hash.
   */
  private ListNode removeFromHash(Object key) {
    // Makes sure the key is not already in the HashMap.
    int hash = key.hashCode();
    int index = (hash & 0x7FFFFFFF) % node_hash.length;
    ListNode prev = null;
    for (ListNode e = node_hash[index] ; e != null ; e = e.next_hash_entry) {
      if (key.equals(e.key)) {
        // Found entry, so remove it baby!
        if (prev == null) {
          node_hash[index] = e.next_hash_entry;
        }
        else {
          prev.next_hash_entry = e.next_hash_entry;
        }
        return e;
      }
      prev = e;
    }

    // Not found so return 'null'
    return null;
  }

  /**
   * Clears the entire hashtable of all entries.
   */
  private void clearHash() {
    for (int i = node_hash.length - 1; i >= 0; --i) {
      node_hash[i] = null;
    }
  }


  // ---------- Public cache methods ----------

  /**
   * Returns the number of nodes that are currently being stored in the
   * cache.
   */
  public int nodeCount() {
    return current_cache_size;
  }

  /**
   * Puts an Object into the cache with the given key.
   */
  public final void put(Object key, Object ob) {

    // Do we need to clean any cache elements out?
    checkClean();

    // Check whether the given key is already in the Hashtable.

    ListNode node = getFromHash(key);
    if (node == null) {

      node = createListNode();
      node.key = key;
      node.contents = ob;

      // Add node to top.
      node.next = list_start;
      node.previous = null;
      list_start = node;
      if (node.next == null) {
        list_end = node;
      }
      else {
        node.next.previous = node;
      }

      notifyObjectAdded(key, ob);

      // Add node to key mapping
      putIntoHash(node);

    }
    else {

      // If key already in Hashtable, all we need to do is set node with
      // the new contents and bring the node to the start of the list.

      node.contents = ob;
      bringToHead(node);

    }

  }

  /**
   * If the cache contains the cell with the given key, this method will
   * return the object.  If the cell is not in the cache, it returns null.
   */
  public final Object get(Object key) {
    ListNode node = getFromHash(key);

    if (node != null) {
      // Bring node to start of list.
//      bringToHead(node);

      return node.contents;
    }

    return null;
  }

  /**
   * Ensures that there is no cell with the given key in the cache.  This is
   * useful for ensuring the cache does not contain out-dated information.
   */
  public final Object remove(Object key) {
    ListNode node = removeFromHash(key);

    if (node != null) {
      // If removed node at head.
      if (list_start == node) {
        list_start = node.next;
        if (list_start != null) {
          list_start.previous = null;
        }
        else {
          list_end = null;
        }
      }
      // If removed node at end.
      else if (list_end == node) {
        list_end = node.previous;
        if (list_end != null) {
          list_end.next = null;
        }
        else {
          list_start = null;
        }
      }
      else {
        node.previous.next = node.next;
        node.next.previous = node.previous;
      }

      Object contents = node.contents;
      notifyObjectRemoved(key, contents);

      // Set internals to null to ensure objects get gc'd
      node.contents = null;
      node.key = null;

      return contents;
    }

    return null;
  }

  /**
   * Clear the cache of all the entries.
   */
  public void removeAll() {
    clearHash();
    notifyAllCleared();
//    if (current_cache_size != 0) {
//      current_cache_size = 0;
//      clearHash();
//    }
    list_start = null;
    list_end = null;
  }

  public void clear() {
    removeAll();
  }


  /**
   * Creates a new ListNode.  If there is a free ListNode on the
   * 'recycled_nodes' then it obtains one from there, else it creates a new
   * blank one.
   */
  private final ListNode createListNode() {
    return new ListNode();
  }

  /**
   * Cleans away some old elements in the cache.  This method walks from the
   * end, back 'wipe_count' elements putting each object on the recycle stack.
   *
   * @returns the number entries that were cleaned.
   */
  protected final int clean() {

    ListNode node = list_end;
    if (node == null) {
      return 0;
    }

    int actual_count = 0;
    while (node != null && shouldWipeMoreNodes()) {
      Object nkey = node.key;
      Object ncontents = node.contents;

      notifyWipingNode(ncontents);

      removeFromHash(nkey);
      // Help garbage collector with old objects
      node.contents = null;
      node.key = null;
      ListNode old_node = node;
      // Move to previous node
      node = node.previous;

      // Help the GC by clearing away the linked list nodes
      old_node.next = null;
      old_node.previous = null;

      notifyObjectRemoved(nkey, ncontents);
      ++actual_count;
    }

    if (node != null) {
      node.next = null;
      list_end = node;
    }
    else {
      list_start = null;
      list_end = null;
    }

    return actual_count;
  }

  /**
   * Brings 'node' to the start of the list.  Only nodes at the end of the
   * list are cleaned.
   */
  private final void bringToHead(ListNode node) {
    if (list_start != node) {

      ListNode next_node = node.next;
      ListNode previous_node = node.previous;

      node.next = list_start;
      node.previous = null;
      list_start = node;
      node.next.previous = node;

      if (next_node != null) {
        next_node.previous = previous_node;
      }
      else {
        list_end = previous_node;
      }
      previous_node.next = next_node;

    }
  }

  /**
   * Returns a prime number from PRIME_LIST that is the closest prime greater
   * or equal to the given value.
   */
  public static int closestPrime(int value) {
    for (int i = 0; i < PRIME_LIST.length; ++i) {
      if (PRIME_LIST[i] >= value) {
        return PRIME_LIST[i];
      }
    }
    // Return the last prime
    return PRIME_LIST[PRIME_LIST.length - 1];
  }

  /**
   * A list of primes ordered from lowest to highest.
   */
  private final static int[] PRIME_LIST = new int[] {
     3001, 4799, 13999, 15377, 21803, 24247, 35083, 40531, 43669, 44263, 47387,
     50377, 57059, 57773, 59399, 59999, 75913, 96821, 140551, 149011, 175633,
     176389, 183299, 205507, 209771, 223099, 240259, 258551, 263909, 270761,
     274679, 286129, 290531, 296269, 298021, 300961, 306407, 327493, 338851,
     351037, 365489, 366811, 376769, 385069, 410623, 430709, 433729, 434509,
     441913, 458531, 464351, 470531, 475207, 479629, 501703, 510709, 516017,
     522211, 528527, 536311, 539723, 557567, 593587, 596209, 597451, 608897,
     611069, 642547, 670511, 677827, 679051, 688477, 696743, 717683, 745931,
     757109, 760813, 763957, 766261, 781559, 785597, 788353, 804493, 813559,
     836917, 854257, 859973, 883217, 884789, 891493, 902281, 910199, 915199,
     930847, 939749, 940483, 958609, 963847, 974887, 983849, 984299, 996211,
     999217, 1007519, 1013329, 1014287, 1032959, 1035829, 1043593, 1046459,
     1076171, 1078109, 1081027, 1090303, 1095613, 1098847, 1114037, 1124429,
     1125017, 1130191, 1159393, 1170311, 1180631, 1198609, 1200809, 1212943,
     1213087, 1226581, 1232851, 1287109, 1289867, 1297123, 1304987, 1318661,
     1331107, 1343161, 1345471, 1377793, 1385117, 1394681, 1410803, 1411987,
     1445261, 1460497, 1463981, 1464391, 1481173, 1488943, 1491547, 1492807,
     1528993, 1539961, 1545001, 1548247, 1549843, 1551001, 1553023, 1571417,
     1579099, 1600259, 1606153, 1606541, 1639751, 1649587, 1657661, 1662653,
     1667051, 1675273, 1678837, 1715537, 1718489, 1726343, 1746281, 1749107,
     1775489, 1781881, 1800157, 1806859, 1809149, 1826753, 1834607, 1846561,
     1849241, 1851991, 1855033, 1879931, 1891133, 1893737, 1899137, 1909513,
     1916599, 1917749, 1918549, 1919347, 1925557, 1946489, 1961551, 1965389,
     2011073, 2033077, 2039761, 2054047, 2060171, 2082503, 2084107, 2095099,
     2096011, 2112193, 2125601, 2144977, 2150831, 2157401, 2170141, 2221829,
     2233019, 2269027, 2270771, 2292449, 2299397, 2303867, 2309891, 2312407,
     2344301, 2348573, 2377007, 2385113, 2386661, 2390051, 2395763, 2422999,
     2448367, 2500529, 2508203, 2509841, 2513677, 2516197, 2518151, 2518177,
     2542091, 2547469, 2549951, 2556991, 2563601, 2575543, 2597629, 2599577,
     2612249, 2620003, 2626363, 2626781, 2636773, 2661557, 2674297, 2691571,
     2718269, 2725691, 2729381, 2772199, 2774953, 2791363, 2792939, 2804293,
     2843021, 2844911, 2851313, 2863519, 2880797, 2891821, 2897731, 2904887,
     2910251, 2928943, 2958341, 2975389
  };





  // ---------- Inner classes ----------

  /**
   * An element in the linked list structure.
   */
  static final class ListNode {

    /**
     * Links to the next and previous nodes.  The ends of the list are 'null'
     */
    ListNode next;
    ListNode previous;

    /**
     * The next node in the hash link on this hash value, or 'null' if last
     * hash entry.
     */
    ListNode next_hash_entry;


    /**
     * The key in the Hashtable for this object.
     */
    Object key;

    /**
     * The object contents for this element.
     */
    Object contents;

  }

}
