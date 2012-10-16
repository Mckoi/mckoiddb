/**
 * com.mckoi.util.Cache  21 Mar 1998
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

package com.mckoi.util;

import java.util.Arrays;

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
  private ListNode createListNode() {
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
  private void bringToHead(ListNode node) {
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
   * Returns a prime number from PRIME_LIST that is a decent candidate size for
   * a hash table that is at least as large as the given value.
   */
  public static int closestPrime(int value) {

    // Search for the value in the array,
    int i = Arrays.binarySearch(PRIME_LIST, value);
    if (i < 0) i = -(i + 1);
    i = Math.min(i, PRIME_LIST.length - 1);
    return PRIME_LIST[i];

//    for (int i = 0; i < PRIME_LIST.length; ++i) {
//      if (PRIME_LIST[i] >= value) {
//        return PRIME_LIST[i];
//      }
//    }
//    // Return the last prime
//    return PRIME_LIST[PRIME_LIST.length - 1];
  }

  /**
   * A pre-generated list of nice primes to use for cache sizes, ordered from
   * lowest to highest.
   */
  private final static int[] PRIME_LIST = new int[] {
    
23, 47, 71, 97, 127, 149, 173, 197, 223, 251, 277, 
307, 331, 353, 379, 401, 431, 457, 479, 503, 541, 563, 
587, 613, 641, 673, 701, 727, 751, 773, 797, 821, 853, 
877, 907, 929, 953, 977, 1009, 1031, 1061, 1087, 1109, 1151, 
1181, 1213, 1237, 1259, 1283, 1307, 1361, 1399, 1423, 1447, 1471, 
1493, 1523, 1549, 1571, 1597, 1619, 1657, 1693, 1721, 1747, 1777, 
1801, 1823, 1847, 1871, 1901, 1931, 1973, 1997, 2027, 2053, 2081, 
2111, 2137, 2161, 2203, 2237, 2267, 2293, 2333, 2357, 2381, 2411, 
2437, 2459, 2503, 2531, 2557, 2579, 2609, 2633, 2657, 2683, 2707, 
2729, 2753, 2777, 2801, 2833, 2857, 2879, 2903, 2927, 2953, 2999, 
3023, 3049, 3079, 3109, 3137, 3163, 3187, 3209, 3251, 3299, 3323, 
3347, 3371, 3407, 3433, 3457, 3491, 3517, 3539, 3571, 3593, 3617, 
3643, 3671, 3697, 3719, 3761, 3793, 3821, 3847, 3877, 3907, 3929, 
3967, 3989, 4013, 4049, 4079, 4111, 4153, 4201, 4241, 4283, 4327, 
4373, 4421, 4481, 4547, 4597, 4649, 4703, 4759, 4817, 4877, 4937, 
4999, 5077, 5147, 5227, 5297, 5381, 5471, 5557, 5639, 5737, 5821, 
5923, 6011, 6101, 6197, 6299, 6397, 6521, 6619, 6719, 6823, 6947, 
7057, 7177, 7297, 7411, 7529, 7649, 7789, 7919, 8053, 8179, 8311, 
8443, 8573, 8707, 8849, 8999, 9137, 9277, 9419, 9587, 9733, 9883, 
10037, 10193, 10357, 10513, 10687, 10847, 11027, 11197, 11369, 11549, 11719, 
11897, 12071, 12251, 12433, 12613, 12799, 12983, 13171, 13367, 13567, 13759, 
13963, 14159, 14369, 14591, 14797, 15013, 15227, 15439, 15649, 15877, 16091, 
16319, 16547, 16787, 17011, 17239, 17467, 17707, 17939, 18181, 18427, 18671, 
18911, 19157, 19403, 19661, 19913, 20161, 20411, 20663, 20921, 21179, 21467, 
21727, 21991, 22259, 22531, 22807, 23081, 23357, 23633, 23909, 24197, 24481, 
24763, 25057, 25343, 25633, 25931, 26227, 26539, 26839, 27143, 27449, 27751, 
28057, 28387, 28697, 29009, 29327, 29641, 29959, 30293, 30631, 30971, 31307, 
31643, 31973, 32303, 32647, 32983, 33329, 33679, 34019, 34361, 34721, 35069, 
35419, 35771, 36131, 36493, 36857, 37217, 37579, 37951, 38317, 38693, 39079, 
39451, 39827, 40213, 40591, 40973, 41357, 41759, 42157, 42557, 42953, 43391, 
43783, 44179, 44579, 44983, 45389, 45817, 46229, 46639, 47051, 47491, 47903, 
48337, 48757, 49177, 49597, 50021, 50459, 50891, 51329, 51767, 52201, 52639, 
53077, 53527, 53987, 54437, 54881, 55331, 55787, 56237, 56701, 57163, 57637, 
58099, 58567, 59029, 59497, 59971, 60443, 60913, 61403, 61879, 62383, 62861, 
63347, 63839, 64327, 64817, 65309, 65809, 66301, 66797, 67307, 67807, 68311, 
68813, 69317, 69827, 70351, 70867, 71387, 71909, 72431, 72949, 73471, 73999, 
74527, 75079, 75611, 76147, 76679, 77213, 77761, 78301, 78853, 79397, 79943, 
80489, 81041, 81611, 82163, 82721, 83299, 83857, 84421, 84991, 85571, 86137, 
86711, 87281, 87853, 88427, 89003, 89591, 90173, 90787, 91373, 91961, 92551, 
93151, 93761, 94379, 94993, 95597, 96199, 96821, 97429, 98041, 98663, 99277, 
99901, 100517, 101141, 101771, 102397, 103043, 103669, 104297, 104933, 105563, 106207, 
106853, 107507, 108161, 108803, 109451, 110119, 110771, 111427, 112087, 112741, 113417, 
114073, 114743, 115421, 116089, 116789, 117497, 118169, 118843, 119533, 120209, 120889, 
121571, 122251, 122939, 123631, 124337, 125029, 125731, 126433, 127133, 127837, 128549, 
129263, 129967, 130681, 131413, 132137, 132851, 133571, 134287, 135007, 135727, 136447, 
137177, 137909, 138637, 139367, 140111, 140863, 141601, 142357, 143107, 143873, 144629, 
145381, 146141, 146891, 147647, 148399, 149153, 149909, 150697, 151471, 152239, 153059, 
153841, 154613, 155383, 156157, 156941, 157721, 158507, 159287, 160073, 160861, 161659, 
162451, 163243, 164039, 164837, 165653, 166457, 167261, 168067, 168887, 169693, 170503, 
171317, 172147, 172969, 173807, 174631, 175453, 176299, 177127, 177953, 178781, 179623, 
180463, 181297, 182141, 182981, 183823, 184669, 185519, 186377, 187237, 188107, 188983, 
189851, 190709, 191579, 192461, 193327, 194197, 195071, 195967, 196837, 197711, 198589, 
199483, 200363, 201247, 202129, 203017, 203909, 204797, 205703, 206597, 207491, 208387, 
209299, 210209, 211129, 212039, 212969, 213881, 214807, 215723, 216641, 217559, 218479, 
219407, 220333, 221261, 222193, 223129, 224069, 225023, 225961, 226901, 227849, 228793, 
229739, 230683, 231631, 232591, 233549, 234511, 235483, 236449, 237409, 238369, 239333, 
240319, 241291, 242261, 243233, 244217, 245209, 246187, 247183, 248167, 249181, 250169, 
251159, 252151, 253153, 254147, 255149, 256147, 257161, 258161, 259163, 260171, 261223, 
262231, 263257, 264269, 265313, 266333, 267353, 268403, 269429, 270461, 271489, 272533, 
273569, 274609, 275651, 276707, 277747, 278801, 279847, 280897, 281947, 283001, 284051, 
285113, 286171, 287233, 288293, 289361, 290429, 291503, 292573, 293651, 294731, 295819, 
296909, 297989, 299087, 300187, 301303, 302399, 303491, 304597, 305717, 306809, 307903, 
308999, 310111, 311237, 312343, 313471, 314581, 315691, 316801, 317921, 319037, 320153, 
321289, 322409, 323537, 324661, 325807, 326939, 328093, 329233, 330383, 331519, 332687, 
333857, 335009, 336157, 337313, 338461, 339613, 340777, 341947, 343127, 344291, 345451, 
346627, 347801, 348989, 350159, 351341, 352523, 353699, 354877, 356077, 357263, 358447, 
359633, 360821, 362027, 363217, 364411, 365611, 366811, 368021, 369247, 370451, 371663, 
372871, 374083, 375311, 376529, 377749, 378967, 380189, 381413, 382643, 383869, 385109, 
386339, 387577, 388813, 390067, 391331, 392569, 393847, 395089, 396349, 397597, 398857, 
400109, 401371, 402631, 403889, 405157, 406423, 407699, 408971, 410239, 411527, 412807, 
414083, 415379, 416659, 417941, 419231, 420521, 421807, 423097, 424397, 425701, 426997, 
428297, 429599, 430897, 432199, 433501, 434807, 436127, 437467, 438793, 440131, 441449, 
442769, 444089, 445427, 446753, 448093, 449419, 450761, 452131, 453527, 454889, 456233, 
457571, 458917, 460267, 461627, 462983, 464351, 465701, 467063, 468421, 469787, 471161, 
472523, 473887, 475271, 476639, 478039, 479419, 480803, 482179, 483557, 484951, 486331, 
487717, 489101, 490493, 491899, 493291, 494687, 496123, 497521, 498923, 500333, 501769, 
503197, 504607, 506047, 507461, 508901, 510319, 511757, 513203, 514637, 516077, 517501, 
518933, 520361, 521791, 523261, 524701, 526139, 527581, 529027, 530501, 531977, 533447, 
534913, 536377, 537841, 539303, 540769, 542237, 543703, 545189, 546661, 548143, 549623, 
551093, 552581, 554077, 555557, 557041, 558521, 560017, 561521, 563009, 564497, 565997, 
567493, 568987, 570487, 572023, 573523, 575027, 576533, 578041, 579563, 581089, 582601, 
584141, 585671, 587189, 588733, 590263, 591791, 593321, 594857, 596399, 597967, 599513, 
601061, 602603, 604171, 605719, 607301, 608851, 610409, 611969, 613523, 615101, 616669, 
618227, 619793, 621359, 622927, 624497, 626113, 627709, 629281, 630863, 632447, 634031, 
635617, 637199, 638801, 640411, 642011, 643619, 645233, 646831, 648433, 650059, 651667, 
653273, 654877, 656483, 658111, 659723, 661343, 662957, 664579, 666203, 667829, 669451, 
671081, 672743, 674371, 676007, 677639, 679277, 680917, 682597, 684239, 685907, 687551, 
689201, 690869, 692521, 694189, 695843, 697507, 699169, 700831, 702497, 704161, 705827, 
707501, 709201, 710873, 712561, 714247, 715927, 717631, 719333, 721037, 722723, 724433, 
726137, 727843, 729551, 731249, 732959, 734659, 736361, 738071, 739777, 741491, 743203, 
744917, 746653, 748379, 750097, 751823, 753547, 755273, 757019, 758753, 760489, 762227, 
763967, 765707, 767471, 769231, 770981, 772757, 774511, 776267, 778027, 779791, 781559, 
783317, 785093, 786859, 788621, 790397, 792163, 793931, 795703, 797497, 799291, 801077, 
802873, 804653, 806447, 808237, 810023, 811819, 813613, 815411, 817211, 819017, 820837, 
822667, 824477, 826283, 828101, 829949, 831769, 833593, 835421, 837257, 839087, 840907, 
842729, 844553, 846383, 848213, 850043, 851881, 853717, 855581, 857419, 859259, 861109, 
862957, 864803, 866653, 868529, 870391, 872251, 874109, 875969, 877837, 879701, 881591, 
883471, 885359, 887233, 889123, 890999, 892877, 894763, 896647, 898543, 900443, 902333, 
904261, 906179, 908071, 909971, 911873, 913771, 915683, 917591, 919511, 921457, 923369, 
925279, 927191, 929113, 931067, 932999, 934919, 936869, 938803, 940733, 942661, 944591, 
946549, 948487, 950423, 952363, 954307, 956261, 958213, 960173, 962131, 964081, 966041, 
967999, 969977, 971939, 973901, 975869, 977849, 979819, 981797, 983771, 985759, 987739, 
989719, 991703, 993683, 995669, 997663, 999653, 1001659, 1003679, 1005677, 1007681, 1009727, 
1011733, 1013741, 1015747, 1017781, 1019801, 1021831, 1023851, 1025873, 1027891, 1029907, 1031981, 
1034003, 1036027, 1038073, 1040101, 1042133, 1044167, 1046203, 1048261, 1050307, 1052413, 1054457, 
1056509, 1058567, 1060621, 1062671, 1064731, 1066789, 1068857, 1070921, 1072991, 1075069, 1077143, 
1079213, 1081279, 1083349, 1085419, 1087517, 1089611, 1091687, 1093777, 1095859, 1097947, 1100039, 
1102147, 1104241, 1106363, 1108463, 1110583, 1112689, 1114801, 1116911, 1119029, 1121143, 1123267, 
1125379, 1127507, 1129619, 1131737, 1133857, 1135997, 1138117, 1140239, 1142363, 1144499, 1146661, 
1148837, 1150973, 1153109, 1155247, 1157393, 1159541, 1161683, 1163831, 1165991, 1168151, 1170311, 
1172467, 1174627, 1176787, 1178953, 1181137, 1183333, 1185497, 1187687, 1189871, 1192069, 1194241, 
1196431, 1198607, 1200799, 1202987, 1205173, 1207363, 1209557, 1211761, 1213951, 1216147, 1218367, 
1220591, 1222789, 1224991, 1227241, 1229447, 1231663, 1233887, 1236161, 1238381, 1240607, 1242823, 
1245067, 1247291, 1249519, 1251743, 1253969, 1256197, 1258429, 1260661, 1262897, 1265167, 1267411, 
1269683, 1271927, 1274183, 1276433, 1278701, 1280969, 1283237, 1285507, 1287787, 1290049, 1292309, 
1294571, 1296839, 1299143, 1301413, 1303693, 1305971, 1308247, 1310527, 1312813, 1315151, 1317443, 
1319729, 1322021, 1324313, 1326607, 1328909, 1331207, 1333511, 1335847, 1338167, 1340477, 1342799, 
1345117, 1347433, 1349753, 1352069, 1354391, 1356709, 1359053, 1361383, 1363717, 1366087, 1368439, 
1370773, 1373129, 1375481, 1377821, 1380157, 1382501, 1384843, 1387189, 1389533, 1391893, 1394251, 
1396607, 1398967, 1401349, 1403747, 1406159, 1408523, 1410887, 1413253, 1415629, 1418009, 1420399, 
1422797, 1425187, 1427563, 1429943, 1432351, 1434737, 1437133, 1439521, 1441931, 1444411, 1446803, 
1449209, 1451609, 1454021, 1456439, 1458841, 1461283, 1463719, 1466137, 1468547, 1470971, 1473389, 
1475813, 1478231, 1480663, 1483087, 1485541, 1487977, 1490429, 1492859, 1495297, 1497731, 1500181, 
1502621, 1505083, 1507531, 1509997, 1512479, 1514959, 1517413, 1519871, 1522331, 1524799, 1527271, 
1529741, 1532231, 1534727, 1537199, 1539679, 1542179, 1544651, 1547129, 1549609, 1552087, 1554569, 
1557053, 1559549, 1562051, 1564543, 1567037, 1569541, 1572047, 1574543, 1577071, 1579579, 1582081, 
1584607, 1587121, 1589633, 1592159, 1594693, 1597229, 1599803, 1602323, 1604857, 1607399, 1609969, 
1612517, 1615049, 1617589, 1620121, 1622659, 1625201, 1627739, 1630303, 1632853, 1635401, 1637963, 
1640519, 1643069, 1645643, 1648217, 1650793, 1653383, 1655947, 1658509, 1661111, 1663681, 1666261, 
1668833, 1671421, 1674011, 1676593, 1679189, 1681787, 1684373, 1686967, 1689553, 1692149, 1694761, 
1697357, 1699969, 1702573, 1705181, 1707787, 1710389, 1713007, 1715617, 1718251, 1720867, 1723481, 
1726103, 1728733, 1731361, 1733981, 1736617, 1739251, 1741877, 1744507, 1747153, 1749833, 1752467, 
1755113, 1757771, 1760419, 1763081, 1765741, 1768399, 1771051, 1773703, 1776389, 1779053, 1781743, 
1784401, 1787087, 1789751, 1792423, 1795091, 1797769, 1800451, 1803127, 1805819, 1808497, 1811179, 
1813897, 1816583, 1819271, 1821959, 1824649, 1827341, 1830047, 1832791, 1835501, 1838203, 1840921, 
1843631, 1846357, 1849063, 1851779, 1854491, 1857203, 1859917, 1862633, 1865371, 1868107, 1870829, 
1873567, 1876309, 1879049, 1881787, 1884523, 1887283, 1890019, 1892771, 1895513, 1898257, 1901021, 
1903787, 1906537, 1909307, 1912061, 1914817, 1917581, 1920343, 1923107, 1925873, 1928653, 1931453, 
1934263, 1937041, 1939837, 1942627, 1945403, 1948181, 1950979, 1953761, 1956553, 1959361, 1962161, 
1964951, 1967753, 1970567, 1973369, 1976167, 1978981, 1981787, 1984639, 1987451, 1990273, 1993087, 
1995913, 1998727, 2001547, 2004377, 2007199, 2010023, 2012849, 2015677, 2018507, 2021339, 2024177, 
2027021, 2029871, 2032711, 2035567, 2038411, 2041283, 2044129, 2046983, 2049847, 2052709, 2055569, 
2058439, 2061313, 2064187, 2067061, 2069929, 2072801, 2075669, 2078551, 2081423, 2084297, 2087179, 
2090069, 2092963, 2095853, 2098739, 2101651, 2104541, 2107447, 2110343, 2113249, 2116183, 2119087, 
2121989, 2124919, 2127841, 2130757, 2133673, 2136583, 2139497, 2142431, 2145359, 2148287, 2151211, 
2154137, 2157091, 2160017, 2162947, 2165893, 2168827, 2171761, 2174699, 2177653, 2180603, 2183557, 
2186519, 2189477, 2192431, 2195381, 2198347, 2201317, 2204273, 2207237, 2210209, 2213171, 2216143, 
2219111, 2222089, 2225059, 2228053, 2231027, 2234009, 2236987, 2239987, 2242973, 2245961, 2248951, 
2251943, 2254933, 2257939, 2260961, 2263957, 2266961, 2269961, 2272973, 2275993, 2279009, 2282017, 
2285039, 2288051, 2291119, 2294137, 2297203, 2300239, 2303263, 2306299, 2309327, 2312363, 2315399, 
2318453, 2321507, 2324551, 2327597, 2330641, 2333689, 2336743, 2339797, 2342869, 2345921, 2348987, 
2352041, 2355097, 2358157, 2361221, 2364287, 2367361, 2370427, 2373529, 2376599, 2379673, 2382749, 
2385827, 2388907, 2391997, 2395103, 2398189, 2401279, 2404387, 2407499, 2410601, 2413721, 2416837, 
2419939, 2423039, 2426143, 2429257, 2432363, 2435473, 2438587, 2441717, 2444833, 2447953, 2451079, 
2454209, 2457337, 2460467, 2463619, 2466749, 2469889, 2473027, 2476163, 2479307, 2482451, 2485607, 
2488757, 2491903, 2495071, 2498219, 2501383, 2504543, 2507707, 2510867, 2514037, 2517197, 2520367, 
2523533, 2526709, 2529881, 2533081, 2536267, 2539441, 2542619, 2545811, 2549003, 2552191, 2555429, 
2558639, 2561833, 2565023, 2568233, 2571427, 2574623, 2577821, 2581027, 2584229, 2587441, 2590649, 
2593859, 2597081, 2600309, 2603537, 2606753, 2609989, 2613211, 2616437, 2619667, 2622911, 2626171, 
2629409, 2632657, 2635891, 2639149, 2642389, 2645639, 2648897, 2652149, 2655397, 2658659, 2661917, 
2665177, 2668433, 2671693, 2674957, 2678219, 2681507, 2684771, 2688053, 2691329, 2694607, 2697887, 
2701177, 2704469, 2707751, 2711039, 2714353, 2717639, 2720953, 2724251, 2727559, 2730869, 2734177, 
2737477, 2740799, 2744099, 2747401, 2750707, 2754041, 2757361, 2760671, 2763989, 2767319, 2770639, 
2773997, 2777317, 2780641, 2783999, 2787329, 2790673, 2794003, 2797337, 2800703, 2804041, 2807381, 
2810737, 2814083, 2817433, 2820781, 2824139, 2827493, 2830853, 2834213, 2837581, 2840939, 2844311, 
2847679, 2851049, 2854433, 2857801, 2861189, 2864569, 2867947, 2871347, 2874727, 2878123, 2881513, 
2884897, 2888287, 2891687, 2895077, 2898479, 2901893, 2905303, 2908721, 2912131, 2915533, 2918939, 
2922349, 2925773, 2929183, 2932597, 2936023, 2939449, 2942881, 2946319, 2949763, 2953199, 2956631, 
2960071, 2963501, 2966963, 2970413, 2973857, 2977313, 2980753, 2984203, 2987651, 2991103, 2994581, 
2998031, 3001489, 3004943, 3008417, 3011881, 3015343, 3018881, 3022363, 3025837, 3029309, 3032789, 
3036269, 3039761, 3043237, 3046717, 3050197, 3053689, 3057227, 3060749, 3064241, 3067733, 3071231, 
3074741, 3078259, 3081761, 3085273, 3088783, 3092291, 3095797, 3099307, 3102817, 3106339, 3109859, 
3113387, 3116947, 3120473, 3123997, 3127529, 3131057, 3134591, 3138127, 3141659, 3145201, 3148741, 
3152321, 3155923, 3159469, 3163051, 3166613, 3170179, 3173761, 3177329, 3180893, 3184469, 3188033, 
3191593, 3195161, 3198731, 3202301, 3205871, 3209441, 3213029, 3216611, 3220193, 3223771, 3227353, 
3230957, 3234551, 3238153, 3241753, 3245357, 3248977, 3252577, 3256181, 3259783, 3263389, 3266999, 
3270607, 3274231, 3277843, 3281461, 3285083, 3288707, 3292327, 3295967, 3299617, 3303259, 3306883, 
3310547, 3314203, 3317849, 3321491, 3325159, 3328799, 3332437, 3336089, 3339751, 3343397, 3347053, 
3350719, 3354371, 3358031, 3361739, 3365399, 3369059, 3372727, 3376397, 3380089, 3383773, 3387443, 
3391117, 3394799, 3398477, 3402169, 3405881, 3409573, 3413257, 3416951, 3420647, 3424363, 3428057, 
3431749, 3435457, 3439153, 3442867, 3446567, 3450281, 3453997, 3457703, 3461417, 3465139, 3468851, 
3472583, 3476299, 3480017, 3483761, 3487489, 3491227, 3494969, 3498697, 3502427, 3506159, 3509903, 
3513649, 3517387, 3521131, 3524891, 3528641, 3532387, 3536161, 3539917, 3543677, 3547459, 3551221, 
3554981, 3558749, 3562511, 3566293, 3570067, 3573839, 3577631, 3581419, 3585209, 3589031, 3592819, 
3596599, 3600383, 3604177, 3607963, 3611761, 3615559, 3619393, 3623197, 3627023, 3630821, 3634627, 
3638473, 3642299, 3646117, 3649931, 3653761, 3657607, 3661421, 3665237, 3669073, 3672899, 3676721, 
3680549, 3684389, 3688219, 3692053, 3695911, 3699767, 3703631, 3707471, 3711311, 3715169, 3719017, 
3722867, 3726721, 3730579, 3734443, 3738311, 3742201, 3746077, 3749939, 3753817, 3757703, 3761591, 
3765493, 3769379, 3773257, 3777131, 3781007, 3784901, 3788833, 3792721, 3796609, 3800501, 3804397, 
3808307, 3812201, 3816103, 3820009, 3823927, 3827833, 3831743, 3835651, 3839573, 3843533, 3847451, 
3851363, 3855287, 3859213, 3863173, 3867107, 3871039, 3874979, 3878921, 3882871, 3886801, 3890737, 
3894673, 3898619, 3902557, 3906541, 3910507, 3914453, 3918401, 3922349, 3926311, 3930271, 3934261, 
3938251, 3942209, 3946177, 3950147, 3954127, 3958103, 3962081, 3966059, 3970033, 3974023, 3977999, 
3982031, 3986033, 3990029, 3994021, 3998041, 4002041, 4006039, 4010047, 4014047, 4018043, 4022041, 
4026053, 4030063, 4034071, 4038079, 4042091, 4046101, 4050121, 4054139, 4058167, 4062197, 4066219, 
4070243, 4074277, 4078339, 4082389, 4086421, 4090507, 4094543, 4098583, 4102649, 4106699, 4110751, 
4114819, 4118893, 4122941, 4127003, 4131059, 4135123, 4139189, 4143253, 4147313, 4151377, 4155467, 
4159541, 4163611, 4167697, 4171771, 4175867, 4179949, 4184027, 4188127, 4192219, 4196303, 4200397, 
4204489, 4208579, 4212679, 4216787, 4220893, 4224991, 4229101, 4233247, 4237397, 4241507, 4245617, 
4249741, 4253863, 4257977, 4262119, 4266253, 4270391, 4274521, 4278649, 4282777, 4286923, 4291057, 
4295209, 4299391, 4303529, 4307669, 4311809, 4315981, 4320187, 4324337, 4328497, 4332649, 4336837, 
4340993, 4345153, 4349311, 4353493, 4357673, 4361837, 4366007, 4370203, 4374373, 4378553, 4382731, 
4386919, 4391099, 4395283, 4399471, 4403657, 4407857, 4412053, 4416257, 4420453, 4424653, 4428859, 
4433057, 4437259, 4441477, 4445681, 4449899, 4454141, 4458359, 4462571, 4466789, 4471007, 4475239, 
4479463, 4483709, 4487939, 4492171, 4496411, 4500649, 4504883, 4509119, 4513373, 4517651, 4521893, 
4526143, 4530401, 4534667, 4538917, 4543207, 4547467, 4551737, 4555997, 4560263, 4564523, 4568803, 
4573069, 4577371, 4581659, 4585939, 4590217, 4594493, 4598771, 4603051, 4607333, 4611631, 4615927, 
4620223, 4624517, 4628807, 4633141, 4637449, 4641773, 4646071, 4650389, 4654697, 4659013, 4663327, 
4667647, 4671973, 4676297, 4680623, 4684949, 4689283, 4693609, 4697947, 4702277, 4706633, 4710961, 
4715311, 4719643, 4723981, 4728343, 4732703, 4737053, 4741397, 4745743, 4750091, 4754443, 4758799, 
4763159, 4767527, 4771889, 4776251, 4780637, 4785031, 4789399, 4793771, 4798163, 4802533, 4806911, 
4811299, 4815677, 4820069, 4824473, 4828871, 4833271, 4837661, 4842067, 4846469, 4850887, 4855297, 
4859713, 4864121, 4868543, 4872961, 4877387, 4881809, 4886227, 4890637, 4895057, 4899481, 4903901, 
4908329, 4912751, 4917173, 4921597, 4926043, 4930483, 4934929, 4939369, 4943831, 4948267, 4952713, 
4957153, 4961599, 4966051, 4970503, 4974953, 4979411, 4983877, 4988339, 4992809, 4997273
    
    
//     3001, 4799, 13999, 15377, 21803, 24247, 35083, 40531, 43669, 44263, 47387,
//     50377, 57059, 57773, 59399, 59999, 75913, 96821, 140551, 149011, 175633,
//     176389, 183299, 205507, 209771, 223099, 240259, 258551, 263909, 270761,
//     274679, 286129, 290531, 296269, 298021, 300961, 306407, 327493, 338851,
//     351037, 365489, 366811, 376769, 385069, 410623, 430709, 433729, 434509,
//     441913, 458531, 464351, 470531, 475207, 479629, 501703, 510709, 516017,
//     522211, 528527, 536311, 539723, 557567, 593587, 596209, 597451, 608897,
//     611069, 642547, 670511, 677827, 679051, 688477, 696743, 717683, 745931,
//     757109, 760813, 763957, 766261, 781559, 785597, 788353, 804493, 813559,
//     836917, 854257, 859973, 883217, 884789, 891493, 902281, 910199, 915199,
//     930847, 939749, 940483, 958609, 963847, 974887, 983849, 984299, 996211,
//     999217, 1007519, 1013329, 1014287, 1032959, 1035829, 1043593, 1046459,
//     1076171, 1078109, 1081027, 1090303, 1095613, 1098847, 1114037, 1124429,
//     1125017, 1130191, 1159393, 1170311, 1180631, 1198609, 1200809, 1212943,
//     1213087, 1226581, 1232851, 1287109, 1289867, 1297123, 1304987, 1318661,
//     1331107, 1343161, 1345471, 1377793, 1385117, 1394681, 1410803, 1411987,
//     1445261, 1460497, 1463981, 1464391, 1481173, 1488943, 1491547, 1492807,
//     1528993, 1539961, 1545001, 1548247, 1549843, 1551001, 1553023, 1571417,
//     1579099, 1600259, 1606153, 1606541, 1639751, 1649587, 1657661, 1662653,
//     1667051, 1675273, 1678837, 1715537, 1718489, 1726343, 1746281, 1749107,
//     1775489, 1781881, 1800157, 1806859, 1809149, 1826753, 1834607, 1846561,
//     1849241, 1851991, 1855033, 1879931, 1891133, 1893737, 1899137, 1909513,
//     1916599, 1917749, 1918549, 1919347, 1925557, 1946489, 1961551, 1965389,
//     2011073, 2033077, 2039761, 2054047, 2060171, 2082503, 2084107, 2095099,
//     2096011, 2112193, 2125601, 2144977, 2150831, 2157401, 2170141, 2221829,
//     2233019, 2269027, 2270771, 2292449, 2299397, 2303867, 2309891, 2312407,
//     2344301, 2348573, 2377007, 2385113, 2386661, 2390051, 2395763, 2422999,
//     2448367, 2500529, 2508203, 2509841, 2513677, 2516197, 2518151, 2518177,
//     2542091, 2547469, 2549951, 2556991, 2563601, 2575543, 2597629, 2599577,
//     2612249, 2620003, 2626363, 2626781, 2636773, 2661557, 2674297, 2691571,
//     2718269, 2725691, 2729381, 2772199, 2774953, 2791363, 2792939, 2804293,
//     2843021, 2844911, 2851313, 2863519, 2880797, 2891821, 2897731, 2904887,
//     2910251, 2928943, 2958341, 2975389
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
