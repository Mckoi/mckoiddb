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

package com.mckoi.util;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;

/**
 * A cache that puts both the key and value behind a Java references, thus
 * making data stored eligible for reclamation by the Garbage Collector.
 *
 * @author Tobias Downer
 */

public class ReferenceCache<K, V> {

  /**
   * The entry array.
   */
  private final HashEntry<K, V>[] hash_map;
  
  /**
   * The reference type.
   */
  private final int ref_type;

  /**
   * Different reference types.
   */
  public final static int WEAK   = 1;
  public final static int SOFT   = 2;

  /**
   * Constructor. The size of the cache must be a prime number in size (use
   * Cache.closestPrime to find a good prime number).
   */
  public ReferenceCache(int size, int ref_type) {
    this.hash_map = new HashEntry[size];
    this.ref_type = ref_type;
  }

  /**
   * Creates a HashEntry element.
   */
  protected HashEntry<K, V> createEntry() {
    return new HashEntry();
  }

  protected Reference createReference(KeyValue kv) {
    if (ref_type == WEAK) {
      return new WeakReference(kv);
    }
    else if (ref_type == SOFT) {
      return new SoftReference(kv);
    }
    else {
      throw new RuntimeException("Invalid reference type");
    }
  }

  /**
   * Puts a value in the cache. If the key already exists then it is
   * overwritten. If value is null then the key/value entry is removed.
   */
  public V put(K key, V value) {
    // The hash position,
    final int hash_index = Math.abs(key.hashCode()) % hash_map.length;
    // Get the entry from the map,
    HashEntry<K, V> entry = hash_map[hash_index];
    HashEntry<K, V> previous = null;
    HashEntry<K, V> first_empty = null;
    while (entry != null) {
      KeyValue<K, V> kv = entry.getKeyValue();
      HashEntry<K, V> next = entry.next;
      // If it's empty then maybe use this,
      if (kv == null) {
        if (first_empty == null) {
          first_empty = entry;
        }
      }
      else {
        if (kv.key.equals(key)) {
          // Ok, found what we are looking for,
          V old_value = kv.value;
          if (value != null) {
            // Assign this entry a new value,
            kv.value = value;
          }
          else {
            // Remove from the entry list,
            if (previous == null) {
              hash_map[hash_index] = next;
            }
            else {
              previous.next = next;
            }
          }
          // And return,
          return old_value;
        }
      }
      // The next entry,
      previous = entry;
      entry = next;
    }

    // Recycle,
    if (first_empty != null) {
      first_empty.assign(key, value);
    }
    else {
      // Add a new item to the map,
      entry = createEntry();
      entry.assign(key, value);
      entry.next = hash_map[hash_index];
      hash_map[hash_index] = entry;
    }

    return null;

  }

  /**
   * Gets a value from the cache. If the key is not present in the map then
   * returns null.
   */
  public V get(K key) {

    // The hash position,
    int hash_index = Math.abs(key.hashCode()) % hash_map.length;
    // Get the entry from the map,
    HashEntry<K, V> entry = hash_map[hash_index];
    HashEntry<K, V> previous = null;
    while (entry != null) {
      KeyValue<K, V> kv = entry.getKeyValue();
      HashEntry<K, V> next = entry.next;
      // If it's null then we remove this entry,
      if (kv == null) {
        if (previous == null) {
          hash_map[hash_index] = next;
        }
        else {
          previous.next = next;
        }
      }
      else {
        // Is this what we are looking for?
        if (kv.key.equals(key)) {
          // Yes, so return it,
          return kv.value;
        }
        // Otherwise keep looking,
        previous = entry;
      }
      // The next entry,
      entry = next;
    }
    return null;

  }

  
  

  // ----------

  protected class HashEntry<K, V> {

    private Reference<KeyValue<K, V>> reference = null;
    private HashEntry<K, V> next = null;

    public HashEntry() {
    }

    private Reference<KeyValue<K, V>> createReference(KeyValue kv) {
      // Annoying generics cast here, can we get rid of it?
      return (Reference<KeyValue<K, V>>)
                                      ReferenceCache.this.createReference(kv);
    }

    /**
     * Assign a key value to this entry,
     */
    public void assign(K key, V value) {
      KeyValue<K, V> kv = new KeyValue(key, value);
      reference = createReference(kv);
    }

    /**
     * Returns the key value from this entry. Note that if this succeeds then
     * it will make it not illegible for GC while a strong reference to it is
     * being held.
     */
    public KeyValue<K, V> getKeyValue() {
      return reference.get();
    }

  }

  protected static class KeyValue<K, V> {

    private final K key;
    private V value;

    KeyValue(K key, V value) {
      this.key = key;
      this.value = value;
    }

  }

//  public static void main(String[] args) {
//    int cache_size = Cache.closestPrime(15000);
//    ReferenceCache<String, String> cache = new ReferenceCache(cache_size, WEAK);
//    Map<String, String> java_map = new HashMap();
//    
//    for (int i = 0; i <= 10000; ++i) {
//      cache.put("zonk" + i, "rawr " + i);
//      java_map.put("zonk" + i, "rawr " + i);
//    }
//    //System.gc();
//    for (int i = 100; i >= 0; --i) {
//      System.out.println(cache.get("zonk" + i));
//      if (i % 3 == 0) {
//        cache.put("zonk" + i, null);
//        java_map.put("zonk" + i, null);
//      }
//    }
//    for (int i = 0; i <= 100; ++i) {
//      System.out.println(cache.get("zonk" + i));
//    }
//    StringBuilder b = new StringBuilder();
//    for (int i = 0; i < 10000; ++i) {
//      Integer i1 = new Integer(i + 40);
//      b.append(i1);
//      b.append(" -- ");
//      
//    }
//    System.gc();
//    for (int i = 0; i <= 10000; ++i) {
//      String v = cache.get("zonk" + i);
//      String check_v = java_map.get("zonk" + i);
//      if (v != null && !v.equals(check_v)) {
//        throw new RuntimeException("Map error!");
//      }
//      System.out.print(v);
//    }
//  }
  
}
