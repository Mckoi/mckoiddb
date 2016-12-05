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

import java.math.BigInteger;

/**
 * An atomic data element that may be safely accessed and modified across
 * transactions with consistent characteristics.  Used to implement sequences.
 * <p>
 * All atomic data elements are 16 bytes in size (128 bits).
 *
 * @author Tobias Downer
 */

public interface AtomicData {

  /**
   * Returns the key identifier for this data element.
   */
  AtomicKey getKey();

  /**
   * Sets this element to the given value.
   */
  void setValue(byte[] buf);

  /**
   * Gets this element (copies it to the given byte[] array).
   */
  void getValue(byte[] buf);

  /**
   * Returns this data element as a BigInteger value.
   */
  BigInteger toBigInteger();

  /**
   * Sets this data element as a BigInteger value.
   */
  void setValue(BigInteger bi);

  /**
   * Adds a quantity to the atomic value (as represented by a BigInteger) and
   * returns the new value as a BigInteger.  The add and fetch is atomic,
   * meaning there is an implied lock when changing the value and fetching the
   * next value.  This method can be used to implement sequence generators.
   */
  BigInteger addThenFetch(long add_amount);

  /**
   * Adds a quantity to the atomic value (as represented by a BigInteger) and
   * returns the value as a BigInteger as it was before the quantity was added.
   * The fetch and add operation is atomic.
   */
  BigInteger fetchThenAdd(long add_amount);

}
