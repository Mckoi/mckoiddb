/**
 * com.mckoi.treestore.AtomicData  Dec 27, 2007
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

package com.mckoi.data;

import java.math.BigInteger;

/**
 * An atomic data element that may be safely accessed and modified across
 * transactions with consistant characteristics.  Used to implement sequences.
 * <p>
 * All atomic data elements are 16 bytes in size (128 bits).
 *
 * @author Tobias Downer
 */

public interface AtomicData {

  /**
   * Returns the key identifier for this data element.
   */
  public AtomicKey getKey();
  
  /**
   * Sets this element to the given value.
   */
  public void setValue(byte[] buf);

  /**
   * Gets this element (copies it to the given byte[] array).
   */
  public void getValue(byte[] buf);
  
  /**
   * Returns this data element as a BigInteger value.
   */
  public BigInteger toBigInteger();
  
  /**
   * Sets this data element as a BigInteger value.
   */
  public void setValue(BigInteger bi);

  /**
   * Adds a quantity to the atomic value (as represented by a BigInteger) and
   * returns the new value as a BigInteger.  The add and fetch is atomic,
   * meaning there is an implied lock when changing the value and fetching the
   * next value.  This method can be used to implement sequence generators.
   */
  public BigInteger addThenFetch(long add_amount);

  /**
   * Adds a quantity to the atomic value (as represented by a BigInteger) and
   * returns the value as a BigInteger as it was before the quantity was added.
   * The fetch and add operation is atomic.
   */
  public BigInteger fetchThenAdd(long add_amount);
  
  
//  /**
//   * Writes the serialization of this object out to the given DataFile object.
//   */
//  public void writeTo(DataFile data) throws java.io.IOException;

}
