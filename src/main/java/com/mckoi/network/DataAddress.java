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

package com.mckoi.network;

import com.mckoi.data.NodeReference;

/**
 * Represents the address of some data stored in the network data address
 * space, encoded as an 124-bit value.
 * <p>
 * A DataAddress is a block_id and data_id component which references a string
 * of data in the network data address space. Internally, these components are
 * encoded as an 124-bit value. The data_id part is encoded in the lower 16
 * bits of the value limiting the number of individual items stored in a block
 * to 65536 elements. The remaining 108 bits of the value represent the block
 * id, which has a limit of 3.24E+32 blocks.
 * <p>
 * Typically a block will be represented as a numbered file in the block
 * directory of the storage machine.
 * <p>
 * Note that the address space is very large. The reason for the large address
 * space is to provide flexibility in how regions of the address space are
 * allocated. It is not expected for the available address space to be used
 * compactly. In practice, the amount of data stored in the address space
 * is expected to be many factors smaller than the logical bounds.
 *
 * @author Tobias Downer
 */

public final class DataAddress implements Comparable<DataAddress> {

  /**
   * The address value as a NodeReference.
   */
  private final NodeReference address_value;

  /**
   * Constructs the address from a NodeReference object.
   */
  DataAddress(NodeReference address_value) {
    this.address_value = address_value;
  }

  /**
   * Constructs the address as a block_id/data_id pair.
   */
  DataAddress(BlockId block_id, int data_id) {
    // PENDING: Check for overflow?
    final long[] block_addr = block_id.getReferenceAddress();
    block_addr[1] |= data_id & 0x0FFFF;
    address_value = new NodeReference(block_addr);
  }
  
  /**
   * Returns the address value as a NodeReference.
   */
  public NodeReference getValue() {
    return address_value;
  }

  /**
   * Returns the block id part of the address value.
   */
  public BlockId getBlockId() {
    return getBlockIdFrom(address_value);
  }

  /**
   * Returns the data id part of the address value.
   */
  public int getDataId() {
    return ((int) address_value.getLowLong()) & 0x0FFFF;
  }

  /**
   * Returns this object if this data address is greater than or equal
   * to the given address, otherwise returns the given address.
   */
  public DataAddress max(DataAddress address) {
    if (getValue().compareTo(address.getValue()) >= 0) {
      return this;
    }
    else {
      return address;
    }
  }

  /**
   * Formats this object as a string (which can be parsed by the parse
   * method).
   */
  public String formatString() {
    return getValue().formatString();
  }

  /**
   * Returns the DataAddress object parsed from the given string.
   */
  public static DataAddress parseString(String str) {

    // Security check,
    SecurityManager security = System.getSecurityManager();
    if (security != null) {
      security.checkPermission(MckoiNetworkPermission.PARSE_DATA_ADDRESS);
    }

    // PENDING SECURITY: This needs to be a protected function that is not
    //   available to user functions. We do not want to allow users to be able
    //   to arbitrarily create DataAddress objects because it could be used to
    //   inspect the contents of a database outside the snapshot sandbox.
    //
    //   This function does need to be available to be used by low level
    //   operations such as a consensus function.

    return new DataAddress(NodeReference.parseString(str));
  }

  /**
   * External method for creating a DataAddress from a NodeReference. Requires
   * a security check.
   */
  public static DataAddress createDataAddress(
                                  NodeReference node_reference) {

    // Security check,
    SecurityManager security = System.getSecurityManager();
    if (security != null) {
      security.checkPermission(MckoiNetworkPermission.CREATE_DATA_ADDRESS);
    }

    return new DataAddress(node_reference);

  }

  /**
   * Returns the BlockId for the given NodeReference.
   */
  public static BlockId getBlockIdFrom(NodeReference node) {
    long addr_low = node.getLowLong();
    long addr_high = node.getHighLong();
    addr_low = (addr_low >> 16) & 0x0FFFFFFFFFFFFL;
    addr_low |= (addr_high & 0x0FF) << 48;
    addr_high = addr_high >> 16;
    return new BlockId(addr_high, addr_low);
  }


  @Override
  public String toString() {
    return getValue().toString();
  }

  @Override
  public int hashCode() {
    return getValue().hashCode();
  }

  @Override
  public boolean equals(Object ob) {
    if (this == ob) {
      return true;
    }
    if (!(ob instanceof DataAddress)) {
      return false;
    }
    return getValue().equals(((DataAddress) ob).getValue());
  }

  @Override
  public int compareTo(DataAddress o) {
    return getValue().compareTo(o.getValue());
  }

}
