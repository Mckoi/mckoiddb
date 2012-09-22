/**
 * com.mckoi.data.NodeReference  Jun 11, 2010
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

import com.mckoi.store.AreaWriter;
import java.io.IOException;

/**
 * A reference to a node object in the address space of all nodes. A reference
 * value is an 128-bit value. The top 4 bits of a reference value are reserved
 * for special codes (such as in-memory references, etc).
 *
 * @author Tobias Downer
 */

public final class NodeReference extends Integer128Bit {

  /**
   * Constructor.
   */
  public NodeReference(long[] ref) {
    super(ref);
  }

  /**
   * Constructor.
   */
  public NodeReference(long high, long low) {
    super(high, low);
  }

  /**
   * Returns the 4-bit encoded top part of the reference.
   */
  public int getReservedBits() {
    return ((int) (ref[0] >> 60)) & 0x0F;
  }

  /**
   * Returns true if the referenced node is held in memory.
   */
  public boolean isInMemory() {
    return getReservedBits() == 1;
  }

  /**
   * Returns true if the reference is a special encoded node.
   */
  public boolean isSpecial() {
    int res_bits = getReservedBits();
    return (res_bits >= 2 && res_bits < 8);
  }

  /**
   * Returns a TreeNode representing the special information represented by
   * this node reference (for example, a sparse node).
   */
  public TreeNode createSpecialTreeNode() {
    long c = ref[0] & 0x0F000000000000000L;
    // If it's a sparce special node,
    if (c == SPARSE_HIGH_LONG) {
      // Create the sparse node
      byte b = (byte) (ref[0] & 0x0FF);
      long sparse_size = ref[1];

      if (sparse_size > Integer.MAX_VALUE || sparse_size < 0) {
        throw new RuntimeException("sparse_size out of range");
      }

      return new SparseLeafNode(this, b, (int) sparse_size);
    }
    else {
      throw new RuntimeException("Unknown special node.");
    }

  }


  /**
   * Formats this node reference as a parsable string.
   */
  public String formatString() {
    StringBuilder b = new StringBuilder();
    b.append(Long.toHexString(ref[0]));
    b.append(".");
    b.append(Long.toHexString(ref[1]));
    return b.toString();
  }

  // ----- Statics -----

  private static final long INMEMORY_HIGH_LONG;
  private static final long SPARSE_HIGH_LONG;
  private static final long ENCODEDA_190BITS_HIGH_LONG;
  static {
    long code = 1;
    code = code << 60;
    INMEMORY_HIGH_LONG = code;

    code = 2;
    code = code << 60;
    SPARSE_HIGH_LONG = code;

    code = 3;
    code = code << 60;
    ENCODEDA_190BITS_HIGH_LONG = code;
  }

  /**
   * Creates a node that meets the spec of being a reference to a local
   * in-memory 'mutable' node with a 64-bit reference.
   */
  public static NodeReference createInMemoryNode(long ref64bit) {
    return new NodeReference(INMEMORY_HIGH_LONG, ref64bit);
  }

  /**
   * Returns a NodeReference for a sparse tree node of the given byte char and
   * size.
   */
  public static NodeReference createSpecialSparseNode(byte b, long max_size) {
    // Sanity check,
    if (max_size < 0 || max_size > Integer.MAX_VALUE) {
      throw new RuntimeException(
                           "Sparse node size out of range (" + max_size + ")");
    }

    return new NodeReference((SPARSE_HIGH_LONG | b), max_size);
  }

  /**
   * Parses the string into a NodeReference (as formatted with the
   * 'formatString' method.
   */
  public static NodeReference parseString(String str) {
    // Find the deliminator,
    int p = str.indexOf(".");
    if (p == -1) {
      throw new RuntimeException("format error");
    }
    long highv = Long.parseLong(str.substring(0, p), 16);
    long lowv = Long.parseLong(str.substring(p + 1), 16);
    return new NodeReference(highv, lowv);
  }

  // ----- Utility -----

  /**
   * The 'toString' method.
   */
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("NODE:");
    b.append(Long.toHexString(ref[0]));
    b.append(".");
    b.append(Long.toHexString(ref[1]));
    return b.toString();
  }

  // ----- Inner classes -----

  /**
   * A tree leaf that's a span of the repeat of a given byte.
   */
  private static class SparseLeafNode extends TreeLeaf {

    /**
     * The byte that's in the sparse node.
     */
    private final byte sparce_byte;

    private final NodeReference node_ref;
    private final int leaf_size;

    /**
     * Constructor.
     */
    public SparseLeafNode(NodeReference node_ref,
                          byte sparce_byte, int leaf_size) {
      super();
      this.node_ref = node_ref;
      this.leaf_size = leaf_size;
      this.sparce_byte = sparce_byte;
    }

    // ---------- Implemented from TreeLeaf ----------

    @Override
    public NodeReference getReference() {
      return node_ref;
    }

    @Override
    public int getSize() {
      return leaf_size;
    }

    @Override
    public int getCapacity() {
      throw new RuntimeException(
                           "Static node does not have a meaningful capacity.");
    }

    @Override
    public byte get(int position) throws IOException {
      return sparce_byte;
    }

    @Override
    public void get(int position, byte[] buf, int off, int len)
                                                          throws IOException {
      int end = off + len;
      for (int i = off; i < end; ++i) {
        buf[i] = sparce_byte;
      }
    }

    @Override
    public void writeDataTo(AreaWriter writer) throws IOException {
      int sz = getSize();
      for (int i = 0; i < sz; ++i) {
        writer.put(sparce_byte);
      }
    }

    @Override
    public void shift(int position, int offset) throws IOException {
      throw new IOException(
                      "Write methods not available for immutable store leaf.");
    }

    @Override
    public void put(int position, byte[] buf, int off, int len)
                                                          throws IOException {
      throw new IOException(
                      "Write methods not available for immutable store leaf.");
    }

    @Override
    public void setSize(int size) throws IOException {
      throw new IOException(
                      "Write methods not available for immutable store leaf.");
    }

    /**
     * Returns a heap size estimate for this node
     */
    @Override
    public int getHeapSizeEstimate() {
      // The size of the member variables +96 byte estimate for heap use for
      // Java object maintenance.
      return 1 + 8 + 4 + 96;
    }

  }

}
