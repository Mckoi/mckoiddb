/**
 * com.mckoi.network.CompressedNodeSet  Jul 16, 2009
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

package com.mckoi.network;

import com.mckoi.data.NodeReference;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.zip.InflaterInputStream;

/**
 * A compressed set of nodes.
 *
 * @author Tobias Downer
 */

public class CompressedNodeSet implements NodeSet {

  /**
   * The set of node_ids stored in this set.
   */
  private final NodeReference[] node_ids;

  /**
   * The compressed encoded form of the node set.
   */
  private final byte[] compressed_form;

  /**
   * Constructor.
   */
  CompressedNodeSet(NodeReference[] node_ids, byte[] encoded_form) {
    this.node_ids = node_ids;
    this.compressed_form = encoded_form;
  }

  public Iterator<NodeItemBinary> getNodeSetItems() {
    return new CompressedIterator();
  }

  public NodeReference[] getNodeIdSet() {
    return node_ids;
  }

  public void writeEncoded(DataOutput dout) throws IOException {
    dout.writeInt(compressed_form.length);
    dout.write(compressed_form);
  }


  // -----

  private class CompressedIterator implements Iterator<NodeItemBinary> {

    /**
     * The deflater stream over the compressed item set;
     */
    private final InflaterInputStream comp_in;
    private final DataInputStream data_in;

    /**
     * The index of the current node,
     */
    private int node_index;

    CompressedIterator() {
      comp_in = new InflaterInputStream(
                                    new ByteArrayInputStream(compressed_form));
      data_in = new DataInputStream(comp_in);
      this.node_index = 0;
    }

    public boolean hasNext() {
      return node_index < node_ids.length;
    }

    public NodeItemBinary next() {
      NodeItemBinary b = new CompressedNodeItem(node_ids[node_index], data_in);
      ++node_index;
      return b;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  private static class CompressedNodeItem implements NodeItemBinary {

    private final NodeReference node_id;
    private final DataInputStream data_in;

    CompressedNodeItem(NodeReference node_id, DataInputStream data_in) {
      this.node_id = node_id;
      this.data_in = data_in;
    }

    public byte[] asBinary() {
      return null;
    }

    public InputStream getInputStream() {
      return data_in;
    }

    public NodeReference getNodeId() {
      return node_id;
    }

  }

}
