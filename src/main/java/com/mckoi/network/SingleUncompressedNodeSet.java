/**
 * com.mckoi.network.SingleUncompressedNodeSet  Jul 16, 2009
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
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * The binary representation of a single uncompressed node.
 *
 * @author Tobias Downer
 */

class SingleUncompressedNodeSet implements NodeSet {

  private final NodeReference[] node_ids;
  private final byte[] buf;

  SingleUncompressedNodeSet(BlockId block_id, int data_id, byte[] buf) {
    DataAddress da = new DataAddress(block_id, data_id);
    this.node_ids = new NodeReference[] { da.getValue() };
    this.buf = buf;
  }

  SingleUncompressedNodeSet(NodeReference node_id, byte[] buf) {
    this.node_ids = new NodeReference[] { node_id };
    this.buf = buf;
  }

  SingleUncompressedNodeSet(NodeReference[] node_ids, byte[] buf) {
    this.node_ids = node_ids;
    this.buf = buf;
  }

  
  public Iterator<NodeItemBinary> getNodeSetItems() {
    return new Iterator<NodeItemBinary>() {
      int p = 0;
      public boolean hasNext() {
        return p == 0;
      }
      public NodeItemBinary next() {
        if (p != 0) {
          throw new NoSuchElementException();
        }
        ++p;
        return new SUItemBinary(node_ids[0], buf);
      }
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
  
//  public InputStream getNodeSetInputStream() {
//    return new InputStream() {
//      int p = 0;
//      long node_id = node_ids[0];
//      public int read() throws IOException {
//        if (p > buf.length + 12) {
//          return -1;
//        }
//        try {
//          if (p == 0) {
//            return (int) ((node_id >>> 56) & 0xFF);
//          }
//          else if (p == 1) {
//            return (int) ((node_id >>> 48) & 0xFF);
//          }
//          else if (p == 2) {
//            return (int) ((node_id >>> 40) & 0xFF);
//          }
//          else if (p == 3) {
//            return (int) ((node_id >>> 32) & 0xFF);
//          }
//          else if (p == 4) {
//            return (int) ((node_id >>> 24) & 0xFF);
//          }
//          else if (p == 5) {
//            return (int) ((node_id >>> 16) & 0xFF);
//          }
//          else if (p == 6) {
//            return (int) ((node_id >>> 8) & 0xFF);
//          }
//          else if (p == 7) {
//            return (int) ((node_id >>> 0) & 0xFF);
//          }
//          // size
//          else if (p == 8) {
//            return (int) ((buf.length >>> 24) & 0xFF);
//          }
//          else if (p == 9) {
//            return (int) ((buf.length >>> 16) & 0xFF);
//          }
//          else if (p == 10) {
//            return (int) ((buf.length >>> 8) & 0xFF);
//          }
//          else if (p == 11) {
//            return (int) ((buf.length >>> 0) & 0xFF);
//          }
//          else {
//            return buf[p - 12];
//          }
//        }
//        finally {
//          ++p;
//        }
//      }
//    };
//  }

  public NodeReference[] getNodeIdSet() {
    return node_ids;
  }

  public void writeEncoded(DataOutput dout) throws IOException {
    dout.writeInt(buf.length);
    dout.write(buf);
  }

  private static class SUItemBinary implements NodeItemBinary {

    private final NodeReference node_id;
    private final byte[] buf;

    SUItemBinary(NodeReference node_id, byte[] buf) {
      this.node_id = node_id;
      this.buf = buf;
    }

    public byte[] asBinary() {
      return buf;
    }

    public InputStream getInputStream() {
      return new ByteArrayInputStream(buf);
    }

    public NodeReference getNodeId() {
      return node_id;
    }

  }

}
