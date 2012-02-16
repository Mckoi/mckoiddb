/**
 * com.mckoi.network.NodeItemBinary  Jul 16, 2009
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

package com.mckoi.network;

import com.mckoi.data.NodeReference;
import java.io.InputStream;

/**
 * Accessor for a binary node item.
 *
 * @author Tobias Downer
 */

public interface NodeItemBinary {

  /**
   * Returns the nodeid value of the item.
   */
  NodeReference getNodeId();

  /**
   * Returns an input stream that allows iteration through the item. Only one
   * input stream can be made per binary item. The input stream needs to be
   * read to completion if 'asBinary' returns null.
   */
  InputStream getInputStream();

  /**
   * Returns the item as a byte[] array. This returns null if the node item
   * is compressed and the current item is unknown.
   */
  byte[] asBinary();

}
