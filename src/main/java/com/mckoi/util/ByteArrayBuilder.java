/**
 * com.mckoi.util.ByteArrayBuilder  Oct 30, 2011
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * A convenience class for building a byte[] array object. This class
 * implements OutputStream.
 *
 * @author Tobias Downer
 */

public class ByteArrayBuilder extends OutputStream {

  /**
   * The byte array.
   */
  private byte[] buf;

  /**
   * The current size.
   */
  private int count;

  /**
   * Constructs the builder with an internal byte[] array of the given size.
   */
  public ByteArrayBuilder(int size) {
    this.buf = new byte[Math.max(2, size)];
    count = 0;
  }

  /**
   * Constructs the builder with an internal byte[] array of default size.
   */
  public ByteArrayBuilder() {
    this(64);
  }

  /**
   * Ensures there's enough room to fill n number of bytes. If not, expands
   * the byte[] array so that it can.
   */
  protected void ensureCanWrite(int len) {
    int newcount = count + len;
    if (newcount > buf.length) {
      buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newcount));
    }
  }

  /**
   * Returns the number of bytes written.
   */
  public int length() {
    return count;
  }

  /**
   * Returns the backed byte[] buffer.
   */
  public byte[] getBuffer() {
    return buf;
  }

  // --- OutputStream ---

  @Override
  public void write(int b) throws IOException {
    ensureCanWrite(1);
    buf[count] = (byte) b;
    ++count;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if ((off < 0) || (off > b.length) || (len < 0) ||
       ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }
    else if (len == 0) {
      return;
    }
    ensureCanWrite(len);
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

}
