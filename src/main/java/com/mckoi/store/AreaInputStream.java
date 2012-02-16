/**
 * com.mckoi.store.AreaInputStream  07 Nov 2004
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

package com.mckoi.store;

import java.io.IOException;
import java.io.InputStream;

/**
 * An implementation of InputStream that reads data sequentially from an
 * Area object.  If the underlying Area is modified or its pointer is modified
 * then the behaviour of the InputStream is undefined.  This input stream
 * uses a buffer on the heap to reduce the number of reads from the area
 * object.
 * 
 * @author Tobias Downer
 */

public class AreaInputStream extends InputStream {

  /**
   * The Area object.
   */
  private final Area area;

  /**
   * The buffer.
   */
  private byte[] buffer;

  /**
   * The number of bytes available to be read in the buffer.
   */
  private int count;

  /**
   * The point in the buffer.
   */
  private int pos;
  
  /**
   * Constructor.
   */
  public AreaInputStream(Area area, int buffer_size) {
    if (buffer_size <= 0) {
      throw new RuntimeException("buffer_size <= 0");
    }

    this.area = area;
    this.buffer = new byte[buffer_size];
    this.count = 0;
    this.pos = 0;
  }

  public AreaInputStream(Area area) {
    this(area, 512);
  }

  /**
   * Reads data from the area into the buffer.  Returns false when end is
   * reached.
   */
  private boolean fillBuffer() throws IOException {
    if (count - pos <= 0) {
      int read_from_area =
               Math.min(area.capacity() - area.position(), buffer.length);
      // If can't read any more then return false
      if (read_from_area == 0) {
        return false;
      }
      area.get(buffer, 0, read_from_area);
      pos = 0;
      count = read_from_area;
    }
    return true;
  }

  /**
   * Copies a section from the buffer into the given array and returns the
   * amount actually read.
   */
  private int readFromBuffer(byte[] b, int off, int len) throws IOException {
    // If we can't fill the buffer, return -1
    if (!fillBuffer()) {
      return -1;
    }

    // What we can read,
    final int to_read = Math.min(count - pos, len);
    // Read the data,
    System.arraycopy(buffer, pos, b, off, to_read);
    // Advance the position
    pos += to_read;
    // Return the amount read,
    return to_read;
  }

  // ----- Implemented from InputStream -----

  public int read() throws IOException {
    // If we can't fill the buffer, return -1
    if (!fillBuffer()) {
      return -1;
    }

    int p = ((int) buffer[pos]) & 0x0FF;
    ++pos;
    return p;
  }

  public int read(byte[] b, int off, int len) throws IOException {
    int has_read = 0;
    // Try and read
    while (len > 0) {
      int read = readFromBuffer(b, off, len);

      // If the end of the stream reached
      if (read == -1) {
        // And something has been read, return the amount we read,
        if (has_read > 0) {
          return has_read;
        }
        // Otherwise return -1
        else {
          return -1;
        }
      }

      off += read;
      has_read += read;
      len -= read;
    }

    return has_read;
  }

  public long skip(long n) throws IOException {
    // Make sure n isn't larger than an integer max value
    n = Math.min(n, Integer.MAX_VALUE);

    if (n > 0) {
      // Trivially change the area pointer
      area.position(area.position() + (int) n);
      // And empty the buffer
      pos = 0;
      count = 0;

      return n;
    }
    else if (n < 0) {
      throw new RuntimeException("Negative skip");
    }

    return n;
  }

  public int available() throws IOException {
    return (area.capacity() - area.position()) + (count - pos);
  }

  public void close() throws IOException {
    // Nothing to do here,
  }



//  // ---------- Implemented from InputStream ----------
//  
//  public int read() throws IOException {
//    if (area.position() >= area.capacity()) {
//      return -1;
//    }
//    else {
//      byte b = area.get();
//      return ((int) b) & 0x0FF;
//    }
//  }
//
//  public int read(byte[] b, int off, int len) throws IOException {
//    final int max = area.capacity() - area.position();
//    if (max == 0) {
//      return -1;
//    }
//    final int to_read = Math.min(max, len);
//    area.get(b, off, to_read);
//    return to_read;
//  }
//
//  public int available() throws IOException {
//    return area.capacity() - area.position();
//  }
//
//  public long skip(long n) throws IOException {
//    // Make sure n isn't larger than an integer max value
//    n = Math.min(n, Integer.MAX_VALUE);
//
//    if (n > 0) {
//      // Change the area pointer
//      area.position(area.position() + (int) n);
//
//      return n;
//    }
//    else if (n < 0) {
//      throw new RuntimeException("Negative skip");
//    }
//
//    return n;
//  }

}

