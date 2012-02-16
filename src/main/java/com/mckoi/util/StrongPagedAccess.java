/**
 * com.mckoi.util.StrongPagedAccess  Jul 28, 2009
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

package com.mckoi.util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

/**
 * This object wraps a RandomAccessFile to provide a type of buffered access
 * to the file, where the buffers are stored by a strong reference in a hash
 * map. This is a convenient way to improve random access performance on a
 * file. Note that cached pages read from the underlying RandomAccessFile will
 * stay in memory until the object is GC'd, therefore care should be taken with
 * memory management.
 * <p>
 * Note that if the underlying file is modified, this object must either be
 * invalidated entirely or page sections must be invalidated.
 *
 * @author Tobias Downer
 */

public class StrongPagedAccess {

  /**
   * The RandomAccessFile.
   */
  private final RandomAccessFile file;

  /**
   * The WeakHashMap that holds the pages from the backed file.
   */
  private final HashMap<Long, byte[]> cache;

  /**
   * The size of the pages.
   */
  private final int page_size;

  /**
   * The number of cache hits.
   */
  private int cache_hit;

  /**
   * The number of cache misses.
   */
  private int cache_miss;

  /**
   * Constructor.
   */
  public StrongPagedAccess(RandomAccessFile file, int page_size) {
    this.file = file;
    this.page_size = page_size;
    this.cache = new HashMap();
  }

  private byte[] fetchPage(long page_no) throws IOException {
    byte[] page = cache.get(page_no);
    if (page == null) {
      page = new byte[page_size];
      file.seek(page_no * page_size);
      int n = 0;
      int sz = page_size;
      while (sz > 0) {
        int read_count = file.read(page, n, sz);
        if (read_count == -1) {
          // eof
          break;
        }
        n += read_count;
        sz -= read_count;
      }
      cache.put(new Long(page_no), page);
      ++cache_miss;
    }
    else {
      ++cache_hit;
    }
    return page;
  }

  /**
   * Returns the number of cache hits.
   */
  public int getCacheHits() {
    return cache_hit;
  }

  /**
   * Returns the number of cache misses.
   */
  public int getCacheMiss() {
    return cache_miss;
  }

  /**
   * Clears the cache if the number of pages exceeds the given count. This is
   * a very crude way of freeing up cache resources.
   */
  public void clearIfOverSize(int number) {
    if (cache.size() > number) {
      cache.clear();
    }
  }

  /**
   * Invalidates the given section of the buffer (presumably because the file
   * changed).
   */
  public void invalidateSection(long pos, int sz) {
    while (sz > 0) {
      // Get the page,
      long page_no = (pos / page_size);
      // Remove it from the cache,
      cache.remove(page_no);

      long next_page_pos = (page_no + 1) * page_size;
      int skip = (int) (next_page_pos - pos);

      // Go to the next page,
      sz -= skip;
      pos += skip;
    }
  }

  /**
   * Gets a part of the file. This may change the pointer of the
   * RandomAccessFile. Note that this does not perform length check of the
   * file and thus will never return -1 indicated the end of the file
   * reached.
   */
  public int read(long pos, byte[] buf, int off, int len) throws IOException {
    // Get the page,
    long page_no = (pos / page_size);
    // Is the page in the cache?
    byte[] page = fetchPage(page_no);

    // The offset of the position inside the page,
    int page_off = (int) (pos - (page_no * page_size));
    // The maximum we can read,
    int max_read = page_size - page_off;
    // How much we are going to read,
    int to_read = Math.min(len, max_read);

    // Go ahead and copy the content,
    System.arraycopy(page, page_off, buf, off, to_read);

    return to_read;
  }

  /**
   * Reads a single byte from the file at the given position.
   */
  public byte readByte(long pos) throws IOException {
    // Get the page,
    long page_no = (pos / page_size);
    // Is the page in the cache?
    byte[] page = fetchPage(page_no);

    // The offset of the position inside the page,
    int page_off = (int) (pos - (page_no * page_size));

    // Return the value at the page offset,
    return page[page_off];
  }

  /**
   * Read the section of the file fully.
   */
  public void readFully(long pos, byte[] buf, int off, int len)
                                                          throws IOException {
    while (len > 0) {
      int read_count = read(pos, buf, off, len);
      len -= read_count;
      pos += read_count;
      off += read_count;
    }
  }

  /**
   * Reads a long value from the given position in the file.
   */
  public long readLong(long pos) throws IOException {
    long c1 = (((int) readByte(pos + 0)) & 0x0FF);
    long c2 = (((int) readByte(pos + 1)) & 0x0FF);
    long c3 = (((int) readByte(pos + 2)) & 0x0FF);
    long c4 = (((int) readByte(pos + 3)) & 0x0FF);
    long c5 = (((int) readByte(pos + 4)) & 0x0FF);
    long c6 = (((int) readByte(pos + 5)) & 0x0FF);
    long c7 = (((int) readByte(pos + 6)) & 0x0FF);
    long c8 = (((int) readByte(pos + 7)) & 0x0FF);

    return (c1 << 56) + (c2 << 48) + (c3 << 40) +
           (c4 << 32) + (c5 << 24) + (c6 << 16) + (c7 <<  8) + (c8);
  }

  /**
   * Reads an integer value from the given position in the file.
   */
  public int readInt(long pos) throws IOException {
    int c1 = (((int) readByte(pos + 0)) & 0x0FF);
    int c2 = (((int) readByte(pos + 1)) & 0x0FF);
    int c3 = (((int) readByte(pos + 2)) & 0x0FF);
    int c4 = (((int) readByte(pos + 3)) & 0x0FF);
    return (c1 << 24) + (c2 << 16) + (c3 << 8) + (c4);
  }

  /**
   * Reads a short value from the given position in the file.
   */
  public short readShort(long pos) throws IOException {
    int c1 = (((int) readByte(pos + 0)) & 0x0FF);
    int c2 = (((int) readByte(pos + 1)) & 0x0FF);
    return (short) ((c1 << 8) + (c2));
  }

}
