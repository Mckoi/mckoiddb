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

import com.mckoi.util.ByteArrayUtil;

/**
 * A DataFile that is read-only and backs another DataFile, where all access
 * to the data goes through a paged buffer system.
 *
 * @author Tobias Downer
 * @deprecated Recommended that this is not used.
 */

public class BufferedDataFile implements AddressableDataFile {

  /**
   * The backed data file.
   */
  private DataFile data_file;

  /**
   * The current position.
   */
  private long position;

  /**
   * The size of the backed file.
   */
  private long size = -1;

  /**
   * The list of pages.
   */
  private final BufferedPage[] pages;

  /**
   * The size of the pages.
   */
  private final int page_size;

  /**
   * The number of pages.
   */
  private final int max_page_count;


  private byte[] encode_buf = new byte[8];
  private byte[] page_buf;
  private int page_offset;
  private long counter = 0;

  /**
   * Creates the buffered data file.
   */
  public BufferedDataFile(DataFile data_file) {
    this.data_file = data_file;
    this.position = 0;
    this.size = -1;
    this.page_size = 2048;
    this.max_page_count = 4;
    this.pages = new BufferedPage[max_page_count];
  }

  private UnsupportedOperationException readOnlyException() {
    return new java.lang.UnsupportedOperationException("Data file is read-only");
  }

  /**
   * Initializes inner variables.
   */
  private void init() {
    if (size == -1) {
      size = data_file.size();
    }
  }

  /**
   * Fetches the given page number.
   */
  private BufferedPage fetchPage(final long pos) {

    // The page number being fetched,
    final long page_no = pos / page_size;

    int last_null = -1;
    int oldest = -1;
    long old_val = Long.MAX_VALUE;
    // Is it buffered?
    int sz = max_page_count;
    for (int i = 0; i < sz; ++i) {
      BufferedPage p = pages[i];
      if (p == null) {
        last_null = i;
      }
      else if (p.page_no == page_no) {
        ++counter;
        p.last_ts = counter;
        return p;
      }
      else {
        long last_ts = p.last_ts;
        if (old_val > last_ts) {
          old_val = last_ts;
          oldest = i;
        }
      }
    }

    // Not found so load it,
    // Read the data,
    long page_pos = page_no * page_size;
    int to_get = (int) Math.min((long) page_size, size - page_pos);
    byte[] buf = new byte[to_get];
    data_file.position(page_pos);
    data_file.get(buf, 0, to_get);

    // Create the buffered page object,
    BufferedPage p = new BufferedPage();
    p.page_no = page_no;
    ++counter;
    p.last_ts = counter;
    p.page_content = buf;

    // Put it in the array either at the last 'null' entry or overwrite the
    // oldest page.
    if (last_null >= 0) {
      pages[last_null] = p;
    }
    else {
      pages[oldest] = p;
    }

    return p;
  }

//  /**
//   * Fetches the page from the backed file provided it fully encapsulates an
//   * object of the given size (in bytes). If it does fully encapsulate, the
//   * 'page_buf' and 'page_offset' values are set with the page where the
//   * object is found, and this function returns true. Otherwise returns
//   * false and the object's state is not changed.
//   */
//  private boolean pageEncapsulates(int size) {
//    int page_pos = (int) (position % page_size);
//    if (page_pos + size > page_size) {
//      // Object falls off the current page,
//      return false;
//    }
//    // Set up,
//    BufferedPage p = fetchPage(position);
//    page_buf = p.page_content;
//    page_offset = page_pos;
//    return true;
//  }

  /**
   * Fetches the number of bytes at the given position and either returns
   * true to indicate the result can be found in the 'page_buf' and
   * 'page_offset' location, or false to indicate the result is stored in the
   * 'encode_buf' array.
   * <p>
   * This has a maximum limit of 8 bytes.
   */
  private boolean fetchObject(int fetch_size) {
    int page_pos = (int) (position % page_size);
    BufferedPage p = fetchPage(position);
    if (page_pos + fetch_size > page_size) {
      // Object falls off the current page, so read into the 'encode_buf'
      // array.
      byte[] buf = p.page_content;
      int i = 0;
      while (fetch_size > 0) {
        encode_buf[i] = buf[page_pos];
        ++page_pos;
        ++i;
        --fetch_size;
        // Fetch the next page,
        if (page_pos >= page_size) {
          p = fetchPage(position + i);
          buf = p.page_content;
          page_pos = 0;
        }
      }
      position += fetch_size;
      return false;
    }
    else {
      // The results are in the page buf,
      page_buf = p.page_content;
      page_offset = page_pos;
      position += fetch_size;
      return true;
    }
  }



  @Override
  public long size() {
    init();
    return size;
  }

  @Override
  public void shift(long offset) {
    throw readOnlyException();
  }

  @Override
  public void setSize(long size) {
    throw readOnlyException();
  }

  @Override
  public void replicateTo(DataFile target) {
    data_file.position(position);
    data_file.replicateTo(target);
    position = data_file.position();
  }

  @Override
  public void replicateFrom(DataFile from) {
    throw readOnlyException();
  }

  @Override
  public void putShort(short s) {
    throw readOnlyException();
  }

  @Override
  public void putLong(long l) {
    throw readOnlyException();
  }

  @Override
  public void putInt(int i) {
    throw readOnlyException();
  }

  @Override
  public void putChar(char c) {
    throw readOnlyException();
  }

  @Override
  public void put(byte[] buf) {
    throw readOnlyException();
  }

  @Override
  public void put(byte[] buf, int off, int len) {
    throw readOnlyException();
  }

  @Override
  public void put(byte b) {
    throw readOnlyException();
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public void position(long position) {
    this.position = position;
  }

  @Override
  public short getShort() {
    init();
    if (fetchObject(2)) {
      return ByteArrayUtil.getShort(page_buf, page_offset);
    }
    else {
      return ByteArrayUtil.getShort(encode_buf, 0);
    }
  }

  @Override
  public long getLong() {
    init();
    if (fetchObject(8)) {
      return ByteArrayUtil.getLong(page_buf, page_offset);
    }
    else {
      return ByteArrayUtil.getLong(encode_buf, 0);
    }
  }

  @Override
  public int getInt() {
    init();
    if (fetchObject(4)) {
      return ByteArrayUtil.getInt(page_buf, page_offset);
    }
    else {
      return ByteArrayUtil.getInt(encode_buf, 0);
    }
  }

  @Override
  public char getChar() {
    init();
    if (fetchObject(2)) {
      return ByteArrayUtil.getChar(page_buf, page_offset);
    }
    else {
      return ByteArrayUtil.getChar(encode_buf, 0);
    }
  }

  @Override
  public void get(byte[] buf, int off, int len) {
    // Pass through, for now,
    data_file.position(position);
    data_file.get(buf, off, len);
    position = data_file.position();
  }

  @Override
  public byte get() {
    if (fetchObject(1)) {
      return page_buf[page_offset];
    }
    else {
      return encode_buf[0];
    }
  }

  @Override
  public void delete() {
    throw readOnlyException();
  }

  @Override
  public void copyTo(DataFile target, long size) {
    data_file.position(position);
    data_file.copyTo(target, size);
    position = data_file.position();
  }

  @Override
  public void copyFrom(DataFile from, long size) {
    throw readOnlyException();
  }

  @Override
  public Object getBlockLocationMeta(long start_position, long end_position) {
    if (data_file instanceof AddressableDataFile) {
      AddressableDataFile adf = (AddressableDataFile) data_file;
      return adf.getBlockLocationMeta(start_position, end_position);
    }
    return null;
  }

  // ----- Inner classes -----

  private static class BufferedPage {

    private long page_no;
    private byte[] page_content;
    private long last_ts;

  }

}
