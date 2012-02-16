/**
 * com.mckoi.treestore.DataFileUtils  13 Sep 2006
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

import java.io.*;

/**
 * Static utility methods and classes for managing data in a DataFile.
 * 
 * @author Tobias Downer
 */

public final class DataFileUtils {

  /**
   * Creates an InputStream object wrapped around a DataFile object.  This is
   * a simple wrapper around a DataFile object. Any access on the underlying
   * DataFile object while the input stream is in use will lead to undefined
   * behaviour.
   * <p>
   * Note that this is only intended as a short lived object for reading
   * information from a DataFile.
   */
  public static InputStream asInputStream(DataFile df) {
    return new DFInputStream(df);
  }

  /**
   * Creates an OutputStream object wrapped around a DataFile object.  This is
   * a simple wrapper around a DataFile object. Any access on the underlying
   * DataFile object while the output stream is in use will lead to undefined
   * behaviour. This object overwrites any information stored in the data
   * file past the current position.
   * <p>
   * Note that this is only intended as a short lived object for writing data
   * to a DataFile.
   */
  public static OutputStream asOutputStream(DataFile df) {
    return new DFOutputStream(df);
  }

  /**
   * Creates an OutputStream object wrapped around a DataFile object that
   * skips the front part of the file that is already written if the data
   * is the same. For example, if the DataFile currently contains
   * { 1, 7, 3, 1, 5, 0, 9 } and { 1, 7, 3, 1, 2, 2, 2, 5, 0, 9 } is written
   * to the output stream, the actual write will start at the
   * {2, 2, 2, 5, 0, 9 } sequence at index 4. The first 4 bytes are not
   * overwritten.
   * <p>
   * The purpose of this is to detect and prevent writing to a data file
   * when the content is already the same, or when the start is the same.
   * The implementation is very simple and will not detect sameness past the
   * first matching sequence.
   * <p>
   * This object acts like a FileOutputStream such that any information
   * stored in the DataFile past the position is overwritten. When the
   * OutputStream is closed the DataFile is truncated to the current
   * position.
   */
  public static OutputStream asSimpleDifferenceOutputStream(DataFile df) {
    return new SDDFOutputStream(df);
  }

  /**
   * Creates and returns a DataInputStream object wrapped around a DataFile
   * object. Any access on the underlying
   * DataFile object while the input stream is in use will lead to undefined
   * behaviour.
   * <p>
   * Note that this is only intended as a short lived object for reading
   * information from a DataFile.
   */
  public static DataInputStream asDataInputStream(DataFile df) {
    return new DataInputStream(asInputStream(df));
  }
  
  /**
   * Creates and returns a DataOutputStream object wrapped around a DataFile
   * object. Any access on the underlying
   * DataFile object while the output stream is in use will lead to undefined
   * behaviour. This object overwrites any information stored in the data
   * file past the current position.
   * <p>
   * Note that this is only intended as a short lived object for writing data
   * to a DataFile.
   */
  public static DataOutputStream asDataOutputStream(DataFile df) {
    return new DataOutputStream(asOutputStream(df));
  }

  /**
   * Returns an empty immutable DataFile implementation.
   */
  public static AddressableDataFile getEmptyImmutableDataFile() {
    return new EmptyDataFile();
  }

  // ----- Inner classes -----

  private static class DFInputStream extends InputStream {
    
    private final DataFile df;
    private long marked_position;
    
    DFInputStream(DataFile df) {
      this.df = df;
      this.marked_position = 0;
    }

    public int read() throws IOException {
      if (df.position() >= df.size()) {
        return -1;
      }
      else {
        return ((int) df.get()) & 0x0FF;
      }
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
      // Trivial condition
      if (len == 0) {
        return 0;
      }
      long p = df.position();
      long s = df.size();
      // The amount to read, either the length of the array or the amount of
      // data left available, whichever is smaller.
      long to_read = Math.min((long) len, s - p);
      // If nothing to read, return -1 (end of stream reached).
      if (to_read == 0) {
        return -1;
      }
      // Fill up the array
      int act_read = (int) to_read;
      df.get(buf, off, act_read);
      return act_read;
    }

    @Override
    public long skip(long n) throws IOException {
      long p = df.position();
      long s = df.size();
      long to_skip = Math.min(n, s - p);
      df.position(p + to_skip);
      return to_skip;
    }

    @Override
    public int available() throws IOException {
      long p = df.position();
      long s = df.size();
      long available = Math.min((long) Integer.MAX_VALUE, s - p);
      return (int) available;
    }

    @Override
    public void mark(int readlimit) {
      marked_position = df.position();
    }

    @Override
    public void reset() throws IOException {
      df.position(marked_position);
    }

    @Override
    public boolean markSupported() {
      return true;
    }

  }

  private static class DFOutputStream extends OutputStream {
    
    private final DataFile df;
    
    DFOutputStream(DataFile df) {
      this.df = df;
    }

    public void write(int b) throws IOException {
      df.put((byte) b);
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
      df.put(buf, off, len);
    }
    
    @Override
    public void flush() throws IOException {
      super.flush();
    }

  }

  private static class SDDFOutputStream extends OutputStream {

    private final DataFile df;
//    private long change_count = 0;

    SDDFOutputStream(DataFile df) {
      this.df = df;
    }

    public void write(int b) throws IOException {
      // Only write if either the position is at the end or the current value
      // is different.
      byte ib = (byte) b;
      long p = df.position();
      // If not at the end yet,
      if (p < df.size()) {
        // Get the current value,
        byte cur_b = df.get();
        // If the value we are inserting is different,
        if (cur_b != ib) {
          // Reposition and put the new value,
          df.position(p);
          df.put(ib);
//          ++change_count;
        }
      }
      // At the end so write the byte,
      else {
        df.put(ib);
      }
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
      long p = df.position();
      long sz = df.size();
      // Scan until we reach a byte that's different,
      int end = off + len;
      for (; off < end && p < sz; ++off, ++p) {
        // We break at the first byte that's different,
        if (df.get() != buf[off]) {
          df.position(p);
          break;
        }
      }
      len = end - off;
      // Ok, now write the rest,
//      change_count += len;
      df.put(buf, off, len);
    }

    @Override
    public void flush() throws IOException {
      super.flush();
    }

    @Override
    public void close() throws IOException {
      // Truncate,
      long p = df.position();
      if (p < df.size()) {
        df.setSize(p);
      }
      super.close();
    }

  }

  /**
   * An empty immutable DataFile implementation that is 0 bytes in size and
   * generates an exception on all query(except size query) and mutation
   * methods.
   */
  static class EmptyDataFile implements AddressableDataFile {
    private RuntimeException emptyDataFileException() {
      return new RuntimeException("EmptyDataFile");
    }
    @Override
    public long size() {
      return 0;
    }
    @Override
    public void position(long position) {
      if (position != 0) {
        throw new RuntimeException("Position out of bounds");
      }
    }
    @Override
    public long position() {
      return 0;
    }
    @Override
    public byte get() {
      throw emptyDataFileException();
    }
    @Override
    public void get(byte[] buf, int off, int len) {
      throw emptyDataFileException();
    }
    @Override
    public short getShort() {
      throw emptyDataFileException();
    }
    @Override
    public int getInt() {
      throw emptyDataFileException();
    }
    @Override
    public long getLong() {
      throw emptyDataFileException();
    }
    @Override
    public char getChar() {
      throw emptyDataFileException();
    }
    @Override
    public void setSize(long size) {
      throw emptyDataFileException();
    }
    @Override
    public void delete() {
      throw emptyDataFileException();
    }
    @Override
    public void shift(long offset) {
      throw emptyDataFileException();
    }
    @Override
    public void put(byte b) {
      throw emptyDataFileException();
    }
    @Override
    public void put(byte[] buf, int off, int len) {
      throw emptyDataFileException();
    }
    @Override
    public void put(byte[] buf) {
      throw emptyDataFileException();
    }
    @Override
    public void putShort(short s) {
      throw emptyDataFileException();
    }
    @Override
    public void putInt(int i) {
      throw emptyDataFileException();
    }
    @Override
    public void putLong(long l) {
      throw emptyDataFileException();
    }
    @Override
    public void putChar(char c) {
      throw emptyDataFileException();
    }

    // Legacy
    @Override
    public void copyTo(DataFile target, long size) {
      target.copyFrom(this, size);
    }
    // Legacy
    @Override
    public void replicateTo(DataFile target) {
      target.replicateFrom(this);
    }

    @Override
    public void copyFrom(DataFile from, long size) {
      // If the size being copied is not zero, generate error
      if (size > 0) {
        throw emptyDataFileException();
      }
    }
    @Override
    public void replicateFrom(DataFile from) {
      // If from is not empty, generate an error
      if (from.size() > 0) {
        throw emptyDataFileException();
      }
    }

    @Override
    public Object getBlockLocationMeta(long start_position, long end_position) {
      return null;
    }

  }

}
