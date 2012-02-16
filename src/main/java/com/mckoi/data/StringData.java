/**
 * com.mckoi.treestore.StringData  Dec 14, 2007
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

package com.mckoi.data;

import java.text.CharacterIterator;
import java.io.Reader;
import java.io.Writer;
import java.io.IOException;

/**
 * An object that wraps a DataFile to provide an mutable string object of any
 * size.  This object is useful for implementing character string based stored
 * data items (textual documents, logs, etc).
 * <p>
 * This class provides efficient methods for character iteration, substring by
 * location, and string removal and insertion by location.  Each character
 * of the string is stored as a 16 bit char primitive.
 *
 * @author Tobias Downer
 */

public class StringData {

  /**
   * The backing DataFile object.
   */
  private final DataFile data;

  /**
   * Constructs the string object mapped to the given DataFile object.
   */
  public StringData(DataFile data) {
    this.data = data;
  }

  /**
   * The number of characters in the string.
   */
  private long charCount() {
    return data.size() / 2;
  }

  /**
   * Positions the backed data file to the given character position.
   */
  private void position(long pos) {
    data.position(pos * 2);
  }

  /**
   * Sets the size of the string data item (by number of characters).
   */
  private void setSize(long sz) {
    data.setSize(sz * 2);
  }

  /**
   * Writes a string at the given position, expanding the size of the string
   * object if necessary.
   */
  private void actualWriteString(long pos, String str) {
    int len = str.length();
    // Position and write the characters
    position(pos);
    for (int i = 0; i < len; ++i) {
      data.putChar(str.charAt(i));
    }
  }

  /**
   * Reads the string from the file and returns a String object.
   */
  private String readString(long pos, int sz) {
    StringBuilder buf = new StringBuilder();
    position(pos);
    for (int i = 0; i < sz; ++i) {
      buf.append(data.getChar());
    }
    return buf.toString();
  }

  /**
   * Returns the number of characters in the string.  This operation is a
   * function of the size of the mapped data file, therefore a fast computation.
   */
  public long length() {
    return charCount();
  }

  /**
   * Writes a string to the given position in the string object overwriting any
   * string characters that may already be there, and expanding the size of the
   * data file to accommodate any characters that may be written past the end
   * of the current size of the string.  'pos' = 0 is the first character, 1
   * is the second character, etc.
   */
  public void writeString(long pos, String str) {
    int len = str.length();
    long str_end = pos + len;
    // If pos + len is greater than the current size, then set it to a new
    // size
    if (str_end > charCount()) {
      setSize(str_end);
    }
    actualWriteString(pos, str);
  }

  /**
   * Appends a string to the end of the data, expanding the size of the file
   * to accommodate the new data.
   */
  public void append(String str) {
    // Set the position to write the string
    long pos = charCount();
    setSize(pos + str.length());
    // And write the data
    actualWriteString(pos, str);
  }

  /**
   * Inserts a string at the given position in the text area shifting all
   * text after the position forward by the size of the string being
   * inserted and expanding the length of the string object by the size of the
   * inserted string. 'pos' = 0 is the first character, 1 is the second
   * character, etc.
   */
  public void insert(long pos, String str) {
    // The length
    int len = str.length();
    // shift the data area by the length of the string
    position(pos);
    data.shift(len * 2);
    // and write the string
    actualWriteString(pos, str);
  }

  /**
   * Removes a section of the text area and shifts text past the area into the
   * space removed and shrinking the length of the string object.  'pos' = 0
   * is the first character, 1 is the second character, etc.
   */
  public void remove(long pos, long size) {
    // Some checks
    long data_size = charCount();
    assert(pos >= 0 && size >= 0 && pos + size < data_size);

    position(pos + size);
    data.shift(-(size * 2));
  }

  /**
   * Returns a substring of the string data object where 'pos' is the position
   * of the first character of the substring and 'size' is the size of the
   * substring in characters.
   */
  public String substring(long pos, int size) {
    // Some checks
    long data_size = charCount();
    assert(pos >= 0 && size >= 0 && pos + size < data_size);

    return readString(pos, size);
  }

  /**
   * Returns the entire string data object as a string.
   */
  public String toString() {
    return readString(0, (int) charCount());
  }

  /**
   * Returns a CharacterIterator object over the complete string.  The returned
   * object is undefined if the data changes while this iterator is in use.
   */
  public CharacterIterator getCharacterIterator() {
    return new SDCharIterator(0,
                   (int) Math.min((long) Integer.MAX_VALUE, charCount()));
  }

  /**
   * Returns a java.io.Reader object that can be used to read characters
   * sequentially in the string. The behavior of the returned object is
   * undefined if the data changes while this object is in use.
   */
  public Reader getReader() {
    return new SDReader(0, charCount());
  }

  /**
   * Returns a java.io.Writer object that can be used to write characters at
   * the given position.  Any existing characters in the data object from
   * the given position will be overwritten. The behavior of the returned
   * object is undefined if the data changes while this object is in use.
   */
  public Writer getWriter(long pos) {
    return new SDWriter(pos);
  }

  /**
   * Returns a java.io.Writer object that can be used to write characters
   * to the string object from the start position onwards. Any existing
   * information in the data object will be overwritten. This is the
   * same as 'getWriter(0)'.
   */
  public Writer getWriter() {
    return getWriter(0);
  }


  // ----- Inner classes ------

  /**
   * The Writer implementation.
   */
  private class SDWriter extends Writer {

    long pos;

    SDWriter(long pos) {
      this.pos = pos;
    }

    public void write(int v) throws IOException {
      position(pos);
      ++pos;
      data.putChar((char) v);
    }

    public void write(String str, int off, int len) throws IOException {
      // Change the size if necessary
      long enda = charCount();
      if (pos + len > enda) {
        setSize(pos + len);
      }
      // Position and write
      position(pos);
      for (int i = off; i < off + len; ++i) {
        data.putChar(str.charAt(i));
      }
      pos += len;
    }
    
    public void write(char[] cbuf, int off, int len) throws IOException {
      // Change the size if necessary
      long enda = charCount();
      if (pos + len > enda) {
        setSize(pos + len);
      }
      // Position and write
      position(pos);
      for (int i = off; i < off + len; ++i) {
        data.putChar(cbuf[i]);
      }
      pos += len;
    }

    public void close() throws IOException {
      // No implementation
    }

    public void flush() throws IOException {
      // No implementation
    }

  }

  /**
   * The Reader implementation.
   */
  private class SDReader extends Reader {

    long pos;
    long end;
    long mark;

    SDReader(long s, long e) {
      this.pos = s;
      this.end = e;
    }



    public int read() throws IOException {
      // End of stream reached
      if (pos >= end) {
        return -1;
      }
      position(pos);
      ++pos;
      return data.getChar();
    }

    public int read(char[] cbuf, int off, int len) throws IOException {
      assert(len >= 0 && off >= 0);
      // As per the contract, if we have reached the end return -1
      if (pos >= end) {
        return -1;
      }

      position(pos);
      long act_end = Math.min(pos + len, end);
      int to_read = (int) (act_end - pos);
      for (int i = off; i < off + to_read; ++i) {
        cbuf[i] = data.getChar();
      }
      pos += to_read;
      return to_read;
    }

    public int skip(int skip) throws IOException {
      assert(skip >= 0);
      long act_end = Math.min(pos + skip, end);
      pos = act_end;
      return (int) (act_end - pos);
    }

    public void mark(int mark_limit) throws IOException {
      mark = pos;
    }

    public void reset() throws IOException {
      pos = mark;
    }

    public void close() throws IOException {
      // No implementation
    }

  }

  /**
   * The character iterator.
   */
  private class SDCharIterator implements CharacterIterator {

    int pos;
    int start, end;

    SDCharIterator(int s, int e) {
      this.pos = s - 1;
      this.start = s;
      this.end = e;
    }

    public char current() {
      // If position is out of range, return done
      if (pos < start || pos >= end) {
        return DONE;
      }
      position(pos);
      return data.getChar();
    }

    public char first() {
      pos = start;
      return current();
    }

    public char last() {
      pos = end - 1;
      return current();
    }

    public char next() {
      ++pos;
      return current();
    }

    public char previous() {
      --pos;
      return current();
    }

    public int getBeginIndex() {
      return start;
    }

    public int getEndIndex() {
      return end - 1;
    }

    public int getIndex() {
      return pos;
    }

    public char setIndex(int p) {
      pos = p;
      return current();
    }

    public Object clone() {
      return new SDCharIterator(start, end);
    }

  }

}
