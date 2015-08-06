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
 * An implementation of DataFile where the data is a byte array on the Java
 * heap. This is useful for debugging purposes.
 *
 * @author Tobias Downer
 * @deprecated In development, do not use.
 */

public class ByteArrayDataFile implements DataFile {

  /**
   * The size of the array.
   */
  private long count = 0;
  
  /**
   * The current position.
   */
  private long position = 0;
  
  /**
   * The buffer.
   */
  private byte[] buf;

  /**
   * Constructor.
   */
  public ByteArrayDataFile(int size) {
    this.buf = new byte[size];
    this.count = 0;
  }

  public ByteArrayDataFile() {
    this(4096);
  }

  public ByteArrayDataFile(byte[] buf, int off, int len) {
    this.buf = new byte[len];
    System.arraycopy(buf, off, this.buf, 0, len);
    count = len;
  }

  public ByteArrayDataFile(byte[] buf) {
    this(buf, 0, buf.length);
  }

  /**
   * Checks the bounds of the data file to ensure it can read at least the
   * given number of bytes.
   */
  private void checkBounds(int c) {
    if (position < 0 || position + c > count) {
      throw new java.lang.ArrayIndexOutOfBoundsException();
    }
  }



  @Override
  public long size() {
    return count;
  }

  @Override
  public void position(long position) {
    this.position = position;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public byte get() {
    checkBounds(1);
    byte b = buf[(int) position];
    ++position;
    return b;
  }

  @Override
  public void get(byte[] buf, int off, int len) {
    checkBounds(len);
    System.arraycopy(this.buf, (int) position, buf, off, len);
    position += len;
  }

  @Override
  public short getShort() {
    checkBounds(2);
    short s = ByteArrayUtil.getShort(buf, (int) position);
    position += 2;
    return s;
  }

  @Override
  public int getInt() {
    checkBounds(4);
    int i = ByteArrayUtil.getInt(buf, (int) position);
    position += 4;
    return i;
  }

  @Override
  public long getLong() {
    checkBounds(8);
    long l = ByteArrayUtil.getLong(buf, (int) position);
    position += 8;
    return l;
  }

  @Override
  public char getChar() {
    checkBounds(2);
    char c = ByteArrayUtil.getChar(buf, (int) position);
    position += 2;
    return c;
  }

  @Override
  public void setSize(long size) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void delete() {
    count = 0;
  }

  @Override
  public void shift(long offset) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void put(byte b) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void put(byte[] buf, int off, int len) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void put(byte[] buf) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void putShort(short s) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void putInt(int i) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void putLong(long l) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void putChar(char c) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void copyFrom(DataFile from, long size) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void replicateFrom(DataFile from) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void copyTo(DataFile target, long size) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void replicateTo(DataFile target) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
