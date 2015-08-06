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

/**
 * An implementation of AddressableDataFile that simply delegates all methods
 * through to the parent. The intention of this is to protect the input
 * data file from being cast and possibly exposing secure information.
 *
 * @author Tobias Downer
 */

public class DelegateAddressableDataFile implements AddressableDataFile {

  private final AddressableDataFile backed;
  
  /**
   * Constructor.
   */
  public DelegateAddressableDataFile(AddressableDataFile data_file) {
    this.backed = data_file;
  }

  @Override
  public long size() {
    return backed.size();
  }

  @Override
  public void shift(long offset) {
    backed.shift(offset);
  }

  @Override
  public void setSize(long size) {
    backed.setSize(size);
  }

  @Override
  public void replicateTo(DataFile target) {
    target.replicateFrom(this);
  }

  @Override
  public void replicateFrom(DataFile from) {
    backed.replicateFrom(from);
  }

  @Override
  public void putShort(short s) {
    backed.putShort(s);
  }

  @Override
  public void putLong(long l) {
    backed.putLong(l);
  }

  @Override
  public void putInt(int i) {
    backed.putInt(i);
  }

  @Override
  public void putChar(char c) {
    backed.putChar(c);
  }

  @Override
  public void put(byte[] buf) {
    backed.put(buf);
  }

  @Override
  public void put(byte[] buf, int off, int len) {
    backed.put(buf, off, len);
  }

  @Override
  public void put(byte b) {
    backed.put(b);
  }

  @Override
  public long position() {
    return backed.position();
  }

  @Override
  public void position(long position) {
    backed.position(position);
  }

  @Override
  public short getShort() {
    return backed.getShort();
  }

  @Override
  public long getLong() {
    return backed.getLong();
  }

  @Override
  public int getInt() {
    return backed.getInt();
  }

  @Override
  public char getChar() {
    return backed.getChar();
  }

  @Override
  public void get(byte[] buf, int off, int len) {
    backed.get(buf, off, len);
  }

  @Override
  public byte get() {
    return backed.get();
  }

  @Override
  public void delete() {
    backed.delete();
  }

  @Override
  public void copyTo(DataFile target, long size) {
    target.copyFrom(this, size);
  }

  @Override
  public void copyFrom(DataFile from, long size) {
    backed.copyFrom(from, size);
  }

  @Override
  public Object getBlockLocationMeta(long start_position, long end_position) {
    return backed.getBlockLocationMeta(start_position, end_position);
  }

}
