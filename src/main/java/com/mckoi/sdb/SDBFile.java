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

package com.mckoi.sdb;

import com.mckoi.data.AddressableDataFile;
import com.mckoi.data.DataFile;

/**
 * An object that represents a file in the Simple Database API. A file
 * is a bounded collection of bytes that can grow and shrink in size, and
 * provides a pointer that can be positioned at any location in the file to
 * read and write data from the file content. Simple Database files provide
 * an efficient shift function for creating or deleting arbitrary areas of the
 * file. In addition, SDBFile objects can be efficiently 'shadow copied' by
 * using the 'copyTo' method.
 * <p>
 * Note that SDBFile borrows all of its functionality from an implementation of
 * com.mckoi.data.DataFile, therefore supports all the DataFile wrapping
 * classes in the com.mckoi.data package.
 *
 * @author Tobias Downer
 */

public class SDBFile implements AddressableDataFile {

  /**
   * The backed transaction.
   */
  private final SDBTransaction transaction;

  /**
   * The name of the file,
   */
  private final String file_name;

  /**
   * The DataFile object.
   */
  private final AddressableDataFile parent;

  /**
   * Set to true when the file is changed.
   */
  private boolean changed = false;

  /**
   * Constructor.
   */
  SDBFile(SDBTransaction transaction,
          String file_name, AddressableDataFile parent) {
    this.transaction = transaction;
    this.file_name = file_name;
    this.parent = parent;
  }

  /**
   * Adds a log entry that a change has been made to this file.
   */
  private void logChange() {
    if (changed) {
      // Already changed, so do nothing
    }
    else {
      transaction.logFileChange(file_name);
      changed = true;
    }
  }

  /**
   * Returns the name of this file.
   */
  public String getName() {
    return file_name;
  }

  // ----- Implemented from DataFile -----

  @Override
  public void copyFrom(DataFile from, long size) {
    transaction.checkValid();
    parent.copyFrom(from, size);
    logChange();
  }

  @Override
  public void replicateFrom(DataFile from) {
    transaction.checkValid();
    parent.replicateFrom(from);
    logChange();
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
  public Object getBlockLocationMeta(long start_position, long end_position) {
    // No transformation,
    return parent.getBlockLocationMeta(start_position, end_position);
  }

//  public void copyTo(DataFile target, long size) {
//    transaction.checkValid();
//    if (target instanceof SDBFile) {
//      SDBFile target_file = (SDBFile) target;
//      parent.copyTo(target_file.parent, size);
//      target_file.logChange();
//    }
//    else {
//      parent.copyTo(target, size);
//    }
//  }
//
//  public void replicateTo(DataFile target) {
//    transaction.checkValid();
//    if (target instanceof SDBFile) {
//      SDBFile target_file = (SDBFile) target;
//      parent.replicateTo(target_file.parent);
//      target_file.logChange();
//    }
//    else {
//      parent.replicateTo(target);
//    }
//  }

  @Override
  public void delete() {
    transaction.checkValid();
    logChange();
    parent.delete();
  }

  @Override
  public byte get() {
    transaction.checkValid();
    return parent.get();
  }

  @Override
  public void get(byte[] buf, int off, int len) {
    transaction.checkValid();
    parent.get(buf, off, len);
  }

  @Override
  public char getChar() {
    transaction.checkValid();
    return parent.getChar();
  }

  @Override
  public int getInt() {
    transaction.checkValid();
    return parent.getInt();
  }

  @Override
  public long getLong() {
    transaction.checkValid();
    return parent.getLong();
  }

  @Override
  public short getShort() {
    transaction.checkValid();
    return parent.getShort();
  }

  @Override
  public void position(long position) {
    transaction.checkValid();
    parent.position(position);
  }

  @Override
  public long position() {
    transaction.checkValid();
    return parent.position();
  }

  @Override
  public void put(byte b) {
    transaction.checkValid();
    logChange();
    parent.put(b);
  }

  @Override
  public void put(byte[] buf, int off, int len) {
    transaction.checkValid();
    logChange();
    parent.put(buf, off, len);
  }

  @Override
  public void put(byte[] buf) {
    transaction.checkValid();
    logChange();
    parent.put(buf);
  }

  @Override
  public void putChar(char c) {
    transaction.checkValid();
    logChange();
    parent.putChar(c);
  }

  @Override
  public void putInt(int i) {
    transaction.checkValid();
    logChange();
    parent.putInt(i);
  }

  @Override
  public void putLong(long l) {
    transaction.checkValid();
    logChange();
    parent.putLong(l);
  }

  @Override
  public void putShort(short s) {
    transaction.checkValid();
    logChange();
    parent.putShort(s);
  }

  @Override
  public void setSize(long size) {
    transaction.checkValid();
    logChange();
    parent.setSize(size);
  }

  @Override
  public void shift(long offset) {
    transaction.checkValid();
    logChange();
    parent.shift(offset);
  }

  @Override
  public long size() {
    transaction.checkValid();
    return parent.size();
  }

}
