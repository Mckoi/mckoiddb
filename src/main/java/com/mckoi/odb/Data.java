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

package com.mckoi.odb;

import com.mckoi.data.AddressableDataFile;
import com.mckoi.data.DataFile;

/**
 * An implementation of ODBData.
 *
 * @author Tobias Downer
 */

class Data implements ODBData {

  private ODBTransactionImpl transaction;

  private Reference reference;

  private AddressableDataFile data_file;

  private boolean updated;

  /**
   * Constructor.
   */
  Data(ODBTransactionImpl transaction,
       Reference reference, AddressableDataFile data_file) {
    this.transaction = transaction;
    this.reference = reference;
    this.data_file = data_file;
    this.updated = false;
  }

  /**
   * Called whenever the data is mutated.
   */
  private void logUpdated() {
    if (!updated) {
      transaction.getObjectLog().logDataChange(reference);
      updated = true;
    }
  }

  // ------

  @Override
  public ODBClass getODBClass() {
    return null;
  }

  @Override
  public Reference getReference() {
    return reference;
  }

  // ------

  @Override
  public long size() {
    return data_file.size();
  }

  @Override
  public void shift(long offset) {
    logUpdated();
    data_file.shift(offset);
  }

  @Override
  public void setSize(long size) {
    logUpdated();
    data_file.setSize(size);
  }

  @Override
  public void replicateTo(DataFile target) {
    target.replicateFrom(this);
  }

  @Override
  public void replicateFrom(DataFile from) {
    logUpdated();
    data_file.replicateFrom(from);
  }

  @Override
  public void putShort(short s) {
    logUpdated();
    data_file.putShort(s);
  }

  @Override
  public void putLong(long l) {
    logUpdated();
    data_file.putLong(l);
  }

  @Override
  public void putInt(int i) {
    logUpdated();
    data_file.putInt(i);
  }

  @Override
  public void putChar(char c) {
    logUpdated();
    data_file.putChar(c);
  }

  @Override
  public void put(byte[] buf) {
    logUpdated();
    data_file.put(buf);
  }

  @Override
  public void put(byte[] buf, int off, int len) {
    logUpdated();
    data_file.put(buf, off, len);
  }

  @Override
  public void put(byte b) {
    logUpdated();
    data_file.put(b);
  }

  @Override
  public long position() {
    return data_file.position();
  }

  @Override
  public void position(long position) {
    data_file.position(position);
  }

  @Override
  public short getShort() {
    return data_file.getShort();
  }

  @Override
  public long getLong() {
    return data_file.getLong();
  }

  @Override
  public int getInt() {
    return data_file.getInt();
  }

  @Override
  public char getChar() {
    return data_file.getChar();
  }

  @Override
  public void get(byte[] buf, int off, int len) {
    data_file.get(buf, off, len);
  }

  @Override
  public byte get() {
    return data_file.get();
  }

  @Override
  public void delete() {
    logUpdated();
    data_file.delete();
  }

  @Override
  public void copyTo(DataFile target, long size) {
    target.copyFrom(this, size);
  }

  @Override
  public void copyFrom(DataFile from, long size) {
    logUpdated();
    data_file.copyFrom(from, size);
  }

  @Override
  public Object getBlockLocationMeta(long start_position, long end_position) {
    return data_file.getBlockLocationMeta(start_position, end_position);
  }

}
