/**
 * com.mckoi.odb.Data  Feb 17, 2011
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

package com.mckoi.odb;

import com.mckoi.data.AddressableDataFile;
import com.mckoi.data.DataFile;

/**
 * An implementation of ODBData.
 *
 * @author Tobias Downer
 */

class Data implements ODBData {

  private ODBTransaction transaction;

  private Reference reference;

  private AddressableDataFile data_file;

  private boolean updated;

  /**
   * Constructor.
   */
  Data(ODBTransaction transaction,
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

  public ODBClass getODBClass() {
    return null;
  }

  public Reference getReference() {
    return reference;
  }

  // ------

  public long size() {
    return data_file.size();
  }

  public void shift(long offset) {
    logUpdated();
    data_file.shift(offset);
  }

  public void setSize(long size) {
    logUpdated();
    data_file.setSize(size);
  }

  public void replicateTo(DataFile target) {
    target.replicateFrom(this);
  }

  public void replicateFrom(DataFile from) {
    logUpdated();
    data_file.replicateFrom(from);
  }

  public void putShort(short s) {
    logUpdated();
    data_file.putShort(s);
  }

  public void putLong(long l) {
    logUpdated();
    data_file.putLong(l);
  }

  public void putInt(int i) {
    logUpdated();
    data_file.putInt(i);
  }

  public void putChar(char c) {
    logUpdated();
    data_file.putChar(c);
  }

  public void put(byte[] buf) {
    logUpdated();
    data_file.put(buf);
  }

  public void put(byte[] buf, int off, int len) {
    logUpdated();
    data_file.put(buf, off, len);
  }

  public void put(byte b) {
    logUpdated();
    data_file.put(b);
  }

  public long position() {
    return data_file.position();
  }

  public void position(long position) {
    data_file.position(position);
  }

  public short getShort() {
    return data_file.getShort();
  }

  public long getLong() {
    return data_file.getLong();
  }

  public int getInt() {
    return data_file.getInt();
  }

  public char getChar() {
    return data_file.getChar();
  }

  public void get(byte[] buf, int off, int len) {
    data_file.get(buf, off, len);
  }

  public byte get() {
    return data_file.get();
  }

  public void delete() {
    logUpdated();
    data_file.delete();
  }

  public void copyTo(DataFile target, long size) {
    target.copyFrom(this, size);
  }

  public void copyFrom(DataFile from, long size) {
    logUpdated();
    data_file.copyFrom(from, size);
  }

  public Object getBlockLocationMeta(long start_position, long end_position) {
    return data_file.getBlockLocationMeta(start_position, end_position);
  }

}
