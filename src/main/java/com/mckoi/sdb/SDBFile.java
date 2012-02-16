/**
 * com.mckoi.sdb.SDBFile  Jul 31, 2009
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

public class SDBFile implements AddressableDataFile, SDBTrustedObject {

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
      return;
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
