/**
 * com.mckoi.data.DelegateAddressableDataFile  Mar 5, 2012
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
