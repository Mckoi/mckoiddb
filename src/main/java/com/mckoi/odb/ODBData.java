/**
 * com.mckoi.odb.ODBData  Aug 2, 2010
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

package com.mckoi.odb;

import com.mckoi.data.AddressableDataFile;
import com.mckoi.data.DataFile;

/**
 * Represents a mutable data object referenced within the Mckoi Object
 * Database data model.
 *
 * @author Tobias Downer
 */

public interface ODBData extends AddressableDataFile, ODBReferenced {

  @Override
  long size();

  @Override
  void position(long position);

  @Override
  long position();

  @Override
  byte get();

  @Override
  void get(byte[] buf, int off, int len);

  @Override
  short getShort();

  @Override
  int getInt();

  @Override
  long getLong();

  @Override
  char getChar();

  @Override
  void setSize(long size);

  @Override
  void delete();

  @Override
  void shift(long offset);

  @Override
  void put(byte b);

  @Override
  void put(byte[] buf, int off, int len);

  @Override
  void put(byte[] buf);

  @Override
  void putShort(short s);

  @Override
  void putInt(int i);

  @Override
  void putLong(long l);

  @Override
  void putChar(char c);

  @Override
  void copyFrom(DataFile from, long size);

  @Override
  void replicateFrom(DataFile from);

}
