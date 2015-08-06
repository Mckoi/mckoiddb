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
