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
 * A data file is a bounded sequence of bytes that is used to represent some
 * format of information in a database system.  DataFile is a flexible
 * structure in which the bounded area can be grown and shrunk, and bytes may
 * be inserted, removed, shifted, copied, read and written at abritrary
 * locations in the sequence.
 * <p>
 * Data access and mutation is managed via a pointer that may be freely
 * moved over the byte sequence using the 'position' method. Most operations
 * move this pointer forward. For example, the 'putLong' method moves the
 * pointer forward by 8 bytes.  Adding data when the pointer is at the end of
 * a DataFile causes the bounded space to grow to accommodate the addition.
 * <p>
 * Implementations of this class should be considered light weight and
 * efficient for all operations.
 * 
 * @author Tobias Downer
 */

public interface DataFile {

  /**
   * Returns the size of the data file address space in bytes.
   */
  long size();

  /**
   * Changes the position of the pointer within the data sequence address space,
   * where position(0) moves to the first location in the sequence.
   */
  void position(long position);

  /**
   * Returns the current pointer location.
   */
  long position();

  // ---------- The get methods ----------
  // Note that these methods will all increment the position by the size of the
  // element read.  For example, 'getInt' will increment the position by 4.

  /**
   * Returns the byte at the current position and forwards the position pointer
   * by 1.
   */
  byte get();

  /**
   * Fills the given byte[] array with 'len' number of bytes at the current
   * position and forwards the position pointer by len.  The bytes are filled
   * at 'off' offset within the byte[] array.  If there is not enough data
   * available in the data file to fulfill the request a RuntimeException is
   * generated.
   */
  void get(byte[] buf, int off, int len);
  
  /**
   * Returns the short value (16 bits) at the current position and forwards
   * the position pointer by 2.
   */
  short getShort();
  
  /**
   * Returns the integer value (32 bits) at the current position and forwards
   * the position pointer by 4.
   */
  int getInt();
  
  /**
   * Returns the long value (64 bits) at the current position and forwards
   * the position pointer by 8.
   */
  long getLong();
  
  /**
   * Returns the unicode character value (16 bits) at the current position and
   * forwards the position pointer by 2.
   */
  char getChar();

//  /**
//   * Returns an immutable subset of this data file.  The returned object is
//   * guaranteed not to change regardless of calls to the mutation methods in
//   * this object.  The returned DataFile is created by copying all or parts of
//   * this data file into memory so this method requires memory use, however,
//   * it is usually a much more efficient way to create a copy of a data file
//   * than by copying the content into another structure.
//   * <p>
//   * A situation where it is useful to use this would be in the creation of a
//   * subset of an index where the data must stay stable and static even when
//   * other operations may be changing the index.
//   * <p>
//   * @param p1 the start point (inclusive)
//   * @param p2 the end point (exclusive)
//   */
//  DataFile getImmutableSubset(long p1, long p2) throws IOException;

  // ---------- General write methods ----------

  /**
   * Sets the size of the file.  If the size set is smaller than the current
   * size then the file is truncated.  If the size is greater than the
   * current size then extra space is added to the end of the file.  We
   * do not define the content of the extra space created in a file by this
   * way.
   */
  void setSize(long size);

  /**
   * Deletes all data in the file.  When this returns the size of the file
   * will be 0.
   */
  void delete();

  /**
   * Shifts all the data after the position location by the given offset.  A
   * negative value will shift all the data backward and reduce the size of
   * the file.  A positive value will shift all the data forward and increase
   * the size of the file.  When shifting forward, the content of the space
   * between 'position' and 'position + shift' is not defined by this contract.
   * <p>
   * The position location is not changed after using this operation.
   * <p>
   * This method is intended to be used when there is a need to insert data
   * or remove data before the end of the file.  It is expected that
   * implementations of this method are able to efficiently shift large
   * magnitudes of data.  For example, shifting 10 MB of data forward by 8
   * bytes should be efficient.
   */
  void shift(long offset);

  /**
   * Writes a byte at the current position and forwards the position pointer
   * by 1.  Any existing data at the position is overwritten.  If the position
   * location is at the end of the file, the size is increased by the size of
   * the data being written.
   */
  void put(byte b);

  /**
   * Writes a length 'len' of the byte[] array starting at offset 'off' at
   * the current position and moves the position pointer forward by 'len'. Any
   * existing data in the file past the current position is overwritten by this
   * operation (up to the amount of data written). If, during the write, the
   * position extends past the end of the file, the size of the file is
   * increased to make room of the data being written.
   */
  void put(byte[] buf, int off, int len);

  /**
   * Writes the entire byte[] array at the current position and moves the
   * position pointer forward by the size of the array. Any
   * existing data in the file past the current position is overwritten by this
   * operation (up to the amount of data written). If, during the write, the
   * position extends past the end of the file, the size of the file is
   * increased to make room of the data being written.
   */
  void put(byte[] buf);

  /**
   * Writes a short value (16 bits) at the current position and forwards the
   * position pointer by 2.  Any existing data at the position is overwritten.
   * If the position location is at the end of the file, the size is increased
   * by the size of the data being written.
   */
  void putShort(short s);

  /**
   * Writes an integer value (32 bits) at the current position and forwards the
   * position pointer by 4.  Any existing data at the position is overwritten.
   * If the position location is at the end of the file, the size is increased
   * by the size of the data being written.
   */
  void putInt(int i);

  /**
   * Writes a long value (64 bits) at the current position and forwards the
   * position pointer by 8.  Any existing data at the position is overwritten.
   * If the position location is at the end of the file, the size is increased
   * by the size of the data being written.
   */
  void putLong(long l);

  /**
   * Writes a character value (16 bits) at the current position and forwards the
   * position pointer by 2.  Any existing data at the position is overwritten.
   * If the position location is at the end of the file, the size is increased
   * by the size of the data being written.
   */
  void putChar(char c);



  /**
   * Copies 'size' amount of data from the current position in the 'from' file
   * to this DataFile at its position location.  If there
   * is less data remaining in this file than 'size', then only the remaining
   * data available is copied.  Any data in this DataFile past its position
   * is shifted forward by the amount of data that is being copied from
   * the target.
   * <p>
   * The 'from' DataFile may be a file in the same transaction as this DataFile
   * or it may be a file in a different transaction, or even a file in another
   * KeyObjectDatabase.  However, the given DataFile may <b>NOT</b> be the
   * same file as this or an instantiated file with the same key from the same
   * transaction.  In other words, you can not use this to move data inside
   * one DataFile object in one transaction.
   * <p>
   * This copy routine may employ any number of optimization tricks.  For
   * example, data copied to a file in a different transaction but in the same
   * database may just need to do some meta tree manipulation operations and
   * reference updates.
   * <p>
   * It is intended for this operation to be a versatile and efficient
   * way to merge data between transactions.  For example, provided no
   * consistency checks fail, we may need to perform a series of copy operations
   * on data from a transaction several versions before the current
   * transaction to update the latest transaction version.
   * <p>
   * This method also provides us a way to express to the data storage system
   * a form of compression of datasets that contain near identical information
   * repeated with slight variations.  Used appropriately, this may improve
   * performance by increasing cache hits and reducing disk reads.
   * <p>
   * When this method returns, the position location in both the source and
   * target will point to the end of the copied sequence.
   */
  void copyFrom(DataFile from, long size);

  /**
   * Replaces the entire contents of this file with the content of the given
   * file in entirety. When this method returns, the target DataFile will
   * be the same size and contain the same data as this file. This is the
   * same as following code sequence;
   * 'this.delete(); from.position(0); this.copyFrom(from, from.size())'
   * <p>
   * Like 'copyFrom', this may employ a number of optimization tricks to
   * perform the operation efficiently, including not changing anything at
   * all if the contents are determined to be the same.
   * <p>
   * It is intended that this operation looks for the changes between this
   * and the target and only copies the meta-data that has changed. This leads
   * to efficient replication of large content.
   * <p>
   * When this method returns, the position location in both the source and
   * destination will point to the end of the content.
   */
  void replicateFrom(DataFile from);

  /**
   * Legacy method - 'copyFrom' with reversed targets. This should always be
   * implemented as 'target.copyFrom(this, size)'
   */
  void copyTo(DataFile target, long size);

  /**
   * Legacy method - 'replicateFrom' with reversed targets. This should always
   * be implemented as 'target.replicateFrom(this)'
   */
  void replicateTo(DataFile target);

}
