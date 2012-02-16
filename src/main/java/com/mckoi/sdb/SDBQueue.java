/**
 * com.mckoi.sdb.SDBQueue  Jul 30, 2010
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

import com.mckoi.data.DataFile;

/**
 * 
 *
 * @author Tobias Downer
 */

public class SDBQueue { //implements SDBTrustedObject, DataFile {

//  /**
//   * The backed transaction.
//   */
//  private final SDBTransaction transaction;
//
//  /**
//   * The name of the queue,
//   */
//  private final String queue_name;
//
//  /**
//   * The DataFile object.
//   */
//  private final DataFile parent;
//
//  /**
//   * The size of the additions made to the queue since started.
//   */
//  private long end_expanded = 0;
//  private long start_consumed = 0;
//
//  /**
//   * Set to true when the transaction notified that a change has been made
//   * to this queue.
//   */
//  private boolean change_notified = false;
//
//
//  /**
//   * Constructor.
//   */
//  SDBQueue(SDBTransaction transaction, String queue_name, DataFile parent) {
//    this.transaction = transaction;
//    this.queue_name = queue_name;
//    this.parent = parent;
//  }
//
//
//  /**
//   * Adds a log entry that a change has been made to expand the queue at the
//   * end.
//   */
//  private void logExpanded(long delta) {
//    // No change, so return
//    if (delta == 0) {
//      return;
//    }
//    if (!change_notified) {
//      transaction.logQueueChange(queue_name);
//      change_notified = true;
//    }
//    end_expanded += delta;
//  }
//
//  /**
//   * Adds a log entry that a change has been made to consume the queue from the
//   * from.
//   */
//  private void logConsumed(long delta) {
//    // No change, so return
//    if (delta == 0) {
//      return;
//    }
//    if (!change_notified) {
//      transaction.logQueueChange(queue_name);
//      change_notified = true;
//    }
//    start_consumed -= delta;
//  }
//
//  /**
//   * Checks the current position is valid to write to.
//   */
//  private void checkPositionValidForWrite() {
//    if (position() != size()) {
//      throw new IndexOutOfBoundsException(
//                                 "Position must be at end of queue to write");
//    }
//  }
//
//  /**
//   * Returns an exception indicating internal queue modification not permitted.
//   */
//  private IllegalStateException modificationException() {
//    throw new IllegalStateException(
//                              "Internal modification of queue not permitted");
//  }
//
//  // ----- Implemented from DataFile -----
//
//  public long size() {
//    return parent.size();
//  }
//
//  public void shift(long offset) {
//    // Only supports shifting positive from the end
//    long pos = position();
//    long sz = size();
//    // If at end,
//    if (pos == sz) {
//      // If positive then we expanding so call is ok
//      if (offset < 0) {
//        throw modificationException();
//      }
//      // OK, pos at end and offset >= 0 meaning the queue is being expanded,
//      logExpanded(offset);
//      parent.shift(offset);
//      return;
//    }
//    else if (pos >= sz - end_expanded) {
//      throw modificationException();
//    }
//    else if (pos + offset != 0) {
//      // Not ok, not deleting from start
//      throw modificationException();
//    }
//    // Ok, pos < (sz - end_expanded) and (pos + offset) == 0.
//    // This means we are shrinking the queue from the front.
//    logConsumed(offset);
//    parent.shift(offset);
//  }
//
//  public void setSize(long size) {
//    // Only allowed to expand the file,
//    long sz = size();
//    if (size < sz) {
//      throw modificationException();
//    }
//    // Checks ok,
//    logExpanded(size - sz);
//    parent.setSize(size);
//  }
//
//  public void putShort(short s) {
//    checkPositionValidForWrite();
//    logExpanded(2);
//    parent.putShort(s);
//  }
//
//  public void putLong(long l) {
//    checkPositionValidForWrite();
//    logExpanded(8);
//    parent.putLong(l);
//  }
//
//  public void putInt(int i) {
//    checkPositionValidForWrite();
//    logExpanded(4);
//    parent.putInt(i);
//  }
//
//  public void putChar(char c) {
//    checkPositionValidForWrite();
//    logExpanded(2);
//    parent.putChar(c);
//  }
//
//  public void put(byte[] buf) {
//    checkPositionValidForWrite();
//    logExpanded(buf.length);
//    parent.put(buf);
//  }
//
//  public void put(byte[] buf, int off, int len) {
//    checkPositionValidForWrite();
//    logExpanded(len);
//    parent.put(buf, off, len);
//  }
//
//  public void put(byte b) {
//    checkPositionValidForWrite();
//    logExpanded(1);
//    parent.put(b);
//  }
//
//  public long position() {
//    return parent.position();
//  }
//
//  public void position(long position) {
//    parent.position(position);
//  }
//
//  public short getShort() {
//    return parent.getShort();
//  }
//
//  public long getLong() {
//    return parent.getLong();
//  }
//
//  public int getInt() {
//    return parent.getInt();
//  }
//
//  public char getChar() {
//    return parent.getChar();
//  }
//
//  public void get(byte[] buf, int off, int len) {
//    parent.get(buf, off, len);
//  }
//
//  public byte get() {
//    return parent.get();
//  }
//
//  public void delete() {
//    // Only if nothing written
//    if (end_expanded != 0) {
//      throw modificationException();
//    }
//    logExpanded(-size());
//    parent.delete();
//  }
//
//  public void copyTo(DataFile target, long size) {
//    // This can be safely delegated.
//    parent.copyTo(target, size);
//  }
//
//  public void replicateTo(DataFile target) {
//    // This can be safely delegated.
//    parent.replicateTo(target);
//  }

}
