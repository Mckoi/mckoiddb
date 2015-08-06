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

package com.mckoi.store;

//import java.util.List;
import java.io.IOException;
import java.io.InputStream;

/**
 * A store is a resource where areas can be allocated and freed to store
 * information (a memory allocator).  A store can be backed by a file system or
 * main memory, or any type of information storage mechanism that allows the
 * creation, modification and fast lookup of blocks of information.
 * <p>
 * Some characteristics of implementations of Store may be separately
 * specified.  For example, a file based store that is intended to persistently
 * store objects may have robustness as a primary requirement.  A main memory
 * based store, or another type of volatile storage system, may not need to be
 * sensitive to system crashes or data consistancy requirements across multiple
 * sessions.
 * <p>
 * Some important assumptions for implementations; The data must not be
 * changed in any way outside of the methods provided by the methods in the
 * class. For persistant implementations, the information must remain the same
 * over invocations, however its often not possible to guarantee this.  At
 * least, the store should be able to recover to the last check point.
 * <p>
 * This interface is the principle class to implement when porting the database
 * to different types of storage devices.
 * <p>
 * Note that we use 'long' identifiers to reference areas in the store however
 * only the first 60 bits of the of an identifer will be used unless we are
 * referencing system (the static area is -1) or implementation specific areas.
 *
 * @author Tobias Downer
 */

public interface Store {

  /**
   * Allocates a block of memory in the store of the specified size and returns
   * an AreaWriter object that can be used to initialize the contents of the
   * area.  Note that an area in the store is undefined until the 'finish'
   * method is called in AreaWriter.
   *
   * @param size the amount of memory to allocate.
   * @return an AreaWriter object that allows the area to be setup.
   * @throws IOException if not enough space available to create the area or
   *   the store is read-only.
   */
  AreaWriter createArea(long size) throws IOException;

  /**
   * Deletes an area that was previously allocated by the 'createArea' method
   * by the area id.  Once an area is deleted the resources may be reclaimed.
   * The behaviour of this method is undefined if the id doesn't represent a
   * valid area.
   *
   * @param id the identifier of the area to delete.
   * @throws IOException (optional) if the id is invalid or the area can not
   *   otherwise by deleted.
   */
  void deleteArea(long id) throws IOException;

  /**
   * Returns an InputStream implementation that allows for the area with the
   * given identifier to be read sequentially.  The behaviour of this method,
   * and InputStream object, is undefined if the id doesn't represent a valid
   * area.
   * <p>
   * When 'id' is -1 then a fixed static area (64 bytes in size) in the store
   * is returned.  The fixed area can be used to store important static
   * information.
   *
   * @param id the identifier of the area to read, or id = -1 is a 64 byte
   *   fixed area in the store.
   * @return an InputStream that allows the area to be read from the start.
   * @throws IOException (optional) if the id is invalid or the area can not
   *   otherwise be accessed.
   */
  InputStream getAreaInputStream(long id) throws IOException;
  
  /**
   * Returns an object that allows for the contents of an area (represented by
   * the 'id' parameter) to be read.  The behaviour of this method, and Area
   * object, is undefined if the id doesn't represent a valid area.
   * <p>
   * When 'id' is -1 then a fixed area (64 bytes in size) in the store is
   * returned.  The fixed area can be used to store important static
   * information.
   *
   * @param id the identifier of the area to read, or id = -1 for a 64 byte
   *   fixed area in the store.
   * @return an Area object that allows access to the part of the store.
   * @throws IOException (optional) if the id is invalid or the area can not
   *   otherwise be accessed.
   */
  Area getArea(long id) throws IOException;

  /**
   * Returns an object that allows for the contents of an area (represented by
   * the 'id' parameter) to be read and written.  The behaviour of this method,
   * and MutableArea object, is undefined if the id doesn't represent a valid
   * area.
   * <p>
   * When 'id' is -1 then a fixed area (64 bytes in size) in the store is
   * returned.  The fixed area can be used to store important static
   * information.
   *
   * @param id the identifier of the area to access, or id = -1 is a 64 byte
   *   fixed area in the store.
   * @return a MutableArea object that allows access to the part of the store.
   * @throws IOException (optional) if the id is invalid or the area can not
   *   otherwise be accessed.
   */
  MutableArea getMutableArea(long id) throws IOException;

  // ---------- Check Point Locking ----------

  /**
   * This method is called before the start of a sequence of write commands
   * between consistant states of some data structure represented by the store.
   * This lock mechanism is intended to inform the store when it is not safe to
   * 'checkpoint' the data in a log, ensuring that no partial updates are
   * committed to a transaction log and the data can be restored in a
   * consistant manner.
   * <p>
   * If the store does not implement a check point log or is otherwise not
   * interested in consistant states of the data, then it is not necessary for
   * this method to do anything.
   * <p>
   * This method prevents a check point from happening during some sequence of
   * operations.  This method should not lock unless a check point is in
   * progress.  This method does not prevent concurrent writes to the store.
   */
  void lockForWrite();

  /**
   * This method is called after the end of a sequence of write commands
   * between consistant states of some data structure represented by the store.
   * See the 'lockForWrite' method for a further description of the operation
   * of this locking mechanism.
   */
  void unlockForWrite();
  
  /**
   * Check point all the updates on this store up to the current time.  When
   * this method returns, there is an implied guarantee that when the store is
   * next invocated that at least the data written to the store up to this
   * point is available from the store.
   * <p>
   * This method will block if there is a write lock on the store (see
   * 'lockForWrite').
   * <p>
   * If the implented store is not interested in maintaining the consistancy of
   * the information between invocations then it is not necessary for this
   * method to do anything.
   * 
   * @throws InterruptedException if check point interrupted (should only
   *     happen under exceptional circumstances).
   * @throws IOException if check point failed because of an IO error.
   */
  void checkPoint() throws InterruptedException, IOException;

  // ---------- Diagnostic ----------

  /**
   * Returns true if the store was closed cleanly.  This is important
   * information that may need to be considered when reading information from
   * the store.  This is typically used to issue a scan on the data in the
   * store when it is not closed cleanly.
   */
  boolean lastCloseClean();

//  /**
//   * Returns a complete list of pointers to all areas in the Store as Long
//   * objects sorted from lowest pointer to highest.  This should be used for
//   * diagnostics only because it may be difficult for this to be generated
//   * with some implementations.  It is useful in a repair tool to determine if
//   * a pointer is valid or not.
//   */
//  List getAllAreas() throws IOException;
  
}
