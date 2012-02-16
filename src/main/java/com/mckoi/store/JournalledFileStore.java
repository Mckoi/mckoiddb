/**
 * com.mckoi.store.ScatteringFileStore  24 Jan 2003
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

package com.mckoi.store;

import java.io.IOException;

/**
 * An implementation of AbstractStore that persists to an underlying data
 * format via a robust journalling system that supports check point and crash
 * recovery.  Note that this object is a bridge between the Store API and the
 * journalled behaviour defined in LoggingBufferManager, JournalledSystem and
 * the StoreDataAccessor implementations.
 * <p>
 * Note that access to the resources is abstracted via a 'resource_name'
 * string.  The LoggingBufferManager object converts the resource name into a
 * concrete object that accesses the actual data.
 *
 * @author Tobias Downer
 */

public final class JournalledFileStore extends AbstractStore {

  /**
   * The name of the resource.
   */
  private final String resource_name;
  
  /**
   * The buffering strategy for accessing the data in an underlying file.
   */
  private final LoggingBufferManager buffer_manager;

  /**
   * The JournalledResource object that's used to journal all read/write
   * operations to the above 'store_accessor'.
   */
  private JournalledResource store_resource;
  
  
  /**
   * Constructs the ScatteringFileStore.
   */
  public JournalledFileStore(String resource_name,
                             LoggingBufferManager buffer_manager,
                             boolean read_only) {
    super(read_only);
    this.resource_name = resource_name;
    this.buffer_manager = buffer_manager;

    // Create the store resource object for this resource name
    this.store_resource = buffer_manager.createResource(resource_name);
  }


  // ---------- JournalledFileStore methods ----------

  /**
   * Deletes this store from the file system.  This operation should only be
   * used when the store is NOT open.
   */
  public boolean delete() throws IOException {
    store_resource.delete();
    return true;
  }

  /**
   * Returns true if this store exists in the file system.
   */
  public boolean exists() throws IOException {
    return store_resource.exists();
  }

  public void lockForWrite() {
    try {
      buffer_manager.lockForWrite();
    }
    catch (InterruptedException e) {
      throw new Error("Interrupted: " + e.getMessage());
    }
  }

  public void unlockForWrite() {
    buffer_manager.unlockForWrite();
  }
  
  public void checkPoint() throws InterruptedException, IOException {
    // We don't flush the log (it's only necessary to flush the log for small
    // updates such as initialization procedures).
    buffer_manager.setCheckPoint(false);
  }
  
  // ---------- Implemented from AbstractStore ----------
  
  /**
   * Internally opens the backing area.  If 'read_only' is true then the
   * store is opened in read only mode.
   */
  protected void internalOpen(boolean read_only) throws IOException {
    store_resource.open(read_only);
  }
  
  /**
   * Internally closes the backing area.
   */
  protected void internalClose() throws IOException {
    store_resource.close();
  }


  protected int readByteFrom(long position) throws IOException {
    return buffer_manager.readByteFrom(store_resource, position);
  }
  
  protected int readByteArrayFrom(long position,
                           byte[] buf, int off, int len) throws IOException {
    return buffer_manager.readByteArrayFrom(store_resource,
                                            position, buf, off, len);
  }
  
  protected void writeByteTo(long position, int b) throws IOException {
    buffer_manager.writeByteTo(store_resource, position, b);
  }

  protected void writeByteArrayTo(long position,
                           byte[] buf, int off, int len) throws IOException {
    buffer_manager.writeByteArrayTo(store_resource,
                                    position, buf, off, len);
  }

  protected long endOfDataAreaPointer() throws IOException {
    return buffer_manager.getDataAreaSize(store_resource);
  }

  protected void setDataAreaSize(long new_size) throws IOException {
    buffer_manager.setDataAreaSize(store_resource, new_size);
  }

  // For diagnosis
  
  public String toString() {
    return "[ JournalledFileStore: " + resource_name + " ]";
  }
  
}

