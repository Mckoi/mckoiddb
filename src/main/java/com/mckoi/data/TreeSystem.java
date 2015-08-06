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

import java.io.IOException;

/**
 * An extended KeyObjectDatabase interface that additionally encapsulates the
 * notion of pieces of data and meta data stored in 'TreeNode' objects. This
 * interface is used to build the backbone framework of a database on
 * which snapshots of the database are managed by the TreeSystemTransaction
 * object.
 * <p>
 * For a reference implementation, see StoreBackedTreeSystem.
 * <p>
 * Implementing this object provides a versatile way of managing version
 * control and persistence. It is not intended for the methods exposed by this
 * interface to be accessed by user code.
 *
 * @author Tobias Downer
 */

public interface TreeSystem {

  /**
   * Returns the max branch size.
   */
  int getMaxBranchSize();

  /**
   * Returns the maximum number of bytes in a leaf.
   */
  int getMaxLeafByteSize();

  // ----- Implementation features -----

  /**
   * Returns true if the implementation requires that the system must notify
   * TreeSystem for every node inserted and deleted through the 'disposeNode'
   * and 'linkLeaf' method. If this returns false, the system may still call
   * 'linkLeaf' and 'disposeNode' but it will not be accurate and the calls
   * should be ignored.
   * <p>
   * Returning false is an optimization for systems that support an offline
   * method for accounting for node data (such as a garbage collector) for
   * resource reclamation.
   */
  boolean featureAccountForAllNodes();

  // ----- Critical stop error handling -----

  /**
   * Checks if the database is in a stop state, if it is throws the stop state
   * exception.
   */
  void checkCriticalStop();
 
  /**
   * Called by an exception handler when an IOException is generated, most
   * typically this is a stopping condition that stops all access to the
   * database immediately.
   */
  CriticalStopError handleIOException(IOException e);

  /**
   * Called by an exception handler when a VirtualMachineError is generated,
   * most typically an OutOfMemoryError.  A caught VirtualMachineError
   * causes the database to enter a critical stop state.
   */
  CriticalStopError handleVMError(VirtualMachineError e);

//  // ----- Atomic objects -----
//
//  /**
//   * Fetches the atomic data record assigned the given identifier.
//   */
//  AtomicData getAtomicData(AtomicKey atomic_id);
//
//  /**
//   * Removes the atomic data record assigned the given identifier.
//   */
//  void removeAtomicData(AtomicKey atomic_id);

  // ----- General managements -----
  
  /**
   * Performs a check point operation, flushing any data stored in
   * caches out to the underlying storage mechanism. This is called
   * by the cache management system after nodes stored in memory were
   * flushed to disk because a threshold of memory use has been met.
   */
  void checkPoint();

//  // ----- Diagnostics -----
//
//  TreeReportNode createDiagnosticGraph() throws IOException;

  // ---------- Node management methods ----------

  /**
   * Returns the maximum size of the local transaction node heaps.
   */
  long getNodeHeapMaxSize();

  /**
   * Fetches an immutable node kept in the tree at the given node reference.
   * If (node_ref & 0x01000000000000000L != 0) then the node is considered a
   * special node (eg, a sparse node).
   */
  TreeNode[] fetchNode(NodeReference[] node_ref) throws IOException;

  /**
   * Returns true if the given node ref is currently cached, false otherwise.
   */
  boolean isNodeAvailableLocally(NodeReference node_ref);

  // ----- Node mutation -----

  /**
   * Performs the sequence of node write operations described by the given
   * TreeWriteSequence object. This is used to flush a complete tree write
   * operation out to the backing store. Returns an array of node_ref 64-bit
   * values that represent the address of every node written to the backing
   * media on the completion of the process.
   */
  NodeReference[] performTreeWrite(TreeWriteSequence sequence)
                                                            throws IOException;

  /**
   * Notifies that a shadow link has been created to the leaf node with the
   * given reference. A shadow link is a reference from a branch to a leaf
   * that is already linked to from another branch.
   * <p>
   * The number of 'disposeNode' operations needed to make a leaf node
   * eligible for reclamation is dependent on the number of shadow links
   * established on the leaf. If the implementation supports reference
   * counting, then this method should increment the reference count on the
   * leaf node, and 'disposeNode' should decrement the reference count.
   * Assuming a newly written node starts with a reference count of 1, once
   * the reference count is 0 the node resources can be reclaimed.
   * <p>
   * Returns true if establishing the shadow link was successful, false if the
   * shadow link was not possible either because the reference count
   * reached max capacity or shadow linking is not permitted.
   */
  boolean linkLeaf(Key key, NodeReference ref) throws IOException;

  /**
   * Called by TreeSystemTransaction when a transaction is disposed without
   * being committed, and this node was created or shadow linked to during
   * the operation of the transaction. This method should therefore modify
   * the reference count (if applicable) and reclaim the resources if it's
   * safe to do so.
   */
  void disposeNode(NodeReference ref) throws IOException;

}
