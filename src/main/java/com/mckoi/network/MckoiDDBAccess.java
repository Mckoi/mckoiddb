/**
 * com.mckoi.network.MckoiDDBAccess  Sep 29, 2012
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

package com.mckoi.network;

import com.mckoi.data.KeyObjectTransaction;
import com.mckoi.data.TreeReportNode;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * The client object for interacting with a Mckoi distributed network. This
 * interface encapsulates the primary functions needed for transactional
 * operations with a MckoiDDB network.
 * <p>
 * It is intended for implementations of this object to be long lived and
 * maintain various static information about network interactions, such as
 * node caches.
 * <p>
 * Use com.mckoi.network.MckoiDDBClientUtils to create instances of this
 * object.
 *
 * @author Tobias Downer
 */

public interface MckoiDDBAccess {

  /**
   * Returns the maximum amount of heap memory in the local JVM allowed to be
   * used to cache transaction node data writes before a commit. If this heap
   * space is exhausted during a transaction interaction in which a lot of data
   * is being written, node data will be flushed from the cache to the network
   * to make additional room.
   */
  long getMaximumTransactionNodeCacheHeapSize();
  
  /**
   * Sets the maximum amount of heap memory in the local JVM allowed to be
   * used to cache transaction node data writes before a commit. If this heap
   * space is exhausted during a transaction interaction in which a lot of data
   * is being written, node data will be flushed from the cache to the network
   * to make additional room.
   */
  void setMaximumTransactionNodeCacheHeapSize(long size_in_bytes);

  /**
   * Returns a TreeReportNode object used for diagnostic/reporting the
   * meta-data of a transaction tree..
   */
  TreeReportNode createDiagnosticGraph(KeyObjectTransaction t)
                                                           throws IOException;

  /**
   * Given a snapshot, traverses the nodes in the snapshot tree adding any
   * nodes that aren't already in the discovered node set into the set. This
   * method can be used for reachability determination as part of a garbage
   * collection function. For reachability determination, you would
   * incrementally call this method over all the historical roots of the
   * paths that need to be preserved. The method may also be used for some
   * diagnostic procedures.
   */
  void discoverNodesInSnapshot(PrintWriter warning_log,
                               DataAddress root_node,
                               DiscoveredNodeSet discovered_node_set);

  /**
   * Persists an empty database into the network and returns a DataAddress
   * object that references the root address of the empty database.
   */
  DataAddress createEmptyDatabase();
  
  /**
   * Queries the manager server and returns the list of path instances
   * currently available on active root servers. This method will actively
   * query the network so the result should be cached by the user if frequent
   * use of this information is needed.
   */
  String[] queryAllNetworkPaths();

  /**
   * Returns the name of the consensus function for the given database path.
   */
  String getConsensusFunction(String path_name);

  /**
   * Returns the root node of the most current published version of the named
   * path instance in the network. If the path instance is not found on the
   * network, an exception is generated.
   */
  DataAddress getCurrentSnapshot(String path_name);

  /**
   * Given a DataAddress of a root node in the network, creates and returns a
   * new transaction object that can be used to access and modify the data
   * stored there.
   */
  KeyObjectTransaction createTransaction(DataAddress root_node);

  /**
   * Creates a new transaction object that contains an empty database, useful
   * for managing temporary structures that are never flushed.
   */
  KeyObjectTransaction createEmptyTransaction();

  /**
   * Flushes modifications made to a transaction out to the network storage
   * machines and returns a DataAddress of the root node of the flushed
   * data.
   * <p>
   * The object referenced by the DataAddress is immutable in the sense that
   * once a DataAddress has been created for a transaction it may be passed
   * around to other clients freely and they may modify the changes but their
   * modifications will not be visible to anyone else.
   */
  DataAddress flushTransaction(KeyObjectTransaction transaction);

  /**
   * Performs a commit operation of the given path instance with the given
   * proposal. The proposal is represented as a DataAddress which should be the
   * result of a flushed transaction. The proposal is formatted in such a way
   * that the consensus processor assigned for the path on the root server will
   * understand how to interpret it.
   * <p>
   * Throws CommitFaultException if the consensus processor rejected the
   * proposal.
   */
  DataAddress performCommit(String path_name, DataAddress proposal)
                                                  throws CommitFaultException;

  /**
   * Disposes the KeyObjectTransaction previously created by the
   * createTransaction method of this object. This is intended as a way to
   * help reclaim resources associated with a transaction, but it is not
   * required to call this.
   */
  void disposeTransaction(KeyObjectTransaction transaction);

  /**
   * Returns an historical set of root nodes published to the root server
   * between the times given, where the time values follow the conventions of
   * System.currentTimeMillis().
   * <p>
   * Note that the returned root nodes may not be completely accurate because
   * the mechanism that records the time for each commit does not follow a
   * strict requirement for accuracy. For example, if a root server managing
   * commits for a path instance fails over to a new host, the clock on the
   * new host may be out of sync with the previous host thus it may appear
   * that some commits have happened at the same time and not in a serial
   * sequence.
   * <p>
   * Therefore consider the nodes returned by this method to be a reasonable
   * approximation of all the snapshot states committed in the given time span.
   */
  DataAddress[] getHistoricalSnapshots(String path_name,
                                       long time_start, long time_end);

}
