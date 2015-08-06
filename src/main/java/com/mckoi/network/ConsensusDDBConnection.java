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

package com.mckoi.network;

import com.mckoi.data.KeyObjectTransaction;

/**
 * Encapsulates the communication functions necessary to talk with a
 * distributed network from the deterministic environment of a consensus
 * function (com.mckoi.network.ConsensusProcessor) of a path instance.
 *
 * @author Tobias Downer
 */

public interface ConsensusDDBConnection {

  /**
   * Returns the root node of the most current published version of the
   * database path instance. This method will be expected to return the root
   * node of the current snapshot and is absolutely certain to be the most
   * current version of the database. Since the consensus function operates as
   * a serial process and the current snapshot can only be updated by the
   * consensus function, there is an implied certainty that the returned
   * snapshot will be the most current.
   */
  DataAddress getCurrentSnapshot();

  /**
   * Returns an historical set of root nodes published on this path instance
   * between the times given, where the time values follow the conventions of
   * System.currentTimeMillis().
   * <p>
   * The accuracy of the root nodes returned from this method follows the
   * same conventions described in the 'getCurrentSnapshot' function.
   */
  DataAddress[] getHistoricalSnapshots(long time_start, long time_end);

  /**
   * Returns the list of every snapshot state that has been published since the
   * given root node (but not including). If no new snapshot states have been
   * made then an empty array is returned.
   * <p>
   * The accuracy of the information returned from this method follows the
   * same conventions described in the 'getCurrentSnapshot' function.
   */
  DataAddress[] getSnapshotsSince(DataAddress root_node);

  /**
   * Given the address of a root node, creates a KeyObjectTransaction object
   * that allows access and mutation of the database locally.
   */
  KeyObjectTransaction createTransaction(DataAddress root_node);

  /**
   * Flushes modifications made to a transaction out to the network storage
   * machines, and returns a unique DataAddress that references the root node
   * of the flushed transaction. Once a transaction is flushed, the information
   * in the transaction may be freely shared to other remote clients that have
   * access to the network simply by sharing the DataAddress object.
   * <p>
   * The structure referenced by the DataAddress is immutible in the sense that
   * once a DataAddress has been created for a transation it may be passed
   * around to other clients freely and they may modify the changes but their
   * modifications will not be visible to anyone else unless they also flush
   * the change and pass around the root node DataAddress of the structure with
   * the changes.
   * <p>
   * Note that this function will always have a network time cost since the
   * information must be written to the network (to multiple machines).
   * Additionally, using this function will always result in storage resources
   * on the network being consumed.
   */
  public DataAddress flushTransaction(KeyObjectTransaction transaction);

  /**
   * Publishes a transaction snapshot as the most recent version of this
   * database path instance on the root server. This makes the given published
   * root node the new current version as returned by 'getCurrentSnapshot'.
   */
  void publishToPath(DataAddress root_node);

}
