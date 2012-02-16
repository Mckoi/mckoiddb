/**
 * com.mckoi.network.ConsensusProcessor  Jun 21, 2009
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

/**
 * ConsensusProcessor defines the characteristics of a database path instance,
 * including how the path instance should be initialized, how proposed changes
 * to the instance are committed, and a way to display some stats about the
 * instance.
 * <p>
 * A ConsensusProcessor implements a deterministic 'commit' function that either
 * makes a proposed change to a database, or makes no change and generates a
 * commit fault exception. The purpose of this interface is to provide a
 * plug-in architecture for structured data models that can ensure their
 * logical consistency in the face of changes to state made at any time.
 * <p>
 * Any none trivial data model will almost always need to enforce some sort of
 * consistency rules (such as integrity constraints) that are not allowed to be
 * violated when a change to a database is proposed. This interface allows a
 * developer to define the consistency rules of a data model by providing a
 * function to allow or reject a proposal, and if a proposal is accepted;
 * define the final format of the accepted changes.
 * <p>
 * For example, a ConsensusProcessor for an SQL database would need to reject a
 * proposal in which a row in a table is inserted with a primary key value
 * that is already present in the table, or a single row that is deleted by
 * concurrent clients. Another example is a ConsensusProcessor for a
 * distributed file system would need to reject concurrent proposals to create
 * a file object with the same name.
 * <p>
 * The ConsensusProcessor interface provides a way to enforce the consistency
 * of a data model. Keep in mind that the 'commit' function, by definition,
 * must be performed in a serial process because to accept or reject a
 * proposal the current state of a database must be known and deterministic.
 * No consensus would be able to be determined if it is unknown if the current
 * state might change. For this reason, the consensus functions 'commit'
 * method must be as efficient as possible.
 *
 * @author Tobias Downer
 */

public interface ConsensusProcessor {

  // ----- Plug in Information -----

  /**
   * The name of this processor, displayed in the adminsitration user
   * interface.
   */
  String getName();

  /**
   * A description of this processor appropriate for display in the help
   * section of the user interface.
   */
  String getDescription();

  // ----- Function -----

  /**
   * This function creates an initial data model state on a completely blank
   * database. This is only ever called once during the lifespan of a
   * particular state, and the given connection will always be a blank
   * untouched database.
   * <p>
   * This function is intended to setup the state of a database to a beginning
   * state.
   */
  void initialize(ConsensusDDBConnection connection);

  /**
   * Attempts to commit a proposed change and create an updated database state.
   * The given ConsensusDDBConnection object provides access to the latest
   * version of the database as well as historical information about previous
   * states. The proposal is represented as a DataAddress object which is the
   * root node of a data tree that will contain the details of the changes
   * proposed.
   * <p>
   * If a proposed commit is rejected, this function throws a
   * CommitFaultException and no change is made. If the proposal is accepted,
   * the returned DataAddress is the new root node of this path instance.
   * <p>
   * Note that this method should be efficiently implemented because it is not
   * possible to perform this function on a single path instance in multiple
   * processes. The reason for this is because the current database state
   * must be deterministic.
   *
   * @param connection the connection to the network where the change will be
   *   made.
   * @param proposal the proposed change.
   */
  DataAddress commit(ConsensusDDBConnection connection, DataAddress proposal)
                                                   throws CommitFaultException;

  /**
   * Returns a String that briefly describes basic stats about the data
   * model of the given snapshot. This function can return null if there are no
   * stat functions implemented.
   * <p>
   * This string is intended to be displayed in a user interface to give some
   * status feedback about a path instance to an administrator.
   */
  String getStats(ConsensusDDBConnection connection, DataAddress snapshot);

}
