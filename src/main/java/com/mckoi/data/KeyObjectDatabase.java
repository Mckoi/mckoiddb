/**
 * com.mckoi.treestore.KeyObjectDatabase  Dec 29, 2007
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

package com.mckoi.data;

/**
 * A KeyObjectDatabase is a transactional key/data (Key/DataFile) map.  This
 * interfaces exposes the process of creating and publishing/disposing an
 * isolated transaction of a database.
 *
 * @author Tobias Downer
 */

public interface KeyObjectDatabase {

  /**
   * Returns a KeyObjectTransaction object that represents the most current
   * view of the database and allows isolated read and write operations on the
   * data including creating new key/data entries and modifying and removing
   * existing entries.
   * <p>
   * The returned object is <b>NOT</b> thread safe and may have various other
   * restrictions on its use depending on the implementation.  However, the
   * implemented transaction object should generally provide efficient
   * implementations on its function.
   */
  public KeyObjectTransaction createTransaction();

  /**
   * Publishes the transaction as the most current version of the
   * database state.  The given KeyObjectTransaction object must be a
   * modified form of the most recent version committed otherwise this
   * operation will fail.  For example, the following sequence will cause an
   * exception;
   * <code>
   *   t1 = createTransaction();
   *   t2 = createTransaction();
   *   publish(t1);
   *   publish(t2);
   * </code>
   * In the example above, the first publish will be successful but the second
   * publish will fail because t2 is no longer backed by the most recent
   * version.
   * <p>
   * If it is necessary to publish changes from an old view then the various
   * data copy/merge facilities should be used (eg. if this is part of a
   * general commit/consensus function).  It is left to the database
   * model to track database changes to manage version publish merge
   * operations.  The reason for this is because operations such as index
   * merges can not be supported in a general way at this level.
   */
  public void publish(KeyObjectTransaction transaction);

  /**
   * Disposes a transaction.  If the transaction was not published by the
   * 'publish' method above, then all data created in the transaction will
   * be deleted.  If the transaction was published, the transaction is marked
   * 'out of scope' and the database may choose to reclaim resources associated
   * with the transaction.
   */
  public void dispose(KeyObjectTransaction transaction);

  /**
   * If the underlying data storage structure supports check points, syncs
   * all the updates to the database up to this point in time.  This
   * method provides some guarantee that information written to the database
   * is consistent over multiple invocations.  A check point will typically
   * result in a sync and journal flush.
   * <p>
   * It is intended for check points to happen immediately after a publish, but
   * it is not necessary and check points can be made at any time.
   */
  public void checkPoint();

}
