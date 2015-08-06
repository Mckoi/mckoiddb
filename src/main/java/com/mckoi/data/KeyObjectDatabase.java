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
