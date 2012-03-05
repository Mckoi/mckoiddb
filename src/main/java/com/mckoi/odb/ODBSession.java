/**
 * com.mckoi.odb.ODBSession  Aug 3, 2010
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

package com.mckoi.odb;

import com.mckoi.data.DataFile;
import com.mckoi.data.KeyObjectTransaction;
import com.mckoi.data.PropertySet;
import com.mckoi.network.DataAddress;
import com.mckoi.network.MckoiDDBClient;

/**
 * The client interface for interacting with an Object Database path instance.
 * The object provides a stable session state in which transactions
 * can be started and interaction with the data model can be performed.
 * ODBSession objects are intended to be long lived.
 *
 * @author Tobias Downer
 */

public class ODBSession {

  /**
   * The MckoiDDBClient object used to handle the connection state
   * with the database.
   */
  private final MckoiDDBClient db_client;

  /**
   * The path name of the ODB data model in the network.
   */
  private final String path_name;

  /**
   * The check whether the path is valid has been performed or not yet.
   */
  private volatile boolean check_performed = false;

  /**
   * Constructs the ODBSession with the MckoiDDBClient object (needed to talk
   * with the network) and the path instance name of the database on the
   * network.
   */
  public ODBSession(MckoiDDBClient client, String path_name) {
    this.db_client = client;
    this.path_name = path_name;
  }

  /**
   * Returns the MckoiDDBClient object used on this session.
   */
  MckoiDDBClient getDatabaseClient() {
    return db_client;
  }

  /**
   * Returns the name of the path instance of this session.
   */
  public String getPathName() {
    return path_name;
  }

  /**
   * Checks the data model of 'path_name' is a valid SDBSession structure.
   */
  private void checkPathValid(KeyObjectTransaction transaction) {
    if (!check_performed) {
      synchronized (this) {
        if (!check_performed) {
          DataFile df = transaction.getDataFile(ODBTransactionImpl.MAGIC_KEY, 'r');
          PropertySet magic_set = new PropertySet(df);
          String ob_type = magic_set.getProperty("ob_type");
          String version = magic_set.getProperty("version");

          // Error if the data is incorrect,
          if (ob_type == null || !ob_type.equals("com.mckoi.odb.ObjectDatabase")) {
            throw new RuntimeException("Path '" + getPathName() +
                                       "' is not a ObjectDatabase");
          }

          check_performed = true;
        }
      }
    }
  }

  /**
   * Returns an ODBTransaction object based on the given root.
   */
  private ODBTransaction createTransaction(DataAddress base_root) {
    // Turn it into a transaction object,
    KeyObjectTransaction transaction = db_client.createTransaction(base_root);
    // Check the path is a valid ODBTransaction format,
    checkPathValid(transaction);
    // Wrap it around an ODBTransaction object, and return it
    return new ODBTransactionImpl(this, base_root, transaction);
  }

  /**
   * Returns an ODBTransaction object based on the data in the given root
   * address. This method is useful for reducing the amount of traffic
   * on the network needed to check if an instance path has changed. Many
   * applications do not always need to fetch the most current
   * snapshot when reading data because it doesn't matter if the information
   * is slightly out of date.
   * <p>
   * This method allows a client to decide for itself when it wants to check
   * if the state of a database has been updated, and allows reusing the
   * old snapshot views when it is appropriate to do so.
   */
  public ODBTransaction createTransaction(ODBRootAddress root_address) {
    // Check the root address session is the same as this object,
    if (root_address.getSession().equals(this)) {
      return createTransaction(root_address.getDataAddress());
    }
    else {
      throw new RuntimeException("root_address is not from this session");
    }
  }

  /**
   * Creates a current snapshot transaction object used for accessing and
   * modifying this Object Database path instance. This method will query the
   * network and fetch the latest version of the path instance from the root
   * server.
   * <p>
   * This is the same as calling 'createTransaction(getCurrentSnapshot())'.
   */
  public ODBTransaction createTransaction() {
    return createTransaction(getCurrentSnapshot());
  }

  /**
   * Returns an ODBRootAddress that represents the most current snapshot of
   * this path instance. This method will query the network and fetch the
   * latest version from the root server.
   */
  public ODBRootAddress getCurrentSnapshot() {
    return new ODBRootAddress(this,
                              db_client.getCurrentSnapshot(getPathName()));
  }

  /**
   * Returns an historical set of root node addresses committed to this
   * path between the times given, where the time values follow the conventions
   * of System.currentTimeMillis().
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
  public ODBRootAddress[] getHistoricalSnapshots(
                                             long time_start, long time_end) {
    DataAddress[] roots =
         db_client.getHistoricalSnapshots(getPathName(), time_start, time_end);
    // Wrap the returned objects in SDBRootAddress,
    ODBRootAddress[] odb_roots = new ODBRootAddress[roots.length];
    for (int i = 0; i < roots.length; ++i) {
      odb_roots[i] = new ODBRootAddress(this, roots[i]);
    }
    return odb_roots;
  }

  /**
   * Returns true if the sessions compare equally.
   */
  public boolean equals(Object ob) {
    if (ob == this) {
      return true;
    }
    else if (!(ob instanceof ODBSession)) {
      return false;
    }
    else {
      ODBSession session = (ODBSession) ob;
      return db_client == session.db_client && path_name.equals(path_name);
    }
  }

}
