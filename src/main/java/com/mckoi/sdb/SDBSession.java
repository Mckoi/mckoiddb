/**
 * com.mckoi.sdb.SDBSession  Jun 26, 2009
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
import com.mckoi.data.KeyObjectTransaction;
import com.mckoi.data.PropertySet;
import com.mckoi.network.DataAddress;
import com.mckoi.network.MckoiDDBAccess;
import com.mckoi.network.MckoiDDBClient;

/**
 * The client interface for interacting with a Simple Database path instance.
 * The object provides a stable session state in which transactions
 * can be started and interaction with the data model can be performed.
 * SDBSession objects are intended to be long lived.
 *
 * @author Tobias Downer
 */

public final class SDBSession {

  /**
   * The TCPMckoiDDBClient object used to handle the connection state
   * with the database.
   */
  private final MckoiDDBAccess db_client;

  /**
   * The path name of the SDB data model in the network.
   */
  private final String path_name;

  /**
   * The check whether the path is valid has been performed or not yet.
   */
  private volatile boolean check_performed = false;

  /**
   * Constructs the SDBSession with the MckoiDDBClient object (needed to talk
   * with the network) and the path instance name of the database on the
   * network.
   */
  public SDBSession(MckoiDDBClient client, String path_name) {
    this((MckoiDDBAccess) client, path_name);
  }

  /**
   * Constructs the SDBSession with the MckoiDDBAccess object (needed to talk
   * with the network) and the path instance name of the database on the
   * network.
   */
  public SDBSession(MckoiDDBAccess client, String path_name) {
    // Null checks
    if (client == null || path_name == null) throw new NullPointerException();

    this.db_client = client;
    this.path_name = path_name;
  }

  /**
   * Returns the TCPMckoiDDBAccess object used on this session.
   * <p>
   * Security: Don't expose this outside the package scope!
   */
  MckoiDDBAccess getDatabaseClient() {
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
          DataFile df = transaction.getDataFile(SDBTransaction.MAGIC_KEY, 'r');
          PropertySet magic_set = new PropertySet(df);
          String ob_type = magic_set.getProperty("ob_type");
          String version = magic_set.getProperty("version");

          // Error if the data is incorrect,
          if (ob_type == null || !ob_type.equals("com.mckoi.sdb.SimpleDatabase")) {
            throw new RuntimeException("Path '" + getPathName() +
                                       "' is not a SimpleDatabase");
          }

          check_performed = true;
        }
      }
    }
  }

  /**
   * Returns an SDBTransaction object based on the given root.
   */
  private SDBTransaction createTransaction(DataAddress base_root) {
    // Turn it into a transaction object,
    KeyObjectTransaction transaction = db_client.createTransaction(base_root);
    // Check the path is a valid SDBTransaction format,
    checkPathValid(transaction);
    // Wrap it around an SDBTransaction object, and return it
    return new SDBTransaction(this, base_root, transaction);
  }

  /**
   * Returns an SDBTransaction object based on the data in the given root
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
  public SDBTransaction createTransaction(SDBRootAddress root_address) {
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
   * modifying this Simple Database path instance. This method will query the
   * network and fetch the latest version of the path instance from the root
   * server.
   * <p>
   * This is the same as calling 'createTransaction(getCurrentSnapshot())'.
   */
  public SDBTransaction createTransaction() {
    return createTransaction(getCurrentSnapshot());
  }

  /**
   * Returns an SDBRootAddress that represents the most current snapshot of
   * this path instance. This method will query the network and fetch the
   * latest version from the root server.
   */
  public SDBRootAddress getCurrentSnapshot() {
    return new SDBRootAddress(this,
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
  public SDBRootAddress[] getHistoricalSnapshots(
                                             long time_start, long time_end) {
    DataAddress[] roots =
         db_client.getHistoricalSnapshots(getPathName(), time_start, time_end);
    // Wrap the returned objects in SDBRootAddress,
    SDBRootAddress[] sdb_roots = new SDBRootAddress[roots.length];
    for (int i = 0; i < roots.length; ++i) {
      sdb_roots[i] = new SDBRootAddress(this, roots[i]);
    }
    return sdb_roots;
  }

  /**
   * Returns true if the sessions compare equally.
   */
  @Override
  public boolean equals(Object ob) {
    if (ob == this) {
      return true;
    }
    else if (!(ob instanceof SDBSession)) {
      return false;
    }
    else {
      SDBSession session = (SDBSession) ob;
      return db_client == session.db_client && path_name.equals(path_name);
    }
  }

}
