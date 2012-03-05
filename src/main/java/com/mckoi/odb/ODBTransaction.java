/**
 * com.mckoi.sdb.ODBTransaction  Aug 2, 2010
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

import com.mckoi.network.CommitFaultException;
import java.util.List;

/**
 * A transaction in an Mckoi Object Database data model session. A transaction
 * is a view of the data at a consistent state in the lifespan of the
 * database. The data visible in a transaction will show changes made locally
 * by the client, but otherwise will never change from the time of
 * construction. Changes made to the backed database that happen while a
 * transaction exists will not be visible in the transaction.
 * <p>
 * An ODBTransaction object is NOT thread-safe. It is important that
 * interaction with an ODBTransaction, and also any objects produced by, is
 * only ever accessed by a single thread at a time.
 * <p>
 * Once a series of changes has been made to a transaction, the transaction
 * must be committed for the changes to become permanent. The commit process
 * may discover inconsistencies, such as duplicate entries added to a list or
 * concurrent transactions removing the same item. In such cases, the commit
 * will fail and generate a CommitFaultException.
 *
 * @author Tobias Downer
 */

public interface ODBTransaction {

  /**
   * Returns the path name of the session.
   */
  String getSessionPathName();

  /**
   * Returns an ODBClassCreator for creating a class schema in this
   * transaction. The classes created with a creator object must first be
   * validated before they may be accessed within the transaction. As with all
   * data operations, the transaction must be committed before changes are
   * made permanent.
   */
  ODBClassCreator getClassCreator();

  /**
   * Returns the class instance currently defined with the given name, or null
   * if no class currently defined with that name.
   */
  ODBClass findClass(String class_name);

  /**
   * Defines an object and returns the object. An object must resolve
   * against the given class type. Note that the given arguments may be
   * only either a java.lang.String, a com.mckoi.odb.Reference or null.
   */
  ODBObject constructObject(ODBClass clazz, Object... args);

  /**
   * Returns a class from a reference. The returned class is immutable.
   * <p>
   * Throws NoSuchReferenceException if the reference is invalid.
   */
  ODBClass getClass(Reference ref);

  /**
   * Returns an ODBObject from a reference. Returns null if the reference is
   * to a null object. The returned object is selectively mutable and changes
   * made are published on commit. Fields defined as immutable can not be
   * changed.
   * <p>
   * Throws NoSuchReferenceException if the reference is invalid.
   */
  ODBObject getObject(ODBClass type, Reference ref);

  /**
   * Adds a named item to the database. A named item is a 'window' or starting
   * point of the object graph.
   */
  void addNamedItem(String name, ODBObject item);

  /**
   * Removes a named item from the database. If the named item isn't defined
   * then returns false, otherwise returns true if the named item was deleted.
   */
  boolean removeNamedItem(String name);

  /**
   * Returns a named item from the database, or null if the named item isn't
   * defined.
   */
  ODBObject getNamedItem(String name);

  /**
   * Scans the object graph and creates a set of reachable objects, and then
   * finds the difference between the entire set of references and the set of
   * reachable objects. References that are not in the graph are deleted.
   * <p>
   * This process may take time to complete (it is recommended be done in an
   * offline process). A garbage collection operation will never cause a fault
   * on commit.
   * <p>
   * The contract does not guarantee all unreachable objects will be deleted
   * by this operation since the administrator may define policy on the maximum
   * amount of time a garbage collection cycle can take. This operation should
   * operate incrementally if desired.
   */
  void doGarbageCollection();

  /**
   * Returns the set of all named items as a List of Java String objects.
   * Changes to the list of named items is immediately reflected in the
   * returned object, however the returned list itself is immutable.
   */
  List<String> getNamedItemsList();

  /**
   * Returns the set of all class names as a List of Java String objects.
   * Changes to the classes defined is immediately reflected in the
   * returned object, however the returned list itself is immutable.
   */
  List<String> getClassNamesList();

  /**
   * Commits any changes made in this transaction.
   */
  ODBRootAddress commit() throws CommitFaultException;

}
