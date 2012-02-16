/**
 * com.mckoi.network.MachineProfile  Jul 4, 2009
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
 * Describes a machine in a Mchoi distributed network, used by administration
 * functions.
 *
 * @author Tobias Downer
 */

public class MachineProfile {

  /**
   * The machine service address.
   */
  private final ServiceAddress machine_address;

  /**
   * Status flags,
   */
  private boolean is_block;
  private boolean is_root;
  private boolean is_manager;

  private long heap_used;
  private long heap_total;
  private long storage_used;
  private long storage_total;

  /**
   * If the machine reported a problem when queried, the error message is
   * included here.
   */
  private String problem_message;

  /**
   * Constructor.
   */
  MachineProfile(ServiceAddress address) {
    this.machine_address = address;
  }

  /**
   * Sets the state,
   */
  void setIsBlock(boolean b) {
    is_block = b;
  }

  /**
   * Sets the state,
   */
  void setIsRoot(boolean b) {
    is_root = b;
  }

  /**
   * Sets the state,
   */
  void setIsManager(boolean b) {
    is_manager = b;
  }

  void setHeapUsed(long v) {
    heap_used = v;
  }
  void setHeapTotal(long v) {
    heap_total = v;
  }
  void setStorageUsed(long v) {
    storage_used = v;
  }
  void setStorageTotal(long v) {
    storage_total = v;
  }

  /**
   * Sets the problem when querying the administrator role, if a problem was
   * reported.
   */
  void setProblemMessage(String msg) {
    problem_message = msg;
  }

  // ----- Getters -----

  /**
   * Returns the ServerAddress that locates the machine on the network.
   */
  public ServiceAddress getServiceAddress() {
    return machine_address;
  }

  /**
   * Returns true if this machine is assigned as a block server role.
   */
  public boolean isBlock() {
    return is_block;
  }
  /**
   * Returns true if this machine is assigned as a root server role.
   */
  public boolean isRoot() {
    return is_root;
  }
  /**
   * Returns true if this machine is assigned as a manager server role.
   */
  public boolean isManager() {
    return is_manager;
  }

  /**
   * True if this machine is not assigned any roles.
   */
  boolean isNotAssigned() {
    return !is_block && !is_manager && !is_root;
  }

  /**
   * Returns true if there was an error when optaining information
   * about this machine.
   */
  public boolean isError() {
    return (problem_message != null);
  }

  /**
   * Returns the error message describing why 'isError' returned true.
   */
  public String getProblemMessage() {
    return problem_message;
  }

  /**
   * Returns the amount of heap space reported to be used by the machine.
   */
  public long getHeapUsed() {
    return heap_used;
  }
  /**
   * Returns the total amount of heap space reported by the machine.
   */
  public long getHeapTotal() {
    return heap_total;
  }
  /**
   * Returns the amount of storage space reported to be used by the machine.
   */
  public long getStorageUsed() {
    return storage_used;
  }
  /**
   * Returns the total amount of storage space reported by the machine.
   */
  public long getStorageTotal() {
    return storage_total;
  }

}
