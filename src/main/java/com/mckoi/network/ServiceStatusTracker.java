/**
 * com.mckoi.network.ServiceStatusTracker  Jun 17, 2010
 *
 * Mckoi Database Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2010  Diehl and Associates, Inc.
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

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An object that passively and actively tracks the availability of services
 * on the network. When a service is determined to have failed through an
 * attempted operation, this object is notified of the failure. This tracker
 * will then actively poll the service in the background until it is
 * determined to be available again.
 *
 * @author Tobias Downer
 */

public final class ServiceStatusTracker {

  /**
   * Status that the server is up and fully functional, as far as is known.
   */
  public static final String STATUS_UP = "UP";

  /**
   * Status that the server has been marked as being shut down, therefore the
   * server should not be used to resolve any client requests.
   */
  public static final String STATUS_DOWN_SHUTDOWN = "DOWN SHUTDOWN";

  /**
   * The server is down because it failed a heartbeat check.
   */
  public static final String STATUS_DOWN_HEARTBEAT = "DOWN HEARTBEAT";

  /**
   * The server is down because a client reported an unrecoverable failure on
   * an operation with the server.
   */
  public static final String STATUS_DOWN_CLIENT_REPORT = "DOWN CLIENT REPORT";




  /**
   * The heartbeat thread, this polls block servers that have been reported
   * as failed by client report.
   */
  private final HeartbeatThread heartbeat_thread;

  /**
   * The list of TrackedService objects the heartbeat thread is
   * currently monitoring.
   */
  private final ArrayList<TrackedService> monitored_servers;

  /**
   * The list of event listeners on this heartbeat thread. An event is fired
   * when it is detected that the status of a service changes.
   */
  private final ArrayList<ServiceStatusListener> listeners;

  /**
   * The logger.
   */
  private final static Logger log = Logger.getLogger("com.mckoi.network.Log");


  /**
   * Constructor.
   */
  public ServiceStatusTracker(NetworkConnector network) {
    this.monitored_servers = new ArrayList(128);
    this.listeners = new ArrayList(8);

    this.heartbeat_thread =
                   new HeartbeatThread(network, monitored_servers, listeners);

    // Start the tracker,
    this.heartbeat_thread.setDaemon(true);
    this.heartbeat_thread.start();
  }

  /**
   * Stops the tracker.
   */
  public void stop() {
    heartbeat_thread.doFinish();
  }

  /**
   * Adds a listener for changes in the status of tracked services.
   */
  public void addListener(ServiceStatusListener listener) {
    heartbeat_thread.addListener(listener);
  }

  /**
   * Removes a listener from changes in the status of tracked services.
   */
  public void removeListener(ServiceStatusListener listener) {
    heartbeat_thread.removeListener(listener);
  }

  /**
   * Returns the current status of the given service.
   */
  public String getServiceCurrentStatus(ServiceAddress address, String type) {
    // Search and return,
    // PENDING: Should this be a hash lookup instead for speed?
    synchronized (monitored_servers) {
      for (TrackedService s : monitored_servers) {
        if (s.address.equals(address) &&
            s.type.equals(type)) {
          return s.current_status;
        }
      }
    }
    // Not found in list, so assume the service is up,
    return STATUS_UP;
  }

  /**
   * Returns true if the service is current up.
   */
  public boolean isServiceUp(ServiceAddress address, String type) {
    return getServiceCurrentStatus(address, type).equals(STATUS_UP);
  }

  private void reportServiceDown(ServiceAddress address, String type,
                                 String status) {

    // Default old status,
    String old_status = STATUS_UP;

    // Search and return,
    synchronized (monitored_servers) {
      TrackedService tracked = null;
      for (TrackedService s : monitored_servers) {
        if (s.address.equals(address) &&
            s.type.equals(type)) {
          tracked = s;
          break;
        }
      }

      if (tracked == null) {
        // Not found so add it to the tracker,
        monitored_servers.add(
                   new TrackedService(address, type, status));
      }
      else {
        old_status = tracked.current_status;
        tracked.current_status = status;
      }
    }

    // Fire the event if the status changed,
    if (!old_status.equals(status)) {
      ArrayList<ServiceStatusListener> al;
      synchronized (listeners) {
        al = new ArrayList(listeners.size());
        al.addAll(listeners);
      }
      for (ServiceStatusListener l : al) {
        l.statusChange(address, type, old_status, status);
      }
    }

  }

//  /**
//   * Reports the service is down. When the service comes back up (detected
//   * via a poll), the 'action' run method is called. If 'action' is null then
//   * no user-defined action is performed.
//   */
//  public void reportServiceDownClientReport(
//             ServiceAddress address, String type, Runnable available_action) {
//
//    if (log.isLoggable(Level.FINER)) {
//      log.log(Level.FINER, "reportServiceDownClientReport {0} {1}",
//              new Object[] { address.displayString(), type });
//    }
//
//    reportServiceDown(address, type,
//                      STATUS_DOWN_CLIENT_REPORT, available_action);
//  }

  /**
   * Reports the service is down. When the service comes back up (detected
   * via a poll), the 'action' run method is called. If 'action' is null then
   * no user-defined action is performed.
   */
  public void reportServiceDownClientReport(
                                        ServiceAddress address, String type) {
    if (log.isLoggable(Level.FINER)) {
      log.log(Level.FINER, "reportServiceDownClientReport {0} {1}",
              new Object[] { address.displayString(), type });
    }

    reportServiceDown(address, type, STATUS_DOWN_CLIENT_REPORT);
  }

  /**
   * Report the service is down from a controlled shutdown.
   */
  public void reportServiceDownShutdown(ServiceAddress address, String type) {

    if (log.isLoggable(Level.FINER)) {
      log.log(Level.FINER, "reportServiceDownShutdown {0} {1}",
              new Object[] { address.displayString(), type });
    }

    reportServiceDown(address, type, STATUS_DOWN_SHUTDOWN);
  }



  /**
   * The service being tracked.
   */
  private static class TrackedService {

    // The network address of the service,
    private final ServiceAddress address;
    // The type of the service being tracked (eg. 'block', 'manager', 'root')
    private final String type;
    // The current status
    private volatile String current_status;

    private TrackedService(ServiceAddress address, String type,
                           String status) {
      this.address = address;
      this.type = type;
      this.current_status = status;
    }

    private TrackedService(ServiceAddress address, String type) {
      this(address, type, STATUS_UP);
    }

    @Override
    public int hashCode() {
      return address.hashCode() + type.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || !(obj instanceof TrackedService)) {
        return false;
      }
      final TrackedService other = (TrackedService) obj;
      if (this.address != other.address &&
              (this.address == null || !this.address.equals(other.address))) {
        return false;
      }
      if ((this.type == null) ?
              (other.type != null) : !this.type.equals(other.type)) {
        return false;
      }
      return true;
    }

  }


  /**
   * The heartbeat thread polls servers that have been reported as failed by
   * clients, and updates the status of the server as appropriate.
   */
  private static class HeartbeatThread extends Thread {

    /**
     * The time between polls on the service (default is 10 seconds).
     */
    private int poll_delay_ms = (10 * 1000);

    /**
     * The list of event listeners on this heartbeat thread. An event is fired
     * when it is detected that the status of a service changes.
     */
    private final ArrayList<ServiceStatusListener> listeners;


    private boolean finished = false;

    private final NetworkConnector network;
    private final ArrayList<TrackedService> monitored_servers;

    /**
     * Constructor.
     */
    public HeartbeatThread(NetworkConnector network,
                           ArrayList<TrackedService> monitored_servers,
                           ArrayList<ServiceStatusListener> listeners) {
      this.network = network;
      this.monitored_servers = monitored_servers;
      this.listeners = listeners;
    }

    /**
     * Adds a listener for service status changes.
     */
    public void addListener(ServiceStatusListener listener) {
      synchronized (listeners) {
        // Don't add the same listener twice,
        for (int i = 0; i < listeners.size(); ++i) {
          if (listeners.get(i) == listener) {
            return;
          }
        }
        // Add the listener
        listeners.add(listener);
      }
    }

    /**
     * Removes a listener of a service status change.
     */
    public void removeListener(ServiceStatusListener listener) {
      synchronized (listeners) {
        for (int i = 0; i < listeners.size(); ++i) {
          if (listeners.get(i) == listener) {
            listeners.remove(i);
            return;
          }
        }
      }
    }

//    /**
//     * Adds a server to the list of servers currently being monitored by this
//     * thread.
//     */
//    void addCheck(TrackedService block_server) {
//      synchronized (monitored_servers) {
//        if (!monitored_servers.contains(block_server)) {
//          monitored_servers.add(block_server);
//        }
//      }
//    }

    /**
     * Polls the given server, if the server polls as expected changes the
     * status of the server to 'UP' and removes the server from the monitor
     * list.
     */
    private void pollServer(TrackedService server) {
      boolean poll_ok = true;

      // Send the poll command to the server,
      if (server.type.equals("block")) {
        MessageProcessor p = network.connectBlockServer(server.address);
        MessageStream msg_out = new MessageStream(4);
        msg_out.addMessage("poll");
        msg_out.addString("heatbeatB");
        msg_out.closeMessage();
        ProcessResult msg_in = p.process(msg_out);
        for (Message m : msg_in) {
          // Any error with the poll means no status change,
          if (m.isError()) {
            poll_ok = false;
          }
        }
      }
      else if (server.type.equals("manager")) {
        MessageProcessor p = network.connectManagerServer(server.address);
        MessageStream msg_out = new MessageStream(4);
        msg_out.addMessage("poll");
        msg_out.addString("heatbeatM");
        msg_out.closeMessage();
        ProcessResult msg_in = p.process(msg_out);
        for (Message m : msg_in) {
          // Any error with the poll means no status change,
          if (m.isError()) {
            poll_ok = false;
          }
        }
      }
      else if (server.type.equals("root")) {
        MessageProcessor p = network.connectRootServer(server.address);
        MessageStream msg_out = new MessageStream(4);
        msg_out.addMessage("poll");
        msg_out.addString("heatbeatR");
        msg_out.closeMessage();
        ProcessResult msg_in = p.process(msg_out);
        for (Message m : msg_in) {
          // Any error with the poll means no status change,
          if (m.isError()) {
            poll_ok = false;
          }
        }
      }
      else {
        log.log(Level.SEVERE, "Don't know how to poll type {0}", server.type);
        poll_ok = false;
      }

      // If the poll is ok, set the status of the server to UP and remove from
      // the monitor list,
      if (poll_ok) {
        // The server status is set to 'STATUS_UP' if either the current state
        // is 'DOWN CLIENT REPORT' or 'DOWN HEARTBEAT'
        // Synchronize over 'servers_map' for safe alteration of the ref.
        String old_status;
        synchronized (monitored_servers) {
          old_status = server.current_status;
          if (old_status.equals(STATUS_DOWN_CLIENT_REPORT) ||
              old_status.equals(STATUS_DOWN_HEARTBEAT)) {
            server.current_status = STATUS_UP;
          }
          // Remove the server from the monitored_servers list.
          monitored_servers.remove(server);
        }

        if (log.isLoggable(Level.FINER)) {
          log.log(Level.FINER, "Poll ok. Status now UP for {0} {1}",
                new Object[] { server.address.displayString(), server.type });
        }

        // Fire the event if the status changed,
        ArrayList<ServiceStatusListener> al;
        synchronized (listeners) {
          al = new ArrayList(listeners.size());
          al.addAll(listeners);
        }
        for (ServiceStatusListener l : al) {
          try {
            l.statusChange(server.address, server.type, old_status, STATUS_UP);
          }
          catch (Exception e) {
            // Catch any exception generated. Log it but don't terminate the
            // thread.
            log.log(Level.SEVERE, "Exception in listener during poll", e);
          }
        }


//        // Perform the availability action
//        try {
//          if (server.available_action != null) {
//            server.available_action.run();
//          }
//        }
//        catch (Throwable e) {
//          // Catch all exceptions and log them,
//          log.log(Level.SEVERE, "Error in available action", e);
//        }

      }
      else {
        // Make sure the server status is set to 'DOWN HEARTBEAT' if the poll
        // failed,
        // Synchronize over 'servers_map' for safe alteration of the ref.
        synchronized (monitored_servers) {
          String sts = server.current_status;
          if (sts.equals(STATUS_UP) ||
              sts.equals(STATUS_DOWN_CLIENT_REPORT)) {
            server.current_status = STATUS_DOWN_HEARTBEAT;
          }
        }
      }
    }

    /**
     * The thread run method,
     */
    public void run() {
      try {
        while (true) {
          ArrayList<TrackedService> servers;
            // Wait on the poll delay
          synchronized (this) {
            if (finished) {
              return;
            }
            wait(poll_delay_ms);
            if (finished) {
              return;
            }
          }
          synchronized (monitored_servers) {
            // If there are no servers to monitor, continue the loop,
            if (monitored_servers.size() == 0) {
              continue;
            }
            // Otherwise, copy the monitored servers into the 'servers'
            // object,
            servers = new ArrayList(monitored_servers.size());
            servers.addAll(monitored_servers);
          }
          // Poll the servers
          for (TrackedService s : servers) {
            pollServer(s);
          }
        }
      }
      catch (InterruptedException e) {
        log.warning("Heartbeat thread interrupted");
      }
    }


    public void doFinish() {
      synchronized(this) {
        finished = true;
        notifyAll();
      }
    }

  }

}
