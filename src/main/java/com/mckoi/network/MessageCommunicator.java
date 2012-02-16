/**
 * com.mckoi.network.MessageCommunicator  Jun 20, 2010
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
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This object is used by servers for handling inter-process communication
 * in a fault tolerent way between different processes. This object provides
 * a number of functions for performing various types of message passing.
 * When a message to a service fails, depending on the type of message, the
 * message is put into a queue and retried when the service is available
 * again.
 *
 * @author Tobias Downer
 */

public class MessageCommunicator {

  /**
   * The network connector.
   */
  private final NetworkConnector network;

  /**
   * The tracker of the status of services on the network.
   */
  private final ServiceStatusTracker tracker;

  /**
   * A timer object for scheduling events.
   */
  private final Timer timer;

  /**
   * The map of pending service address to queues.
   */
  private final HashMap<ServiceAddress, RetryMessageQueue> queue_map;

  /**
   * The logger.
   */
  private static final Logger log = Logger.getLogger("com.mckoi.network.Log");


  /**
   * Constructor.
   */
  MessageCommunicator(NetworkConnector network,
                      ServiceStatusTracker tracker, Timer timer) {

    this.network = network;
    this.tracker = tracker;
    this.timer = timer;

    this.queue_map = new HashMap();
  }

  /**
   * Retries all pending messages on the service at the given address. This
   * method returns immediately, the messages are retried on a timer thread.
   */
  void retryMessagesFor(final ServiceAddress address) {
    RetryMessageQueue queue = null;
    synchronized (queue_map) {
      queue = queue_map.get(address);
    }

    if (queue != null) {

      // Schedule on the timer queue,
      timer.schedule(new TimerTask() {
        public void run() {
          ArrayList<String> types;
          ArrayList<MessageStream> messages;
          synchronized (queue_map) {
            // Remove from the queue,
            RetryMessageQueue queue = queue_map.remove(address);
            types = queue.service_types;
            messages = queue.queue;
          }
          // Create a message queue
          ServiceMessageQueue message_queue = createServiceMessageQueue();
          // For each message in the queue,
          int sz = types.size();
          for (int i = 0; i < sz; ++i) {
            String type = types.get(i);
            MessageStream message_stream = messages.get(i);

            if (type.equals("manager")) {
              sendManagerMessage(message_queue, address, message_stream);
            }
            else if (type.equals("root")) {
              sendRootMessage(message_queue, address, message_stream);
            }
            else {
              throw new RuntimeException("Unknown type");
            }
          }
          // Re-enqueue any pending messages,
          message_queue.enqueue();
        }
      }, 500);

    }
  }

  /**
   * Gets the queue for the given service.
   */
  private RetryMessageQueue getRetryMessageQueue(
                                          final ServiceAddress service_addr) {

    synchronized (queue_map) {
      RetryMessageQueue queue = queue_map.get(service_addr);
      if (queue == null) {
        queue = new RetryMessageQueue(service_addr);
        queue_map.put(service_addr, queue);
      }
      return queue;
    }

  }

  /**
   * Create a message for the given command and arguments.
   */
  public static void createMessage(
                       MessageStream message_out, String msg, Object[] args) {

    message_out.addMessage(msg);
    for (Object arg : args) {
      message_out.addObject(arg);
    }
    message_out.closeMessage();
  }

//  /**
//   * Performs a function on the manager server, generating an exception if
//   * the operation fails.
//   */
//  private Message doManagerFunction(ServiceAddress manager_server,
//                                    MessageStream message_out,
//                                    boolean exception_on_fail) {
//
//    // Process the message,
//    MessageProcessor processor = network.connectManagerServer(manager_server);
//    MessageStream message_in = processor.process(message_out);
//
//    // Handle the response,
//    for (Message m : message_in) {
//      if (m.isError()) {
//
//        // PENDING: We should confirm the error is a connection failure before
//        //   reporting the service as down.
//        // Report the service as down to the tracker,
//        tracker.reportServiceDownClientReport(manager_server, "manager");
//
//        // If we failed, generate an exception,
//        log.log(Level.SEVERE, "External Exception:\n" +
//                              m.getExternalThrowable().getStackTrace());
//
//        if (exception_on_fail) {
//          throw new RuntimeException(m.getExternalThrowable().getMessage());
//        }
//        else {
//          return m;
//        }
//
//      }
//      else {
//        return m;
//      }
//    }
//
//    // Odd,
//    throw new RuntimeException(
//                          "Empty reply from manager command: " + message_out);
//  }
//
//  /**
//   * Performs a function on the manager server, generating an exception if
//   * the operation fails.
//   */
//  private Message doRootFunction(ServiceAddress root_server,
//                                 MessageStream message_out,
//                                 boolean exception_on_fail) {
//
//    // Process the message,
//    MessageProcessor processor = network.connectRootServer(root_server);
//    MessageStream message_in = processor.process(message_out);
//
//    // Handle the response,
//    for (Message m : message_in) {
//      if (m.isError()) {
//
//        // PENDING: We should confirm the error is a connection failure before
//        //   reporting the service as down.
//        // Report the service as down to the tracker,
//        tracker.reportServiceDownClientReport(root_server, "root");
//
//        // If we failed, generate an exception,
//        log.log(Level.SEVERE, "External Exception:\n" +
//                              m.getExternalThrowable().getStackTrace());
//
//        if (exception_on_fail) {
//          throw new RuntimeException(m.getExternalThrowable().getMessage());
//        }
//        else {
//          return m;
//        }
//
//      }
//      else {
//        return m;
//      }
//    }
//
//    // Odd,
//    throw new RuntimeException(
//                          "Empty reply from root command: " + message_out);
//  }

  /**
   * Sends a message to a root server.
   */
  private int sendRootMessage(ServiceMessageQueue queue,
                              ServiceAddress root_server,
                              MessageStream message_out) {
    int success_count = 0;

    // Process the message,
    MessageProcessor processor = network.connectRootServer(root_server);
    ProcessResult message_in = processor.process(message_out);

    // Handle the response,
    for (Message m : message_in) {
      if (m.isError()) {
        log.log(Level.SEVERE, "Root error: " + m.getErrorMessage());

        // If we failed, add the message to the retry queue,
        if (queue != null) {
          queue.addMessageStream(root_server, message_out, "root");
        }

        // PENDING: We should confirm the error is a connection failure before
        //   reporting the service as down.
        // Report the service as down to the tracker,
        tracker.reportServiceDownClientReport(root_server, "root");
      }
      else {
        // Message successfully sent,
        ++success_count;
      }
    }

    return success_count;
  }

  /**
   * Sends a message to a manager server.
   */
  private int sendManagerMessage(ServiceMessageQueue queue,
                                 ServiceAddress manager_server,
                                 MessageStream message_out) {
    int success_count = 0;

    // Process the message,
    MessageProcessor processor = network.connectManagerServer(manager_server);
    ProcessResult message_in = processor.process(message_out);

    // Handle the response,
    for (Message m : message_in) {
      if (m.isError()) {
        log.log(Level.SEVERE, "Manager error: " + m.getErrorMessage());

        // If we failed, add the message to the retry queue,
        if (queue != null) {
          queue.addMessageStream(manager_server, message_out, "manager");
        }
        // Report the service as down to the tracker,
        tracker.reportServiceDownClientReport(manager_server, "manager");
      }
      else {
        // Message successfully sent,
        ++success_count;
      }
    }

    return success_count;
  }

  /**
   * Create a ServiceMessageQueue object for handling messages with this
   * object.
   */
  ServiceMessageQueue createServiceMessageQueue() {
    return new MCServiceMessageQueue();
  }

//  /**
//   * Send the message to the given root server. If the message can't be sent
//   * because the destination is not available, the message is added to the
//   * ServiceMessageQueue.  The caller can decide to enqueue the message queue
//   * or not.  Returns the number of messages that were successfully sent
//   * immediately.
//   */
//  int tellRoot(ServiceMessageQueue queue,
//               ServiceAddress root_server,
//               String message, Object... args) {
//
//    int success_count = 0;
//
//    // Create the mesage,
//    MessageStream message_out = new MessageStream(args.length + 3);
//    createMessage(message_out, message, args);
//
//    // Send the root message,
//    success_count += sendRootMessage(queue, root_server, message_out);
//
//    return success_count;
//  }
//
//  /**
//   * Send the message to the given root servers. If the message can't be sent
//   * because the destinations are not available, the message is added to the
//   * ServiceMessageQueue.  The caller can decide to enqueue the message queue
//   * or not.  Returns the number of messages that were successfully sent
//   * immediately.
//   */
//  int tellAllRoots(ServiceMessageQueue queue,
//                   ServiceAddress[] root_servers,
//                   String message, Object... args) {
//
//    int success_count = 0;
//
//    // Create the mesage,
//    MessageStream message_out = new MessageStream(args.length + 3);
//    createMessage(message_out, message, args);
//
//    // Process the messages,
//    for (ServiceAddress root_server : root_servers) {
//
//      // Send the root message,
//      success_count += sendRootMessage(queue, root_server, message_out);
//
//    }
//
//    return success_count;
//  }
//
//  /**
//   * Send the message to the given manager servers. If the message can't be
//   * sent because the destinations are not available, the message is added to
//   * the ServiceMessageQueue.  The caller can decide to enqueue the message
//   * queue or not.  Returns the number of messages that were successfully
//   * sent immediately.
//   */
//  int tellAllManagers(ServiceMessageQueue queue,
//                      ServiceAddress[] manager_servers,
//                      String message, Object... args) {
//
//    int success_count = 0;
//
//    // Create the mesage,
//    MessageStream message_out = new MessageStream(args.length + 3);
//    createMessage(message_out, message, args);
//
//    // Process the messages,
//    for (ServiceAddress manager_server : manager_servers) {
//
//      // Send the message message,
//      success_count += sendManagerMessage(queue, manager_server, message_out);
//
//    }
//
//    return success_count;
//  }
//
//
//
//  // ----- Functions -----
//
//  /**
//   * Performs a function on the given manager service and returns the result
//   * as a Message. If the manager server is down, or the function otherwise
//   * throws an exception during execution, the returned Message object returns
//   * true for 'isError'.
//   */
//  Message doFailableManagerFunction(ServiceAddress manager_server,
//                                    String message, Object... args) {
//
//    // Create the mesage,
//    MessageStream message_out = new MessageStream(args.length + 3);
//    createMessage(message_out, message, args);
//
//    return doManagerFunction(manager_server, message_out, false);
//  }
//
//
//  /**
//   * Performs a function on the given manager service and returns the result
//   * as a Message. If the manager server is down, or the function otherwise
//   * throws an exception during execution, then a runtime exception is
//   * generated.
//   */
//  Message doManagerFunction(ServiceAddress manager_server,
//                            String message, Object... args) {
//
//    // Create the mesage,
//    MessageStream message_out = new MessageStream(args.length + 3);
//    createMessage(message_out, message, args);
//
//    return doManagerFunction(manager_server, message_out, true);
//  }
//
//  /**
//   * Performs a function on the given root service and returns the result
//   * as a Message. If the manager server is down, or the function otherwise
//   * throws an exception during execution, then a runtime exception is
//   * generated.
//   */
//  Message doRootFunction(ServiceAddress root_server,
//                         String message, Object... args) {
//
//    // Create the mesage,
//    MessageStream message_out = new MessageStream(args.length + 3);
//    createMessage(message_out, message, args);
//
//    return doRootFunction(root_server, message_out, true);
//  }


  // ------ Inner classes ------


  private class RetryMessageQueue {

    private final ServiceAddress service_addr;
    private final ArrayList<MessageStream> queue;
    private final ArrayList<String> service_types;

    RetryMessageQueue(ServiceAddress addr) {
      this.service_addr = addr;
      this.queue = new ArrayList();
      this.service_types = new ArrayList();
    }

    void add(MessageStream message_stream, String type) {
      synchronized (queue) {
        queue.add(message_stream);
        service_types.add(type);
        if (log.isLoggable(Level.FINER)) {
          log.log(Level.FINER, "Queuing message {0} to service {1} {2}",
                   new Object[] { message_stream.toString(),
                                  service_addr.toString(), type });
        }
      }
    }

  }

  private class MCServiceMessageQueue extends ServiceMessageQueue {

    MCServiceMessageQueue() {
      super();
    }

    @Override
    public void enqueue() {
      int sz = service_addresses.size();
      for (int i = 0; i < sz; ++i) {
        ServiceAddress service_address = service_addresses.get(i);
        MessageStream message_stream = messages.get(i);
        String service_type = types.get(i);

        getRetryMessageQueue(service_address).add(message_stream, service_type);
      }
    }

  }

}
