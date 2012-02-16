/**
 * com.mckoi.network.ProxyNetworkConnector  Jul 19, 2009
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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;

/**
 * An implementation of NetworkConnector that channels all commands through
 * a single InputStream/OutputStream channel.
 *
 * @author Tobias Downer
 */

public class ProxyNetworkConnector implements NetworkConnector {

  /**
   * The network password.
   */
  private final String net_password;

  /**
   * The input and output communication stream with the proxy server.
   */
  private DataInputStream pin;
  private DataOutputStream pout;
  private final Object proxy_lock = new Object();

  String init_string = null;


  /**
   * Constructs the connector.
   */
  public ProxyNetworkConnector(String net_password) {
    this.net_password = net_password;
  }

  /**
   * Attempts to connect to the proxy server.
   */
  public void connect(InputStream in, OutputStream out) {
    pin = new DataInputStream(new BufferedInputStream(in));
    pout = new DataOutputStream(new BufferedOutputStream(out));

    try {
      // Perform the handshake,
      long v = pin.readLong();
      pout.writeLong(v);
      pout.flush();
      init_string = pin.readUTF();
      pout.writeUTF(net_password);
      pout.flush();
    }
    catch (IOException e) {
      throw new RuntimeException("IO Error", e);
    }
    // Done,
  }

  public void stop() {
    try {
      synchronized (proxy_lock) {
        pout.writeChar('0');
        pout.flush();
      }
      pin.close();
      pout.close();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    finally {
      init_string = null;
      pin = null;
      pout = null;
    }
  }

  /**
   * Connects to the instance administration component of the given address.
   */
  public MessageProcessor connectInstanceAdmin(ServiceAddress address) {
    return new RemoteMessageProcessor(address, 'a');
  }

  /**
   * Connects to a block server at the given address.
   */
  public MessageProcessor connectBlockServer(ServiceAddress address) {
    return new RemoteMessageProcessor(address, 'b');
  }

  /**
   * Connects to a manager server at the given address.
   */
  public MessageProcessor connectManagerServer(ServiceAddress address) {
    return new RemoteMessageProcessor(address, 'm');
  }

  /**
   * Connects to a root server at the given address.
   */
  public MessageProcessor connectRootServer(ServiceAddress address) {
    return new RemoteMessageProcessor(address, 'r');
  }



  /**
   * A message processor on the given remote address.
   */
  private class RemoteMessageProcessor implements MessageProcessor {

    /**
     * The remote address,
     */
    private final ServiceAddress address;

    /**
     * The command dispatcher code.
     */
    private final char command_code;

    /**
     * The message dictionary.
     */
    private final HashMap<String, String> message_dictionary;

    /**
     * Constructor.
     */
    RemoteMessageProcessor(ServiceAddress address, char c) {
      if (address == null) {
        throw new NullPointerException();
      }
      this.address = address;
      this.command_code = c;
      this.message_dictionary = new HashMap();
    }

    public MessageStream process(MessageStream msg_stream) {
      try {
        synchronized (proxy_lock) {
          // Write the message.
          pout.writeChar(command_code);
          address.writeTo(pout);
          msg_stream.writeTo(pout, message_dictionary);
          pout.flush();

          // Fetch the result,
          MessageStream msg_result =
                              MessageStream.readFrom(pin, message_dictionary);
          // And return it,
          return msg_result;
        }

      }
      catch (IOException e) {
        // Probably caused because the proxy closed the connection when a
        // timeout was reached.
        throw new RuntimeException("IO Error", e);
//        MessageStream msg_result = new MessageStream(16);
//        msg_result.addMessage("E");
//        if (e instanceof EOFException) {
//          msg_result.addExternalThrowable(new ExternalThrowable(
//                                          new RuntimeException("EOF", e)));
//        }
//        else {
//          // Report this error as a msg_stream fault,
//          msg_result.addExternalThrowable(new ExternalThrowable(
//                                    new RuntimeException(e.getMessage(), e)));
//        }
//        msg_result.closeMessage();
//        return msg_result;
      }
    }

  }

}
