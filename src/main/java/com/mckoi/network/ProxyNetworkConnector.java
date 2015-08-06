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

package com.mckoi.network;

import java.io.*;
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
    
    // Security check,
    SecurityManager security = System.getSecurityManager();
    if (security != null) security.checkPermission(
                                MckoiNetworkPermission.CREATE_PROXY_CONNECTOR);
    
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

  @Override
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
      e.printStackTrace(System.err);
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
  @Override
  public MessageProcessor connectInstanceAdmin(ServiceAddress address) {
    return new RemoteMessageProcessor(address, 'a');
  }

  /**
   * Connects to a block server at the given address.
   */
  @Override
  public MessageProcessor connectBlockServer(ServiceAddress address) {
    return new RemoteMessageProcessor(address, 'b');
  }

  /**
   * Connects to a manager server at the given address.
   */
  @Override
  public MessageProcessor connectManagerServer(ServiceAddress address) {
    return new RemoteMessageProcessor(address, 'm');
  }

  /**
   * Connects to a root server at the given address.
   */
  @Override
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

    @Override
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
