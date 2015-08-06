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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/**
 * A connector that enables communication to a MckoiDDB network via a proxy
 * server. This is useful for setting up a system where a client can connect
 * to a network of MckoiDDB nodes remotely. Using this connector, when the
 * client requests information from a server the request is sent to the proxy
 * which performs the command on the the clients behalf on the network, and
 * the result is returned to the client.
 * <p>
 * Note that all network communication using this connector is not encrypted
 * so it is not appropriate for use over a public network or the Internet.
 * This type of connection could be made secure if tunneled through a secure
 * channel (such as ssh).
 *
 * @author Tobias Downer
 */

class TCPProxyNetworkConnector extends ProxyNetworkConnector {

  /**
   * Constructs the connector.
   */
  public TCPProxyNetworkConnector(String net_password) {
    super(net_password);
  }

  /**
   * Attempts to connect to the proxy server.
   */
  public void connect(InetAddress proxy_address, int proxy_port) {
    try {
      Socket socket = new Socket(proxy_address, proxy_port);
      super.connect(socket.getInputStream(), socket.getOutputStream());
    }
    catch (IOException e) {
      throw new RuntimeException("IO Error", e);
    }
  }

}
