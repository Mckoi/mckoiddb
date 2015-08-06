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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * An address/port of a service in an IP based network, represented as an
 * 128-bit field. The format of the address follows ipv6 conventions.
 *
 * @author Tobias Downer
 */

public final class ServiceAddress implements Comparable {

  /**
   * The network address in ipv6 format.
   */
  private final byte[] net_address;

  /**
   * The port of the host.
   */
  private final int port;

  /**
   * Cached display string.
   */
  private volatile String cached_display_string = null;

  /**
   * Constructs the address as a 16 byte ipv6 address and port value.
   */
  public ServiceAddress(byte[] net_address, int port) {
    if (net_address.length != 16) {
      throw new IllegalArgumentException(
                               "net_address must be a 16 byte ipv6 address");
    }
    this.net_address = net_address.clone();
    this.port = port;
  }

  /**
   * Constructs the address from an InetAddress and port value.
   */
  public ServiceAddress(InetAddress inet_address, int port) {
    net_address = new byte[16];
    this.port = port;
    byte[] b = inet_address.getAddress();
    // If the address is ipv4,
    if (b.length == 4) {
      // Format the network address as an 16 byte ipv6 on ipv4 network address.
      net_address[10] = (byte) 0x0FF;
      net_address[11] = (byte) 0x0FF;
      net_address[12] = b[0];
      net_address[13] = b[1];
      net_address[14] = b[2];
      net_address[15] = b[3];
    }
    // If the address is ipv6
    else if (b.length == 16) {
      for (int i = 0; i < 16; ++i) {
        net_address[i] = b[i];
      }
    }
    else {
      // Some future inet_address format?
      throw new RuntimeException("Unknown InetAddress address format");
    }
  }

  /**
   * Returns the network address as a 16 byte ipv6 network address. If the
   * address is a 4 byte ipv4 address, it is encoded as an 16 byte ipv6
   * address over a ipv4 network.
   */
  public byte[] getAddress() {
    return net_address.clone();
  }

  /**
   * Returns the port of the service.
   */
  public int getPort() {
    return port;
  }

  /**
   * Returns the address as an InetAddress. This will return either an ipv4
   * or ipv6 InetAddress depending on the format of the network address.
   */
  public InetAddress asInetAddress() {
    try {
      return InetAddress.getByAddress(net_address);
    }
    catch (UnknownHostException e) {
      // It should not be possible for this exception to be generated since
      // the API should have no need to look up a naming database (it's an
      // IP address!)
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /**
   * Reads a ServiceAddress encoded in a data input stream.
   */
  public static ServiceAddress readFrom(DataInput in) throws IOException {
    byte[] buf = new byte[16];
    for (int i = 0; i < 16; ++i) {
      buf[i] = in.readByte();
    }
    int port = in.readInt();
    return new ServiceAddress(buf, port);
  }

  /**
   * Encodes this ServiceAddress to a data output stream.
   */
  public void writeTo(DataOutput out) throws IOException {
    for (int i = 0; i < 16; ++i) {
      out.writeByte(net_address[i]);
    }
    out.writeInt(port);
  }


  /**
   * Parses a service address generated from 'formatString'.
   */
  public static ServiceAddress parseString(String str)
                                    throws IOException, NumberFormatException {
    int p = str.lastIndexOf(":");
    if (p == -1) {
      throw new IOException("Bad line format (didn't find ':') for: " + str);
    }
    String service_addr = str.substring(0, p);
    String service_port = str.substring(p + 1);

    // Map the address into an InetAddress,
    InetAddress service_inet = InetAddress.getByName(service_addr);
    // And now into a ServiceAddress
    return new ServiceAddress(service_inet, Integer.parseInt(service_port));
  }

  /**
   * Formats this ServiceAddress as a string that can be parsed by
   * 'parseString'.
   */
  public String formatString() {
    StringBuilder buf = new StringBuilder();
    buf.append(asInetAddress().getHostAddress());
    buf.append(":");
    buf.append(port);
    return buf.toString();
  }

  /**
   * Returns this ServiceAddress as a string suitable for display to a user.
   */
  public String displayString() {
    if (cached_display_string == null) {
      StringBuilder buf = new StringBuilder();
//      buf.append(asInetAddress().getCanonicalHostName());
      buf.append(asInetAddress().getHostAddress());
      buf.append(":");
      buf.append(port);
      cached_display_string = buf.toString();
    }
    return cached_display_string;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(asInetAddress().toString());
    buf.append(" port:");
    buf.append(port);
    return buf.toString();
  }

  /**
   * Parses a comma deliminated service address string and returns it as
   * an array of ServiceAddress objects. For example;
   * "192.168.1.100:40111 , 192.168.4.20:40111"
   */
  public static ServiceAddress[] parseServiceAddresses(
      String service_addresses_str) throws IOException, NumberFormatException {

    String[] service_addr_str_arr = service_addresses_str.split(",");
    int sz = service_addr_str_arr.length;
    ServiceAddress[] service_addr_arr = new ServiceAddress[sz];
    for (int i = 0; i < sz; ++i) {
      service_addr_arr[i] =
                   ServiceAddress.parseString(service_addr_str_arr[i].trim());
    }
    return service_addr_arr;

  }

  /**
   * Formats a list of service address objects into a comma deliminated string.
   * This is the formatting side of 'parseServiceAddresses'.
   */
  public static String formatServiceAddresses(ServiceAddress[] arr) {

    StringBuilder b = new StringBuilder();
    boolean first = true;
    for (ServiceAddress addr : arr) {
      if (!first) {
        b.append(" , ");
      }
      b.append(addr.formatString());
      first = false;
    }
    return b.toString();

  }

  // -- Hashcode and equals implementation, so this can be a key in a map --

  @Override
  public int hashCode() {
    int v = 0;
    for (int i = 0; i < net_address.length; ++i) {
      v = 31 * v + ((int) net_address[i] & 0x0FF);
    }
    v = 31 * v + port;
    return v;
  }

  @Override
  public boolean equals(Object ob) {
    if (this == ob) {
      return true;
    }
    ServiceAddress dest_ob = (ServiceAddress) ob;
    if (port != dest_ob.port) {
      return false;
    }
    // Return false on the first net address that isn't equal,
    for (int i = 0; i < net_address.length; ++i) {
      if (dest_ob.net_address[i] != net_address[i]) {
        return false;
      }
    }
    // Otherwise the objects match,
    return true;
  }

  @Override
  public int compareTo(Object ob) {
    if (this == ob) {
      return 0;
    }
    ServiceAddress dest_ob = (ServiceAddress) ob;
    // Net items comparison
    for (int i = 0; i < net_address.length; ++i) {
      byte dbi = dest_ob.net_address[i];
      byte sbi = net_address[i];
      if (dest_ob.net_address[i] != net_address[i]) {
        return (int) sbi - (int) dbi;
      }
    }
    // Port comparison
    return (int) port - (int) dest_ob.port;
  }

}
