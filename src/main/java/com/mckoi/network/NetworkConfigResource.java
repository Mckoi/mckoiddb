/**
 * com.mckoi.network.NetworkConfigResource  Jul 19, 2009
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

import com.mckoi.util.StringUtil;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The general configuration properties of a node that may change over the
 * lifespan of a node session. The node configuration is typically located
 * on a server accessible by all the nodes on the network. The current
 * configuration values are periodically refreshed so that a change in the
 * status of the network can be immediately reflected in all the nodes in the
 * network.
 * <p>
 * The node configuration includes two important properties, 'connect_whitelist'
 * which is a comma deliminated list of all IP addresses that a node is
 * permitted to talk with, and 'network_nodelist' which is a list of the
 * addresses of all nodes in the network. For example,
 * <p>
 * <pre>
 *  connect_whitelist=127.0.0.1,\
 *     192.168.12.10, 192.168.12.11,\
 *     192.168.12.12, 192.168.12.15,\
 *     192.168.13.100
 *
 *  network_nodelist=\
 *     192.168.12.10:3500, 192.168.12.11:3500,\
 *     192.168.12.12:3500, 192.168.12.15:3500,\
 *     192.168.13.100:3500
 * </pre>
 *
 * @author Tobias Downer
 */

public abstract class NetworkConfigResource {

  /**
   * The logger.
   */
  protected final Logger log;

  /**
   * The modification time of the configuration resource the last time it
   * was accessed and read.
   */
  private long last_read;

  /**
   * True if the configuration states all IPs are allowed to connect.
   */
  private boolean allow_all_ips = false;

  /**
   * A HashSet of all IP addresses allowed.
   */
  private HashSet<String> allowed_ips = new HashSet();

  /**
   * A list of all 'catch-all' ip addresses.
   */
  private ArrayList<String> catchall_allowed_ips = new ArrayList();

  /**
   * The list of all machine nodes.
   */
  private String all_machine_nodes = "";

  /**
   * The timeout value of configuration checks in seconds.
   */
  private int configcheck_timeout;


  private final Object state_lock = new Object();

  /**
   * Constructor.
   */
  public NetworkConfigResource() {
    this.log = Logger.getLogger("com.mckoi.network.Log");
  }

  /**
   * Loads the resource into memory.
   */
  protected abstract void loadResource() throws IOException;

  /**
   * Retrieves the 'last modified' time of the configuration resource. Returns
   * -1 if the last modification time of the resource is unknown. This is used
   * to skip updating a configuration if the file hasn't changed.
   */
  protected abstract long getLastModifiedTime() throws IOException;

  /**
   * Fetches the configuration node properties from the storage global
   * storage component.
   */
  protected abstract Properties refreshNodeProperties() throws IOException;

  /**
   * Loads the configuration properties, generating an IOException if the
   * configuration properties could not be fetched because the resource
   * is not available.
   */
  public void load() throws IOException {
    loadResource();
    long lmt = getLastModifiedTime();
    if (lmt == -1 || lmt != last_read) {
      last_read = lmt;

      Properties p = refreshNodeProperties();

      String connect_whitelist = p.getProperty("connect_whitelist");
      String network_nodelist = p.getProperty("network_nodelist");

      if (connect_whitelist == null || connect_whitelist.equals("")) {
        throw new IOException("Unable to find 'connect_whitelist' property in " +
                              "the network configuration resource.");
      }
      if (network_nodelist == null || network_nodelist.equals("")) {
        throw new IOException("Unable to find 'network_nodelist' property in " +
                              "the network configuration resource.");
      }

      int set_conf_timeout = 2 * 60;
      String conf_timeout = p.getProperty("configcheck_timeout");
      if (conf_timeout != null) {
        conf_timeout = conf_timeout.trim();
        try {
          set_conf_timeout = Integer.parseInt(conf_timeout);
        }
        catch (Throwable e) {
          log.log(Level.WARNING, "configcheck_timeout property did not parse to an integer");
        }
      }

      log.info("Updating from new network configuration.");

      HashSet<String> all_ips = new HashSet();
      ArrayList<String> call_allowed_ips = new ArrayList();
      boolean alla_ips = false;

      // Is it catchall whitelist?
      if (connect_whitelist.trim().equals("*")) {
        alla_ips = true;
      }
      else {
        List<String> whitelist_ips = StringUtil.explode(connect_whitelist, ",");
        for (String ip : whitelist_ips) {
          ip = ip.trim();
          // Is it a catch all ip address?
          if (ip.endsWith(".*")) {
            // Add to the catchall list,
            call_allowed_ips.add(ip.substring(0, ip.length() - 2));
          }
          else {
            // Add to the ip hashset
            all_ips.add(ip);
          }
        }
      }

      // Synchronize on 'state_lock' while we update the state,
      synchronized (state_lock) {
        // The list of all machine nodes,
        all_machine_nodes = network_nodelist;
        allow_all_ips = alla_ips;
        catchall_allowed_ips = call_allowed_ips;
        allowed_ips = all_ips;
        configcheck_timeout = set_conf_timeout;
      }

    }

  }

  /**
   * Returns true if the given ip address is allowed (whitelisted).
   */
  public boolean isIPAllowed(String ip_address) {
    synchronized (state_lock) {
      // The catchall,
      if (allow_all_ips == true) {
        return true;
      }
      // Check the map,
      if (allowed_ips.contains(ip_address)) {
        return true;
      }
      // Check the catch all list,
      for (String expr : catchall_allowed_ips) {
        if (ip_address.startsWith(expr)) {
          return true;
        }
      }
      // No matches,
      return false;
    }
  }

  /**
   * Returns the 'network_nodelist' property from the configuration.
   */
  public String getNetworkNodelist() {
    synchronized (state_lock) {
      return all_machine_nodes;
    }
  }

  /**
   * Returns the amount of time, in seconds, between checks of the resource
   * file as set by the configuration property 'configcheck_timeout'.
   */
  public int getCheckTimeout() {
    synchronized (state_lock) {
      return configcheck_timeout;
    }
  }


  // ----- Statics


  /**
   * Returns an implementation of NetworkConfigResource based on a URL
   * resource.
   */
  public static NetworkConfigResource getNetConfig(URL url) {
    return new URLNetworkConfigResource(url);
  }

  /**
   * Returns an implementation of NetworkConfigResource based on a File
   * resource.
   */
  public static NetworkConfigResource getNetConfig(File f) {
    return new FileNetworkConfigResource(f);
  }

  /**
   * Parses a network configuration string. Either the string starts with
   * http://, ftp://, etc, and is considered a URL, or starts with file://,
   * or nothing and is considered a file.
   */
  public static NetworkConfigResource parse(String file_string) {
    try {
      if (file_string.startsWith("http://") ||
          file_string.startsWith("ftp://")) {
        // Return it as a URL
        return getNetConfig(new URL(file_string));
      }
      else {
        File f;
        if (file_string.startsWith("file://")) {
          f = new File(file_string.substring(7));
        }
        else {
          f = new File(file_string);
        }
        if (!f.exists() || !f.isFile()) {
          throw new RuntimeException("Network Config file not found: " + file_string);
        }
        // Return it as a file,
        return getNetConfig(f);
      }
    }
    catch (MalformedURLException e) {
      throw new RuntimeException("URL Error ", e);
    }
  }


  // ----- Inner classes

  private static class URLNetworkConfigResource extends NetworkConfigResource {

    private final URL url;

    private long last_modified;
    private Properties properties;

    URLNetworkConfigResource(URL url) {
      this.url = url;
    }

    @Override
    protected void loadResource() throws IOException {
      URLConnection c = url.openConnection();
      c.connect();
      // Get the last modified time,
      last_modified = c.getLastModified();
      if (last_modified == 0) {
        last_modified = -1;
      }
      // Load the file into a Properties object,
      InputStream in = c.getInputStream();
      Properties p = new Properties();
      p.load(in);
      in.close();
      properties = p;
    }

    @Override
    protected long getLastModifiedTime() throws IOException {
      return last_modified;
    }

    @Override
    protected Properties refreshNodeProperties() throws IOException {
      return properties;
    }

  }

  private static class FileNetworkConfigResource extends NetworkConfigResource {

    private final File file;

    FileNetworkConfigResource(File file) {
      this.file = file;
    }

    @Override
    protected void loadResource() throws IOException {
      // Nothing to do,
    }

    @Override
    protected long getLastModifiedTime() throws IOException {
      long v = file.lastModified();
      if (v == 0) {
        return -1;
      }
      else {
        return v;
      }
    }

    @Override
    protected Properties refreshNodeProperties() throws IOException {
      InputStream fin = new BufferedInputStream(new FileInputStream(file));
      Properties p = new Properties();
      p.load(fin);
      fin.close();
      return p;
    }

  }

}
