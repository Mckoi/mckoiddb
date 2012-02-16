/**
 * com.mckoi.network.LocalFileSystemManagerServer  Nov 25, 2008
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

import com.mckoi.data.LocalFileSystemDatabase;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Timer;

/**
 * An implementation of a manager server where the block to server map is
 * a local file system database.
 *
 * @author Tobias Downer
 */

public class LocalFileSystemManagerServer extends DefaultManagerServer {

  /**
   * The base path.
   */
  private final File base_path;
  
  /**
   * The path of the database.
   */
  private final File dbpath;

  /**
   * The block database, created when 'start' is called.
   */
  private LocalFileSystemDatabase local_db;

  /**
   * Constructor.
   */
  public LocalFileSystemManagerServer(NetworkConnector network,
                                      File base_path, File dbpath,
                                      ServiceAddress this_service,
                                      Timer timer) {
    super(network, this_service, timer);
    this.base_path = base_path;
    this.dbpath = dbpath;
  }



  /**
   * Persists the registered block server list.
   */
  void persistBlockServerList(List<MSBlockServer> server_list) {
    try {
      File f = new File(base_path, REGISTERED_BLOCK_SERVERS);
      if (!f.exists()) {
        f.createNewFile();
      }
      else {
        f.delete();
        f.createNewFile();
      }
      
      FileWriter fr = new FileWriter(f);
      PrintWriter out = new PrintWriter(fr);
      for (MSBlockServer s : server_list) {
        out.print(s.server_guid);
        out.print(",");
        out.println(s.address.formatString());
      }

      out.flush();
      out.close();

    }
    catch (IOException e) {
      throw new RuntimeException("Error persisting block server list: " +
                                 e.getMessage());
    }
  }

  /**
   * Persists the registered root server list.
   */
  void persistRootServerList(List<MSRootServer> server_list) {
    try {
      File f = new File(base_path, REGISTERED_ROOT_SERVERS);
      if (!f.exists()) {
        f.createNewFile();
      }
      else {
        f.delete();
        f.createNewFile();
      }

      FileWriter fr = new FileWriter(f);
      PrintWriter out = new PrintWriter(fr);
      for (MSRootServer s : server_list) {
        out.println(s.address.formatString());
      }

      out.flush();
      out.close();

    }
    catch (IOException e) {
      throw new RuntimeException("Error persisting root server list: " +
                                 e.getMessage());
    }
  }

  /**
   * Persists the registered manager list.
   */
  void persistManagerServerList(List<MSManagerServer> servers_list) {
    try {
      File f = new File(base_path, REGISTERED_MANAGER_SERVERS);
      if (!f.exists()) {
        f.createNewFile();
      }
      else {
        f.delete();
        f.createNewFile();
      }

      FileWriter fr = new FileWriter(f);
      PrintWriter out = new PrintWriter(fr);
      for (MSManagerServer s : servers_list) {
        out.println(s.address.formatString());
      }

      out.flush();
      out.close();

    }
    catch (IOException e) {
      throw new RuntimeException("Error persisting manager server list: " +
                                 e.getMessage());
    }
  }

  /**
   * Persists the unique id value.
   */
  void persistManagerUniqueId(int unique_id) {
    try {
      File f = new File(base_path, MANAGER_PROPERTIES);
      if (!f.exists()) {
        f.createNewFile();
      }
      else {
        f.delete();
        f.createNewFile();
      }

      FileWriter fr = new FileWriter(f);
      PrintWriter out = new PrintWriter(fr);
      out.println("id=" + unique_id);

      out.flush();
      out.close();

    }
    catch (IOException e) {
      throw new RuntimeException("Error persisting unique id: " +
                                 e.getMessage());
    }
  }



  /**
   * Starts the manager server.
   */
  public void start() throws IOException {
    local_db = new LocalFileSystemDatabase(dbpath);
    local_db.start();

    setBlockDatabase(local_db);

    // Read the unique id value,
    File f = new File(base_path, MANAGER_PROPERTIES);
    if (f.exists()) {
      BufferedReader rin = new BufferedReader(new FileReader(f));
      while (true) {
        String line = rin.readLine();
        if (line == null) {
          break;
        }
        if (line.startsWith("id=")) {
          int unique_id = Integer.parseInt(line.substring(3));
          setManagerUniqueId(unique_id);
//          System.out.println("ID = " + unique_id);
        }
      }
      rin.close();
    }

    // Read all the registered block servers that were last persisted and
    // populate the manager with them,
    f = new File(base_path, REGISTERED_BLOCK_SERVERS);
    if (f.exists()) {
      BufferedReader rin = new BufferedReader(new FileReader(f));
      while (true) {
        String line = rin.readLine();
        if (line == null) {
          break;
        }
        int p = line.indexOf(",");
        long guid = Long.parseLong(line.substring(0, p));
        ServiceAddress addr = ServiceAddress.parseString(line.substring(p + 1));
        addRegisteredBlockServer(guid, addr);
      }
      rin.close();
    }

    // Read all the registered root servers that were last persisted and
    // populate the manager with them,
    f = new File(base_path, REGISTERED_ROOT_SERVERS);
    if (f.exists()) {
      BufferedReader rin = new BufferedReader(new FileReader(f));
      while (true) {
        String line = rin.readLine();
        if (line == null) {
          break;
        }
        ServiceAddress addr = ServiceAddress.parseString(line);
        addRegisteredRootServer(addr);
      }
      rin.close();
    }

    // Read all the registered manager servers that were last persisted and
    // populate the manager with them,
    f = new File(base_path, REGISTERED_MANAGER_SERVERS);
    if (f.exists()) {
      BufferedReader rin = new BufferedReader(new FileReader(f));
      while (true) {
        String line = rin.readLine();
        if (line == null) {
          break;
        }
        ServiceAddress addr = ServiceAddress.parseString(line);
        addRegisteredManagerServer(addr);
      }
      rin.close();
    }

    // Perform the initialization procedure (contacts the other managers and
    // syncs data).
    super.doStart();

  }

  /**
   * Stops the manager server.
   */
  public void stop() throws IOException {
    try {
      local_db.stop();
    }
    finally {
      super.doStop();
    }
  }


  // ----- Statics -----

  private final static String REGISTERED_BLOCK_SERVERS = "blockservers";
  private final static String REGISTERED_ROOT_SERVERS = "rootservers";
  private final static String REGISTERED_MANAGER_SERVERS = "managerservers";
  private final static String MANAGER_PROPERTIES = "manager.properties";

}
