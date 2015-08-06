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

package com.mckoi.runtime;

import com.mckoi.gui.*;
import com.mckoi.network.MckoiDDBAccess;
import com.mckoi.network.MckoiDDBClient;
import com.mckoi.network.MckoiDDBClientUtils;
import com.mckoi.network.ServiceAddress;
import com.mckoi.odb.ODBSession;
import com.mckoi.odb.ODBTransaction;
import com.mckoi.sdb.SDBSession;
import com.mckoi.sdb.SDBTransaction;
import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.UIManager;
import javax.swing.UIManager.LookAndFeelInfo;
import javax.swing.UnsupportedLookAndFeelException;

/**
 * A simple tool for browsing and viewing an SDB database.
 *
 * @author Tobias Downer
 */

public class DBViewer {

  public static void main(String[] args) {

    String look_and_feel = UIManager.getSystemLookAndFeelClassName();
    LookAndFeelInfo[] lnf_set = UIManager.getInstalledLookAndFeels();
    for (LookAndFeelInfo i : lnf_set) {
      if (i.getClassName().endsWith("NimbusLookAndFeel")) {
        look_and_feel = i.getClassName();
      }
    }

    // Install the look and feel
    try {
      UIManager.setLookAndFeel(look_and_feel);
    } catch (InstantiationException e) {
    } catch (ClassNotFoundException e) {
    } catch (UnsupportedLookAndFeelException e) {
    } catch (IllegalAccessException e) {
    }

    try {

      if (args.length == 1) {

        // Load from client configuration file,
        File client_config_file = new File(args[0]);

        MckoiDDBClient client;
        try {
          MckoiDDBClient tcp_client =
                            MckoiDDBClientUtils.connectTCP(client_config_file);
          client = tcp_client;

          // Open the viewer window
          openViewerWindow(client);

        }
        catch (Throwable e) {
          JOptionPane.showMessageDialog(null,
                            "Unable to connect to network: " + e.getMessage());
          e.printStackTrace(System.err);
          return;
        }

      }
      else {

        System.out.println("com.mckoi.runtime.DBViewer");
        System.out.println();
        System.out.println("Syntax: DBViewer [location of client.conf]");

//        // Open the connect interface,
//
//        final LoginPanel login_panel = new LoginPanel();
//        final JFrame login_frame = new JFrame("Simple Database Viewer Connect");
//        Container c = login_frame.getContentPane();
//        c.setLayout(new BorderLayout());
//        c.add(login_panel, BorderLayout.CENTER);
//
//        login_frame.addWindowListener(new WindowAdapter() {
//          @Override
//          public void windowClosing(WindowEvent evt) {
//            login_frame.dispose();
//            System.exit(0);
//          }
//        });
//        login_panel.addExitActionListener(new ActionListener() {
//          @Override
//          public void actionPerformed(ActionEvent evt) {
//            login_frame.dispose();
//            System.exit(0);
//          }
//        });
//        login_panel.addConnectActionListener(new ActionListener() {
//          @Override
//          public void actionPerformed(ActionEvent evt) {
//            tryConnect(login_frame, login_panel);
//          }
//        });
//
//        login_frame.pack();
//        login_frame.setVisible(true);
//        login_frame.setLocationRelativeTo(null);
      }

    }
    catch (Throwable e) {
      e.printStackTrace(System.err);
    }

  }


//  private static void tryConnect(
//                     final JFrame login_frame, final LoginPanel login_panel) {
//    try {
//      // Test the args,
//      String manager = login_panel.getManagerServerField();
//      String net_pass = login_panel.getNetworkPasswordField();
//
//      if (manager == null || manager.equals("")) {
//        JOptionPane.showMessageDialog(login_frame,
//                                      "Manager server address is empty.");
//        return;
//      }
//      if (net_pass == null || net_pass.equals("")) {
//        JOptionPane.showMessageDialog(login_frame,
//                                      "Network password is empty.");
//        return;
//      }
//
//      ServiceAddress MANAGER;
//      try {
//        MANAGER = ServiceAddress.parseString(manager);
//      }
//      catch (Throwable e) {
//        JOptionPane.showMessageDialog(login_frame,
//                           "Manager server address error: " + e.getMessage());
//        e.printStackTrace(System.err);
//        return;
//      }
//
//      MckoiDDBClient client;
//      // This is a direct connection,
//      try {
//        ServiceAddress[] managers = new ServiceAddress[] { MANAGER };
//        MckoiDDBClient tcp_client =
//                           MckoiDDBClientUtils.connectTCP(managers, net_pass);
//        client = tcp_client;
//      }
//      catch (Throwable e) {
//        JOptionPane.showMessageDialog(login_frame,
//                           "Unable to connect to network: " + e.getMessage());
//        e.printStackTrace(System.err);
//        return;
//      }
//
//      // Everything good so far, so dispose the login frame and open the
//      // viewer frame.
//
//      // Dispose the login frame,
//      login_frame.dispose();
//
//      openViewerWindow(client);
//
//
//    }
//    catch (Throwable e) {
//      e.printStackTrace(System.err);
//    }
//  }


  private static void openViewerWindow(MckoiDDBAccess client) {

    // Fetch the list of paths accessible to the client,
    String[] path_array = client.queryAllNetworkPaths();

    // Map from path names to viewer
    HashMap<String, PathViewer> path_viewer_map = new HashMap();

    // Make a list of SDBPathNode objects,
    ArrayList<PathViewer> paths = new ArrayList();
    for (int i = 0; i < path_array.length; ++i) {
      String path_name = path_array[i];
      String consensus_function = client.getConsensusFunction(path_name);
      PathViewer path_viewer;

      if (consensus_function.equals("com.mckoi.sdb.SimpleDatabase")) {
        SDBSession session = new SDBSession(client, path_name);
        SDBTransaction transaction = session.createTransaction();
        path_viewer = new SDBViewer(path_name, session, transaction);
        paths.add(path_viewer);
      }
      // For Object data model
      else if (consensus_function.equals("com.mckoi.odb.ObjectDatabase")) {
        ODBSession session = new ODBSession(client, path_name);
        ODBTransaction transaction = session.createTransaction();
        path_viewer = new ODBViewer(path_name, session, transaction);
        paths.add(path_viewer);
      }
      // For SQL data model
      else if (consensus_function.equals("com.mckoi.tabledb.TableDatabase")) {
        try {
          // Use reflection to invoke the SQL viewer if the SQL package is
          // installed,
          ViewerFactory invoker = (ViewerFactory)
                Class.forName("com.mckoi.gui.TDBMViewerFactory").newInstance();
          path_viewer = invoker.createPathViewer(client, path_name);
          paths.add(path_viewer);
        }
        catch (ClassNotFoundException e) {
          throw new RuntimeException("Class Not Found", e);
        }
        catch (InstantiationException e) {
          throw new RuntimeException("Instantiation Exception", e);
        }
        catch (IllegalAccessException e) {
          throw new RuntimeException("Illegal Access", e);
        }

//        TDBMSSession session = new TDBMSSession(client, path_name);
//        SQLConnectionInterface interp = new LocalConnectionInterface(session);
//        interp.openTransaction();
//        path_viewer = new TDBMSViewer(path_name, interp);
//        paths.add(path_viewer);
      }

      else {
        path_viewer = null;
      }

      if (path_viewer != null) {
        path_viewer_map.put(path_name, path_viewer);
      }
    }

    // Open the SDB viewer frame
    final JFrame frame = new JFrame("DB Viewer");
    frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
    Container c = frame.getContentPane();
    c.setLayout(new BorderLayout());

    // Create the tables pane and refresh it
    DBContentViewPane tables_pane = new DBContentViewPane(path_viewer_map);

    c.add(tables_pane, BorderLayout.CENTER);

    frame.addWindowListener(new WindowAdapter() {
      @Override
      public void windowClosing(WindowEvent evt) {
        frame.dispose();
        System.exit(0);
      }
    });

    frame.pack();
    frame.setVisible(true);
//    frame.setSize(frame.getSize().width, 750);
    frame.setLocationRelativeTo(null);


  }

}
