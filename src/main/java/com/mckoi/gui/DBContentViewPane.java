/**
 * com.mckoi.gui.DBContentViewPane  Apr 30, 2010
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

package com.mckoi.gui;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Map;
import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;

/**
 * A JPanel that contains a generic database content view for one or more
 * PathViewer elements. The panel components show a tree element on the left
 * with a 'refresh' button above. The tree contains the paths being displayed.
 * The central area shows the selected item contents and any query components.
 *
 * @author Tobias Downer
 */

public class DBContentViewPane extends JPanel {

  private Map<String, PathViewer> path_map;

  private String selected_path_name;

  private final JTree select_list;
  private final NetworkContentTreeModel tree_model;

  private final JPanel switch_panel;
  private final JTabbedPane tab_pane;
  private final JPanel[] query_panel = new JPanel[3];
  private final JPanel view_object_panel;


  public DBContentViewPane(final Map<String, PathViewer> path_map) {
    this.path_map = path_map;

    setPreferredSize(new Dimension(850, 750));

    setLayout(new BorderLayout());

    // The left side list,
    select_list = new DBPathJTree();

    // new PathCellRenderer(select_list.getCellRenderer())
    DefaultTreeCellRenderer cell_renderer = new PathCellRenderer();
    select_list.setCellRenderer(cell_renderer);

    select_list.addTreeSelectionListener(new TreeSelectionListener() {
      public void valueChanged(TreeSelectionEvent evt) {
        selectItem(select_list.getSelectionPath());
      }
    });

    // Wrap it in a scroll pane,
    JScrollPane scrolly_table_list = new JScrollPane(select_list);
    scrolly_table_list.setPreferredSize(new Dimension(260, 0));

    // A button that refreshes the transaction,
    JButton refresh_snapshot = new JButton("Refresh Snapshot");
    refresh_snapshot.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {

        TreePath select_path = select_list.getSelectionPath();

        if (selected_path_name != null) {
          view_object_panel.removeAll();
          CardLayout switch_layout = (CardLayout) switch_panel.getLayout();
          switch_layout.show(switch_panel, "none");
          switch_panel.validate();
          PathViewer viewer = path_map.get(selected_path_name);
          viewer.refresh();
          tree_model.refresh(viewer.getPathTree());

//          TreeModel model = select_list.getModel();

          select_list.setSelectionPath(select_path);

        }

      }
    });

    JPanel left_panel = new JPanel();
    left_panel.setLayout(new BorderLayout());
    left_panel.add(scrolly_table_list, BorderLayout.CENTER);
    left_panel.add(refresh_snapshot, BorderLayout.NORTH);

    // Make the panel where we switch the view between tables and files
    switch_panel = new JPanel();
    switch_panel.setLayout(new CardLayout());

    view_object_panel = new JPanel();
    view_object_panel.setLayout(new BorderLayout());

    // Add the different views to the switch panel
    switch_panel.add(new JLabel("Select an item to view", JLabel.CENTER),
                     "none");
    switch_panel.add(view_object_panel, "view");

    tree_model = new NetworkContentTreeModel("Paths");

    for (String path : path_map.keySet()) {
      tree_model.addPath(path_map.get(path).getPathTree());
    }
    select_list.setModel(tree_model);
    select_list.setRowHeight(20);
    select_list.setLargeModel(true);

    tab_pane = new JTabbedPane();
    tab_pane.addTab("Item", switch_panel);
    for (int i = 0; i < query_panel.length; ++i) {
      query_panel[i] = new JPanel();
      query_panel[i].setLayout(new BorderLayout());
      query_panel[i].add(
              new JLabel("Select a database path to query", SwingConstants.CENTER),
              BorderLayout.CENTER);
      tab_pane.addTab("Query " + (i + 1), query_panel[i]);
    }

    // The split pane.
    JSplitPane split_pane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
    split_pane.setLeftComponent(left_panel);
    split_pane.setRightComponent(tab_pane);

    add(split_pane, BorderLayout.CENTER);

  }

  private void selectItem(TreePath select_path) {
    for (int i = 0; i < query_panel.length; ++i) {
      query_panel[i].removeAll();
      query_panel[i].add(
              new JLabel("Select a database path to query",
                         SwingConstants.CENTER),
              BorderLayout.CENTER);
      query_panel[i].validate();
      query_panel[i].repaint();
    }

    if (select_path != null && select_path.getPathCount() >= 2) {

      TreeNode path_component = (TreeNode) select_path.getPathComponent(1);
      String path_name = path_component.toString();
      selected_path_name = path_name;

      PathViewer path_viewer = path_map.get(path_name);

      for (int i = 0; i < query_panel.length; ++i) {
        JComponent query_component =
                              path_viewer.queryComponent("Query " + (i + 1));
        query_panel[i].removeAll();
        query_panel[i].add(query_component, BorderLayout.CENTER);
        query_panel[i].validate();
        query_panel[i].repaint();
      }

      JComponent ob_view = path_viewer.getObjectView(select_path);

      if (ob_view != null) {
        view_object_panel.removeAll();
        view_object_panel.add(
                path_viewer.getObjectView(select_path),
                BorderLayout.CENTER);

        CardLayout switch_layout = (CardLayout) switch_panel.getLayout();
        switch_layout.show(switch_panel, "view");
        tab_pane.setSelectedIndex(tab_pane.indexOfTab("Item"));
      }
      else {
        view_object_panel.removeAll();
        CardLayout switch_layout = (CardLayout) switch_panel.getLayout();
        switch_layout.show(switch_panel, "none");
      }

    }
    else {
      view_object_panel.removeAll();
      CardLayout switch_layout = (CardLayout) switch_panel.getLayout();
      switch_layout.show(switch_panel, "none");
      selected_path_name = null;
    }
    switch_panel.validate();

  }



  private static class PathCellRenderer extends DefaultTreeCellRenderer {

    @Override
    public Component getTreeCellRendererComponent(JTree tree, Object value,
                               boolean sel, boolean expanded, boolean leaf,
                               int row, boolean hasFocus) {

      return super.getTreeCellRendererComponent(tree, value,
                                           sel, expanded, leaf, row, hasFocus);

//      Rectangle r = getBounds();
//      System.out.println(r);
//
//      if (sel == false) {
//        setOpaque(false);
//      }
//      return this;
    }

  }



  private static class DBPathJTree extends JTree {

    public DBPathJTree() {
      setOpaque(false);
    }


    public void paint(Graphics g) {
      g.setColor(Color.gray);
      Rectangle r = g.getClipBounds();

      TreePath path_top = getClosestPathForLocation(0, r.y);

      if (path_top == null) {
        return;
      }

      int row = getRowForPath(path_top);

      ArrayList<Rectangle> breaks = new ArrayList();

      int y = r.y;
      while (true) {
        TreePath path = getPathForRow(row);
        if (path == null) {
          break;
        }
        Rectangle path_rect = getPathBounds(path);
        if (path_rect.y > r.y + r.height) {
          // We are done,
          break;
        }

        // Render the background,

        String right_align_str = "";

        if (path.getPathCount() == 2) {
//          bg_col = Color.decode("#eaeaf8");
          Object ob = path.getPathComponent(1);
          if (ob instanceof DBPathNode) {
            DBPathNode db_node = (DBPathNode) ob;
            right_align_str = db_node.getPathType();
          }
        }
        g.setColor(Color.white);
        g.fillRect(r.x, path_rect.y, r.width, path_rect.height);

        if (!right_align_str.equals("")) {
          int wid = getSize().width;
          JLabel l = new JLabel(right_align_str, JLabel.RIGHT);
          l.setBorder(new EmptyBorder(0, 2, 0, 2));
          l.setOpaque(false);
          l.setBounds(0, 0, wid, path_rect.height);
          l.setForeground(Color.gray);
          Graphics ng = g.create(0, path_rect.y, wid, path_rect.height);

          l.paint(ng);

          g.setColor(Color.white);
          g.fillRect(path_rect.x, path_rect.y,
                     path_rect.width, path_rect.height);
          breaks.add(path_rect);
        }

        y = path_rect.y + path_rect.height;

        ++row;
      }

      g.setColor(Color.white);
      g.fillRect(r.x, y, r.width, (r.y + r.height) - y);
//      g.setColor(Color.lightGray);
//      g.drawLine(r.x, y + 1, r.x + r.width, y + 1);

      super.paint(g);

      for (Rectangle path_rect : breaks) {
        g.setColor(Color.gray);
        g.drawLine(r.x, path_rect.y, r.x + r.width, path_rect.y);
      }

    }

  }

}
