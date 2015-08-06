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

package com.mckoi.gui;

import com.mckoi.odb.ODBTransaction;
import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.event.HyperlinkEvent;
import javax.swing.event.HyperlinkEvent.EventType;
import javax.swing.event.HyperlinkListener;

/**
 * ODBBrowser implements a user interface for browsing through an Object
 * Database object graph. It functions similar to a web browser in so far as
 * it displays object information on a page with hyperlinks to representing
 * the object references, a back and forward button, and a location bar.
 *
 * @author Tobias Downer
 */

public class ODBBrowser extends JPanel {

  /**
   * The transaction being browsed.
   */
  private ODBTransaction transaction;

  /**
   * The location bar.
   */
  private JTextField location_bar;

  /**
   * The status bar.
   */
  private JLabel status_bar;

  /**
   * The back and forward buttons.
   */
  private JButton back;
  private JButton forward;

  /**
   * The editor view pane.
   */
  private JEditorPane view_pane;

  /**
   * The history of all locations visited.
   */
  private ArrayList<String> history;

  /**
   * The current position in the history array being viewed.
   */
  private int history_pos;



  /**
   * Constructor.
   */
  public ODBBrowser(ODBTransaction transaction) {
    this.transaction = transaction;

    this.history = new ArrayList();
    this.history_pos = 0;

    setLayout(new BorderLayout());

    JPanel top_bar = new JPanel();
    top_bar.setLayout(new BorderLayout());

    JPanel button_panel = new JPanel();
    button_panel.setLayout(new BorderLayout());

    status_bar = new JLabel(" ");
    status_bar.setBorder(new EmptyBorder(1, 6, 1, 6));

    back = new JButton("<");
    forward = new JButton(">");
    location_bar = new JTextField();

    back.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        if (history_pos > 1) {
          --history_pos;
          goLocation(history.get(history_pos - 1));
        }
      }
    });
    forward.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        if (history_pos < history.size()) {
          ++history_pos;
          goLocation(history.get(history_pos - 1));
        }
      }
    });

    button_panel.add(back, BorderLayout.WEST);
    button_panel.add(forward, BorderLayout.EAST);

    top_bar.add(button_panel, BorderLayout.WEST);
    top_bar.add(location_bar, BorderLayout.CENTER);

    view_pane = new JEditorPane("text/html", "");
    view_pane.setEditable(false);
    view_pane.setCaretPosition(0);

    add(top_bar, BorderLayout.NORTH);
    add(new JScrollPane(view_pane), BorderLayout.CENTER);
    add(status_bar, BorderLayout.SOUTH);

    back.setEnabled(false);
    forward.setEnabled(false);

    // Hyperlink selected,
    view_pane.addHyperlinkListener(new HyperlinkListener() {
      public void hyperlinkUpdate(HyperlinkEvent e) {
        EventType evt_type = e.getEventType();
        if (evt_type.equals(EventType.ENTERED)) {
          status_bar.setText(e.getDescription());
        }
        else if(evt_type.equals(EventType.EXITED)) {
          status_bar.setText(" ");
        }
        else if (evt_type.equals(EventType.ACTIVATED)) {
          goLocation(e.getDescription());
        }
      }
    });

    // If the location bar is activated,
    location_bar.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        goLocation(location_bar.getText());
      }
    });

  }


  /**
   * Tells the browser go to the given location.
   */
  public void goLocation(String location) {

    status_bar.setText(" ");

    location_bar.setText(location);
    location_bar.setCaretPosition(0);

    // Get the last entry in the history,
    if (history.size() > 0) {
      String hloc = history.get(history_pos - 1);
      if (!location.equals(hloc)) {
        for (int i = history.size() - 1; i >= history_pos; --i) {
          history.remove(i);
        }
        history.add(location);
        history_pos = history.size();
      }
    }
    else {
      history.add(location);
      history_pos = history.size();
    }

    if (history_pos == history.size()) {
      forward.setEnabled(false);
    }
    else {
      forward.setEnabled(true);
    }
    if (history_pos == 1) {
      back.setEnabled(false);
    }
    else {
      back.setEnabled(true);
    }

    // Create a formatter object,
    ODBHTMLFormatter formatter = new ODBHTMLFormatter(transaction, "");

    // Decode the location,
    String decoded_location = location.replace("%23", "#");

    view_pane.setText(formatter.format(decoded_location));
    view_pane.setCaretPosition(0);

  }
  
  /**
   * Refreshes the browser with the new transaction.
   */
  public void refresh(ODBTransaction transaction) {
    
    this.transaction = transaction;
    goLocation("");

  }

}
