/**
 * com.mckoi.gui.HexViewer  Jul 12, 2009
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

package com.mckoi.gui;

import com.mckoi.data.DataFile;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import javax.swing.*;

/**
 * A simple hex viewer component that formats an arbitrary sized data object
 * into a human understand hex format. The data is displayed in a 16 byte
 * grid together with an ASCII representation.
 * <p>
 * This component does not attempt to perform any buffering of the data.
 *
 * @author Tobias Downer
 */

public class HexViewer extends JComponent implements Scrollable {

  private DataFile data_file;

  private int char_height;
  private int char_width;
  private int panel_height;
  private long line_count;

  /**
   * Constructor.
   */
  public HexViewer() {
    setFont(new Font("monospaced", Font.PLAIN, 12));
    setBackground(Color.white);
    setOpaque(false);
    setFocusable(true);
    addMouseListener(new MouseAdapter() {
      public void mousePressed(MouseEvent e) {
        // We gain focus when clicked on
        requestFocusInWindow();
      }
    });

    setRequestFocusEnabled(true);
  }



  DataFile getDataFile() {
    return data_file;
  }

  private void viewerValidate() {
    Font f = getFont();
    FontMetrics fm = getFontMetrics(f);
    char_width = fm.charWidth('M');
    char_height = fm.getHeight();

    if (data_file == null) {
      line_count = 0;
    }
    else {
      long byte_count = data_file.size();
      int bytes_per_line = 16;
      line_count = (byte_count + (bytes_per_line - 1)) / bytes_per_line;
    }

    panel_height = 0;
    long height_in_bytes = line_count * char_height;
    if (height_in_bytes > Integer.MAX_VALUE) {
      panel_height = Integer.MAX_VALUE;
    }
    else {
      panel_height = (int) height_in_bytes;
    }

    revalidate();
    repaint();
  }

  public void setDataFile(DataFile f) {
    data_file = f;
    viewerValidate();
  }

  public void setFont(Font f) {
    super.setFont(f);
    viewerValidate();
  }

  /**
   * Returns the number of lines needed to display this object.
   */
  public long getLineCount() {
    return line_count;
  }

  @Override
  public void paintComponent(Graphics g) {

    if (g instanceof Graphics2D) {
      Graphics2D g2d = (Graphics2D) g;

      // for antialiasing text
      g2d.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING,
                           RenderingHints.VALUE_TEXT_ANTIALIAS_ON);

    }

    super.paintComponent(g);

    Rectangle r = g.getClipBounds();

    // Discover the part of the DataFile we need to draw,
    int point_start = (r.y / char_height) * 16;
    int point_end = (((r.y + r.height) / char_height) + 1) * 16;

    // Paint the background,
    g.setColor(Color.WHITE);
    g.fillRect(r.x, r.y, r.width, r.height);;
    // Set the font,
    Font f = getFont();
    g.setFont(f);

    FontMetrics fm = g.getFontMetrics(f);
    int baseline = fm.getAscent();

    int pys = (point_start / 16);
    int pye = (point_end / 16);

    int row_header_size = (char_width * 16) + 7;
    int hex_ascii_split = 7 + row_header_size + (16 * ((char_width * 2) + 2));

    long fpos = (pys * 16);
    long fmax = data_file.size();
    data_file.position(fpos);

    StringBuilder ascii_builder = new StringBuilder(16);

    for (int py = pys; py < pye; ++py) {

      int fonty = baseline + (py * char_height);

      // Draw the row header,
      StringBuilder sb = new StringBuilder();
      String hex_position = Long.toHexString((long) py * 16);
      for (int i = 0; i < (16 - hex_position.length()); ++i) {
        sb.append("0");
      }
      sb.append(hex_position);
      g.setColor(Color.GRAY);
      g.drawString(sb.toString(), 0, fonty);

      ascii_builder.setLength(0);

      // Draw the hex values and make up the ascii values,
      g.setColor(Color.BLACK);
      for (int px = 0; px < 16; ++px) {
        if (fpos < fmax) {
          byte b = data_file.get();
          ++fpos;

          char c = (char) b;
          if (Character.isDefined(c) && !Character.isISOControl(c) &&
              f.canDisplay(c) && fm.charWidth(c) == char_width) {
            ascii_builder.append(c);
          }
          else {
            ascii_builder.append('.');
          }

          String hex_byte = Integer.toHexString(((int) b) & 0x0FF);
          if (hex_byte.length() == 1) {
            hex_byte = "0" + hex_byte;
          }
          g.drawString(hex_byte,
                       row_header_size + (px * ((char_width * 2) + 2)),
                       fonty);
        }
      }

      // Draw the ascii values
      g.drawString(ascii_builder.toString(),
                   hex_ascii_split, fonty);

      // Draw the boundary lines between the different sections
      g.setColor(Color.black);
      g.drawLine(row_header_size - 4, r.y, row_header_size - 4, r.y + r.height);
      g.drawLine(hex_ascii_split - 4, r.y, hex_ascii_split - 4, r.y + r.height);

    }

  }

  public Dimension getPreferredSize() {
    return new Dimension((56 * (char_width + 2)) + 4, panel_height);
  }

  public Dimension getPreferredScrollableViewportSize() {
    return getPreferredSize();
  }

  public int getScrollableUnitIncrement(Rectangle visibleRect,
                                        int orientation, int direction) {
    return char_height;
  }

  public int getScrollableBlockIncrement(Rectangle visibleRect,
                                         int orientation, int direction) {
    return visibleRect.getSize().height;
  }

  public boolean getScrollableTracksViewportHeight() {
    return false;
  }

  public boolean getScrollableTracksViewportWidth() {
    return false;
  }

}
