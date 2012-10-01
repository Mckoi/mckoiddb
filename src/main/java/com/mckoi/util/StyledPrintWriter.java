/**
 * ccom.mckoi.util.StyledPrintWriter  Sep 30, 2012
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

package com.mckoi.util;

/**
 * An interface for writing text with a markup style to a console display. The
 * display may choose to format a style any way it chooses - the style text is
 * simply a hint to aid in formatting/coloring the output.
 * <p>
 * Output from a styled print writer can be assumed to be written to a screen
 * of blocks of equal dimension, where each block can contain a single
 * character. The text is written from left to right. A new line will move
 * the cursor to the start of the next line. The are no commands to change the
 * position of the cursor.
 *
 * @author Tobias Downer
 */

public interface StyledPrintWriter {

  /**
   * Outputs a string to the display.
   */
  void print(Object str);

  /**
   * Outputs a string to the display then moves the cursor to the next line.
   */
  void println(Object str);

  /**
   * Outputs a string to the display with the given style.
   */
  void print(Object str, String style);

  /**
   * Outputs a string to the display with the given style then moves the cursor
   * to the next line.
   */
  void println(Object str, String style);

  /**
   * Moves the cursor to the next line.
   */
  void println();

  /**
   * Displays the given exception with the 'error' style.
   */
  void printException(Throwable e);

  /**
   * Flush the current output to the display.
   */
  void flush();

  
  public static final String ERROR = "error";
  public static final String INFO  = "info";
  public static final String DEBUG = "debug";
  
}
