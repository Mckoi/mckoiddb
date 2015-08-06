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

package com.mckoi.util;

import java.io.Writer;

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
   * 
   * @param str
   */
  void print(Object str);

  /**
   * Outputs a string to the display then moves the cursor to the next line.
   * 
   * @param str
   */
  void println(Object str);

  /**
   * Outputs a string to the display with the given style.
   * 
   * @param str
   * @param style
   */
  void print(Object str, String style);

  /**
   * Outputs a string to the display with the given style then moves the cursor
   * to the next line.
   * 
   * @param str
   * @param style
   */
  void println(Object str, String style);

  /**
   * Moves the cursor to the next line.
   */
  void println();

  /**
   * Displays the given exception with the 'error' style.
   * 
   * @param e
   */
  void printException(Throwable e);

  /**
   * Flush the current output to the display.
   */
  void flush();

  /**
   * Returns a java.io.Writer that prints through to this source.
   * 
   * @param style
   * @return 
   */
  Writer asWriter(String style);

  /**
   * Returns a java.io.Writer that prints through to this source.
   * @return 
   */
  Writer asWriter();
  
  public static final String ERROR = "error";
  public static final String INFO  = "info";
  public static final String DEBUG = "debug";
  
}
