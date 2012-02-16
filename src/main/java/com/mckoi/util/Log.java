/**
 * com.mckoi.util.Log  09 Jun 2000
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

package com.mckoi.util;

import java.io.*;
import java.text.*;
import java.util.Date;

/**
 * A log file/stream that logs some information generated by the system.
 * This is intended to help with debugging.  It safely handles concurrent
 * output to the log.
 *
 * @author Tobias Downer
 */

public class Log {

  /**
   * The output stream where log information is output to.
   */
  private final LogWriter log_output;

  /**
   * Date formatter.
   */
  private final DateFormat date_format = DateFormat.getDateTimeInstance();


  public Log(String path) throws FileNotFoundException, IOException {
    this(new File(path));
  }

  public Log(File file, int size, int max_count) throws IOException {
    this.log_output = new LogWriter(file, size, max_count);
  }

  public Log(File file) throws FileNotFoundException, IOException {
    // Defaults to a maximum of 12 512k log files
    this(file, 512 * 1024, 12);
//    this.log_output = new LogWriter(file, 512 * 1024, 12);
  }

  protected Log() {
    log_output = null;
  }

  /**
   * Writes an entry to the log file.  The log file records the time the entry
   * was put into the log, and the string which is the log.
   */
  public synchronized void log(String text) {
    try {
      log_output.write("[");
      log_output.write(date_format.format(new Date()));
      log_output.write("] ");
      log_output.write(text);
      log_output.flush();
    }
    catch (IOException e) {}
  }

  public synchronized void logln(String text) {
    try {
      log_output.write(text);
      log_output.write('\n');
      log_output.flush();
    }
    catch (IOException e) {}
  }

  /**
   * Closes the log file.
   */
  public synchronized void close() {
    try {
      log_output.close();
    }
    catch (IOException e) {}
  }

  // ---------- Static methods ----------

  /**
   * Returns a Log that won't actually store a log.  This is useful for
   * options where the user doesn't want anything logged.
   */
  public static Log nullLog() {
    return new NullLog();
  }

  // ---------- Inner classes ----------

}

/**
 * An implementation of Log that doesn't log anything.
 */
class NullLog extends Log {

  public NullLog() {
    super();
  }

  public void log(String text) {
    // Don't do anything,
  }
  public void logln(String text) {
    // Don't do anything,
  }
  public void close() {
    // Don't do anything,
  }

}

