/**
 * com.mckoi.util.IOWrapStyledPrintWriter  Sep 30, 2012
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

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;

/**
 * An implementation of StyledPrintWriter that wraps an java.io.OutputStream
 * and strips all style information.
 *
 * @author Tobias Downer
 */

public class IOWrapStyledPrintWriter implements StyledPrintWriter {

  private final PrintWriter out;

  public IOWrapStyledPrintWriter(OutputStream cout, boolean auto_flush) {
    this.out = new PrintWriter(cout, auto_flush);
  }
  
  public IOWrapStyledPrintWriter(Writer cout, boolean auto_flush) {
    this.out = new PrintWriter(cout, auto_flush);
  }

  public IOWrapStyledPrintWriter(OutputStream cout) {
    this(cout, true);
  }
  
  public IOWrapStyledPrintWriter(Writer cout) {
    this(cout, true);
  }

  // -----

  @Override
  public void print(Object str) {
    out.print(str);
  }

  @Override
  public void println(Object str) {
    out.println(str);
  }

  @Override
  public void print(Object str, String style) {
    print(str);
  }

  @Override
  public void println(Object str, String style) {
    println(str);
  }

  @Override
  public void println() {
    out.println();
  }

  @Override
  public void printException(Throwable e) {
    if (e != null) {
      e.printStackTrace(out);
    }
  }

  @Override
  public void flush() {
    out.flush();
  }

}
