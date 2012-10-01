/**
 * com.mckoi.util.StyledPrintUtil  Sep 30, 2012
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

import java.io.IOException;
import java.io.Writer;
import java.nio.CharBuffer;

/**
 * Utility methods around StyledPrintWriter classes.
 *
 * @author Tobias Downer
 */

public class StyledPrintUtil {

  /**
   * Returns a Writer that writes to a StyledPrintWriter.
   */
  public static Writer wrapWriter(StyledPrintWriter w) {
    return new WrappedWriter(w);
  }

  private static class WrappedWriter extends Writer {

    private final StyledPrintWriter w;

    WrappedWriter(StyledPrintWriter w) {
      this.w = w;
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
      CharBuffer charbuf = CharBuffer.wrap(cbuf, off, len);
      w.print(charbuf);
    }

    @Override
    public Writer append(CharSequence csq) throws IOException {
      w.print(csq);
      return this;
    }

    @Override
    public Writer append(CharSequence csq, int start, int end) throws IOException {
      if (csq == null) {
        csq = "null";
      }
      w.print(csq.subSequence(start, end));
      return this;
    }

    @Override
    public Writer append(char c) throws IOException {
      w.print(c);
      return this;
    }

    @Override
    public void write(int c) throws IOException {
      w.print((char) c);
    }

    @Override
    public void write(String str) throws IOException {
      w.print(str);
    }

    @Override
    public void write(String str, int off, int len) throws IOException {
      if (str == null) {
        str = "null";
      }
      w.print(str.substring(off, off + len));
    }

    @Override
    public void flush() throws IOException {
      w.flush();
    }

    @Override
    public void close() throws IOException {
    }

  }

}
