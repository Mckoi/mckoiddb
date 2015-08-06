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

import java.io.IOException;
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

  @Override
  public Writer asWriter(String style) {
    return new ReclassWriter(this, style);
  }

  @Override
  public Writer asWriter() {
    return new ReclassWriter(this);
  }

  /**
   * A Writer that can be used in 'asWriter' to bridge between a java.io.Writer
   * and StyledPrintWriter with a single style.
   */
  public static class ReclassWriter extends Writer {

    private final StyledPrintWriter styled_writer;
    private final String style;

    public ReclassWriter(StyledPrintWriter styled_writer, String style) {
      this.styled_writer = styled_writer;
      this.style = style;
    }

    public ReclassWriter(StyledPrintWriter styled_writer) {
      this(styled_writer, null);
    }

    @Override
    public void write(String str) throws IOException {
      StringBuilder to_output = new StringBuilder();
      int i = 0;
      while (i < str.length()) {

        char ch = str.charAt(i);
        ++i;
        boolean is_newline = false;
        if (ch == '\r') {
          is_newline = true;
          // New line, consume \n if it follows,
          if (i < str.length()) {
            if (str.charAt(i) == '\n') {
              ++i;
            }
          }
        }
        else if (ch == '\n') {
          is_newline = true;
        }

        // If we haven't reached a newline yet,
        if (!is_newline) {
          to_output.append(ch);
        }
        // If we have reached a newline,
        else {
          // If there's nothing to output,
          if (to_output.length() == 0) {
            styled_writer.println();
          }
          // Otherwise output it with the newline,
          else {
            if (style != null) {
              styled_writer.println(to_output.toString(), style);
            }
            else {
              styled_writer.println(to_output.toString());
            }
            to_output.setLength(0);
          }
        }
      }

      // If there's something to output remaining,
      if (to_output.length() > 0) {
        if (style != null) {
          styled_writer.print(to_output.toString(), style);
        }
        else {
          styled_writer.print(to_output.toString());
        }
      }
    }

    @Override
    public void write(int c) throws IOException {
      write(String.valueOf((char)c));
    }

    @Override
    public void write(String str, int off, int len) throws IOException {
      write(str.substring(off, off + len));
    }

    @Override
    public void write(char[] cbuf) throws IOException {
      write(String.valueOf(cbuf));
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
      // Convert to string,
      write(String.valueOf(cbuf, off, len));
    }

    @Override
    public void flush() throws IOException {
      // Delegate,
      styled_writer.flush();
    }

    @Override
    public void close() throws IOException {
      // This is a no operation,
    }

  };

}
