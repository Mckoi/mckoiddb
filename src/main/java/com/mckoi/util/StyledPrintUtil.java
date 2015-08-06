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
