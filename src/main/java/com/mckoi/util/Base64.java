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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;

/**
 * Various Base64 utilities.
 *
 * @author Tobias Downer
 */

public class Base64 {

  /**
   * The base64 encoded characters.
   */
  private static String ENC =
          "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  /**
   * Decodes a Base64 encoded string into a byte[] array.
   */
  public static byte[] decode(String str) {
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      StringReader in = new StringReader(str);

      // Fill the buffer,
      byte[] buf = new byte[4];
      while (true) {
        int end = 0;

        for (int i = 0; i < 4; ++i) {
          int c = in.read();
          byte v = 0;
          if (c == -1 || (char) c == '=') {
            // Ignore
          }
          else {
            int s = ENC.indexOf((char) c);
            if (s == -1) {
              throw new RuntimeException("Illegal char: " + (char) c);
            }
            v = (byte) s;
            ++end;
          }
          buf[i] = v;
        }

        // If nothing read, exit
        if (end == 0) {
          break;
        }

        // Turn the buffer into bytes,
        byte first = (byte) (buf[0] << 2 | buf[1] >> 4);
        byte second = (byte) (buf[1] << 4 | buf[2] >> 2);
        byte third = (byte) (buf[2] << 6 | buf[3]);

        bout.write(first);
        if (end == 3) {
          bout.write(second);
        }
        if (end == 4) {
          bout.write(second);
          bout.write(third);
        }

      }

      // Returns the byte array,
      return bout.toByteArray();

    }
    catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }




}
