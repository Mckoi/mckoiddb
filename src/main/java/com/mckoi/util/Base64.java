/**
 * com.mckoi.util.Base64  Dec 20, 2008
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
