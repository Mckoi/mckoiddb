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
import java.io.PrintWriter;

/**
 * Outputs binary formats to a hex output.
 *
 * @author Tobias Downer
 */

public class HexFormatter {
  
  private int byte_width = 16;
  
  public void setByteLineWidth(int wid) {
    byte_width = wid;
  }
  
  /**
   * Formats the given byte array as a hex string.
   * 
   * @param buf
   * @param out 
   */
  public void format(byte[] buf, PrintWriter out) {
    
    StringBuilder line_hex = new StringBuilder();
    StringBuilder line_ascii = new StringBuilder();

    int i = 0;
    while (i < buf.length) {

      int line_end = i + byte_width;
      
      for (; i < line_end; ++i) {
        if (i < buf.length) {
          // Format the line,
          int b = ((int) buf[i]) & 0x0FF;
          String str = Integer.toHexString(b);
          if (str.length() == 1) {
            line_hex.append('0');
          }
          line_hex.append(str);
          char ch = (char) b;
          if (Character.isDefined(ch) && !Character.isISOControl(ch)) {
            line_ascii.append(ch);
          }
          else {
            line_ascii.append('.');
          }
        }
        else {
          // Enter spacing only,
          line_hex.append("  ");
          line_ascii.append(' ');
        }
        
        if ((i & 3) == 3) {
          line_hex.append(" ");
        }
        
      }
      
      out.print(line_hex.toString());
      out.print("   ");
      out.println(line_ascii.toString());
      
      line_hex.setLength(0);
      line_ascii.setLength(0);
    }
    
  }
  
  
}
