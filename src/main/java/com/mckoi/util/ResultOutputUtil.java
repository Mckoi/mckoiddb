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

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for parsing a ResultSet and outputting it in different forms. The
 * forms included are straight text (mono-spaced), HTML, etc.
 *
 * @author Tobias Downer
 */

public class ResultOutputUtil {


  /**
   * Writes a break.
   *   eg. "+--------+----------+---------------+"
   */
  public static void writeBreak(int[] widths, PrintWriter out) {
    out.print('+');
    for (int i = 0; i < widths.length; ++i) {
      int wid = widths[i] + 2;
      for (int n = 0; n < wid; ++n) {
        out.print('-');
      }
      out.print('+');
    }
    out.println();
  }

  /**
   * Writes a row of data.
   *   eg. "|1         |Greetings        |Part-54445    |"
   */
  public static void writeRow(int[] widths, String[] cols, PrintWriter out) {
    out.print('|');
    for (int i = 0; i < widths.length; ++i) {
      String str = cols[i];
      out.print(' ');
      out.print(str);
      // Write padding
      int wid = (widths[i] + 1) - str.length();
      for (int n = 0; n < wid; ++n) {
        out.print(' ');
      }
      out.print('|');
    }
    out.println();
  }

  /**
   * Formats the ResultSet as plain mono-spaced text and outputs the result to
   * the PrintWriter.
   */
  public static void formatAsText(ResultSet result_set, PrintWriter out)
                                                         throws SQLException {
    ResultSetMetaData meta_data = result_set.getMetaData();
    // Maximum widths of each column.
    int[] max_widths = new int[meta_data.getColumnCount()];
    List<String>[] data = new List[meta_data.getColumnCount()];
    for (int i = 0; i < data.length; ++i) {
      data[i] = new ArrayList();
    }
    int row_count = 0;

    for (int i = 0; i < data.length; ++i) {
      String str = meta_data.getColumnLabel(i + 1);
      max_widths[i] = Math.max(str.length(), max_widths[i]);
    }

    // Read in the data for the result set,
    while (result_set.next()) {
      for (int i = 0; i < data.length; ++i) {
        Object ob = result_set.getObject(i + 1);
        String str = "NULL";
        if (ob != null) {
          str = ob.toString();
        }
        data[i].add(str);
        max_widths[i] = Math.max(str.length(), max_widths[i]);
      }
      ++row_count;
    }

    // Output the data we stored
    String[] line = new String[data.length];

    writeBreak(max_widths, out);
    for (int n = 0; n < line.length; ++n) {
      line[n] = meta_data.getColumnLabel(n + 1);
    }
    writeRow(max_widths, line, out);
    writeBreak(max_widths, out);
    for (int i = 0; i < row_count; ++i) {
      for (int n = 0; n < line.length; ++n) {
        line[n] = (String) data[n].get(i);
      }
      writeRow(max_widths, line, out);
    }
    writeBreak(max_widths, out);
    
    out.flush();

  }


}
