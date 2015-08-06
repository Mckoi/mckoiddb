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

package com.mckoi.gui;

/**
 * Various HTML utilities.
 *
 * @author Tobias Downer
 */

public class HTMLUtils {

  /**
   * Returns a simple pagination control string for navigating through pages
   * of a list of items.
   */
  public static void createPaginationControl(StringBuilder out,
                        long pos, int page_size, long total_size,
                        PageLinkRenderer renderer) {

//    System.out.println("pos = " + pos);
//    System.out.println("page_size = " + page_size);
//    System.out.println("total_size = " + total_size);

    long page_count = ((total_size - 1) / page_size) + 1;
    long cur_page = pos / page_size;
    long min_page = Math.max(cur_page - 7, 1);
    long max_page = Math.min(min_page + 14, page_count - 1);

    out.append("[");
    renderer.render(out, 0, page_size, cur_page == 0);
    for (long i = min_page; i < max_page; ++i) {
      out.append(" ");
      renderer.render(out, i, page_size,
                      i == cur_page);
    }
    if (page_count > 1) {
      out.append(" ");
      renderer.render(out, page_count - 1, page_size,
                      cur_page == page_count - 1);
    }
    out.append("] Page ");
    out.append(cur_page + 1);
    out.append(" of ");
    out.append(page_count);

  }

  /**
   * Turns a regular string into a form appropriate for a HTML document.
   */
  public static String htmlify(String str) {
    str = str.replace("&", "&amp;");
    str = str.replace("<", "&lt;");
    str = str.replace(">", "&gt;");
    return str;
  }

  public static interface PageLinkRenderer {

    /**
     * Renders a page link in a pagination control, where 'page_number' is
     * the number of page being rendered, 'page_size' is the size of pages,
     * and 'is_current' is true when the position is on the current page.
     */
    public void render(StringBuilder out,
                       long page_number, long page_size, boolean is_current);

  }

}
