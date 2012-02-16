/**
 * com.mckoi.gui.ODBHTMLFormatter  Feb 5, 2011
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

package com.mckoi.gui;

import com.mckoi.odb.ODBClass;
import com.mckoi.odb.ODBData;
import com.mckoi.odb.ODBList;
import com.mckoi.odb.ODBObject;
import com.mckoi.odb.ODBTransaction;
import com.mckoi.odb.Reference;

/**
 * Utility class that formats ODB elements in HTML format for use in a
 * browser.
 *
 * @author Tobias Downer
 */

public class ODBHTMLFormatter {


  /**
   * The maximum size of strings displayed inline in a table.
   */
  private static int STRING_MAX_SIZE = 24;


  /**
   * The backed transaction.
   */
  private final ODBTransaction transaction;

  /**
   * The string to prepend all hyperlinks.
   */
  private final String hyperlink_prepend;


  /**
   * Constructor.
   */
  public ODBHTMLFormatter(ODBTransaction transaction, String hyperlink_prepend) {
    this.transaction = transaction;
    this.hyperlink_prepend = hyperlink_prepend;
  }

  public ODBHTMLFormatter(ODBTransaction transaction) {
    this(transaction, "");
  }

  /**
   * Given a location, returns a string of HTML formatted markup that describes
   * the object. The location may be formatted as the following;
   * 'class:[class ref]', 'object:[class ref]:[object ref]',
   !* 'object:[class ref]:[object ref]![field name]'. The last location type
   * may represent all types of objects such as List and Data types. The last
   * type may truncate the output text if it is particularly large. Better
   * GUI functionality should be implemented for these types where possible.
   */
  public String format(String location) {

    int fd = location.indexOf(':');
    if (fd == -1) {
      return illegalLocation(location);
    }

    String c = location.substring(0, fd);
    String r = location.substring(fd + 1);

    if (c.equals("baseclass")) {
//      ODBClass odb_class = transaction.findClass(r);
//      if (odb_class == null) {
//        return illegalLocation(location);
//      }
//      return formatClass(odb_class.getReference().toString());
      return illegalLocation(location);
    }
    else if (c.equals("class")) {
      return formatClass(r);
    }
    else if (c.equals("instance")) {
      return formatInstance(r);
    }
//    else if (c.equals("object")) {
//      return formatObject(r);
//    }
    else {
      return illegalLocation(location);
    }

  }

  /**
   * Returns a HTML formatted description of a class reference. The class
   * reference is either a reference string (eg.
   * '0000012df4e7819859f9e4a89f6d3798') or a class name followed by a
   * reference string (eg. 'Machine#0000012df4e7819859f9e4a89f6d3798').
   */
  private String formatClass(String class_string) {

    // The class from the class string,
    ODBClass odb_class = getClassFromString(class_string);

    StringBuilder b = new StringBuilder();
    b.append("<h2>Class ");
    b.append(odb_class.getName());
    b.append("</h2>");
    b.append("<pre>");

    for (int i = 0; i < odb_class.getFieldCount(); ++i) {

      // Get the field type,
      String field_type = odb_class.getFieldType(i);
      String field_str;
      String extra_str = "";

      boolean mutable = odb_class.isFieldMutable(i);

      // Parse it
      if (field_type.equals("[S")) {
        field_str = "String";
        if (mutable) {
          field_str = field_str + "*";
        }
      }
      else if (field_type.startsWith("[L")) {
        int br = field_type.indexOf(">(", 3);
        String element_type = field_type.substring(3, br);

        field_str = "List&lt;" + classHref(element_type) + "&gt;";

        String list_args_str =
                 field_type.substring(br + 2, field_type.indexOf(')', br + 2));
        String[] list_args = list_args_str.split(",");

        boolean allow_duplicates = !list_args[0].equals("unique");
        String collator_arg = list_args.length > 1 ? list_args[1] : null;
        String collator_function = list_args.length > 2 ? list_args[2] : null;

//        extra_str = "  ";
//        extra_str = extra_str + "Element Class: " + classHref(element_type);
        extra_str = extra_str + "  Constraint: " +
                      (allow_duplicates ? "Allow Duplicate Values" :
                                          "Unique Values Only");
        if (collator_function != null) {
          extra_str = extra_str + "\n  Order Function: " +
                                 collator_function + "(" + collator_arg + ")";
        }
        else {
          extra_str = extra_str + "\n  Order Function: By Reference";
        }
        extra_str = extra_str + "\n";

      }
      else if (field_type.startsWith("[D")) {
        field_str = "Data";
        extra_str = "  " + field_type + "\n";
      }
      else {
        // Otherwise it must be a member reference,
        field_str = classHref(field_type);
        if (mutable) {
          field_str = field_str + "*";
        }
      }



      b.append(field_str);
      b.append(" ");
      b.append(HTMLUtils.htmlify(odb_class.getFieldName(i)));
      b.append("\n");
      b.append(extra_str);



    }


    b.append("</pre>");

    return b.toString();
  }

  /**
   * Returns a HTML formatted description of an object instance. The object
   * location must be formatted as '[class ref]:[object ref]' (eg.
   * 'Machine#0000012df4e7819859f9e4a89f6d3798:0000012df4e781980aa04c0e2bbc6983').
   */
  private String formatInstance(String object_string) {

    // Parse out the query string if present,
    String query_string = null;
    int query_delim = object_string.lastIndexOf('?');
    if (query_delim != -1) {
      query_string = object_string.substring(query_delim + 1);
      object_string = object_string.substring(0, query_delim);
    }

    // Process the location,
    int fd = object_string.indexOf(':');
    if (fd == -1) {
      return illegalLocation(object_string);
    }
    String class_string = object_string.substring(0, fd);
    String obj_ref_string = object_string.substring(fd + 1);

    // If there's a field value in the instance,
    String field_val = null;
    int orfd = obj_ref_string.indexOf("!");
    if (orfd >= 0) {
      field_val = obj_ref_string.substring(orfd + 1);
      obj_ref_string = obj_ref_string.substring(0, orfd);
    }

    Reference object_ref = Reference.fromString(obj_ref_string);

    StringBuilder b = new StringBuilder();
    ODBClass odb_class = getClassFromString(class_string);
    ODBObject odb_object = transaction.getObject(odb_class, object_ref);

    final String instance_string =
                             "instance:" + class_string + ":" + obj_ref_string;

    if (field_val == null) {
      // The base object,
      // The class from the class string,
      b.append("[");
      b.append(classHref(class_string));
      b.append("] ");
      b.append(object_ref.toString());
      b.append("<br/><br/>");

      // Make a table model with a single entry containing the instance,
      ODBListModel list_model = new ODBInstanceListModel(odb_object);

      b.append(listAsHtmlTable(list_model, instance_string, null));
    }
    else {
      int field_index = odb_class.indexOfField(field_val);

      if (field_index == -1) {
        return illegalLocation(object_string);
      }

      // Write the object's field content,

      b.append("[");
      b.append(classHref(class_string));
      b.append("] ");
      b.append(objectHrefSimple(odb_object));
      b.append(" ");
      b.append(field_val);
      b.append("<br/><br/>");

      String field_type = odb_class.getFieldType(field_index);
      // A String value,
      if (field_type.equals("[S")) {
        b.append("<pre>");
        b.append(HTMLUtils.htmlify(odb_object.getString(field_index)));
        b.append("</pre>");
      }
      // A data field type,
      else if (field_type.startsWith("[D")) {

        ODBData data = odb_object.getData(field_index);

        b.append("&lt;size = ");
        b.append(data.size());
        b.append("&gt;");
        b.append("<br/><br/>");

        b.append(dataAsHtmlPageable(data,
                 instance_string + "!" + field_val, query_string));

      }
      // A list,
      else if (field_type.startsWith("[L")) {
        final ODBList list = odb_object.getList(field_index);

        ODBListModel list_model = new ODBListModel() {
          public ODBObject getElement(int n) {
            return list.getObject(n);
          }
          public ODBClass getElementClass() {
            return list.getElementClass();
          }
          public Reference getElementReference(int n) {
            return list.get(n);
          }
          public int size() {
            long sz = list.size();
            return sz > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) sz;
          }
        };

        b.append("List Element: ");
        b.append(classHref(list.getElementClass()));
        b.append("<br/><br/>");

        b.append(listAsHtmlTable(list_model,
                            instance_string + "!" + field_val, query_string));
      }
      // A member field,
      else {
        b.append(objectHref(odb_object.getObject(field_index)));
      }

    }

    return b.toString();
  }


  private String classHref(ODBClass member) {
    String out;
    String class_str = member.getInstanceName();
    out = "<a href=\"" + hyperlink_prepend
            + "class:" + class_str + "\">"
            + member.getName() + "</a>";
    return out;
  }


  private String classHref(String class_str) {
    return classHref(getClassFromString(class_str));
  }


  private String objectHref(ODBObject ob) {

    if (ob == null) {
      return "null";
    }

    String out;
    String class_str = ob.getODBClass().getInstanceName();
    String ob_ref_str = ob.getReference().toString();
    ODBClass member = getClassFromString(class_str);
    out = "<a href=\"" + hyperlink_prepend +
          "instance:" + class_str + ":" +
          ob_ref_str + "\">[" +
          member.getName() + "] " + ob_ref_str + "</a>";
    return out;
  }


  private String objectHrefSimple(ODBObject ob) {
    String out;
    String class_str = ob.getODBClass().getInstanceName();
    String ob_ref_str = ob.getReference().toString();
//    ODBClass member = getClassFromString(class_str);
    out = "<a href=\"" + hyperlink_prepend +
          "instance:" + class_str + ":" +
          ob_ref_str + "\">" + ob_ref_str + "</a>";
    return out;
  }


  /**
   * Renders the content of a data object with a simple pagination function.
   * The query_string is used for state.
   */
  private String dataAsHtmlPageable(ODBData data_ob,
                          final String instance_string, String query_string) {

    final int LINE_BYTESIZE = 32;

    // The position in the index and the page size,
    int pos = 0;
    int page_size = 100;

    // Parse state from the query string,
    if (query_string != null) {
      String[] args = query_string.split("&");
      for (int i = 0; i < args.length; ++i) {
        int d = args[i].indexOf("=");
        if (d != -1) {
          String key = args[i].substring(0, d);
          String val = args[i].substring(d + 1);
          if (key.equals("pos")) {
            pos = Integer.parseInt(val);
          }
          else if (key.equals("size")) {
            page_size = Integer.parseInt(val);
          }
        }
      }
    }

    HTMLUtils.PageLinkRenderer renderer = new HTMLUtils.PageLinkRenderer() {
      public void render(StringBuilder out, long page_number, long page_size,
                         boolean is_current) {
        if (!is_current) {
          out.append("<a href=\"");
          out.append(instance_string);
          out.append("?pos=");
          out.append(page_number * page_size);
          out.append("&size=");
          out.append(page_size);
          out.append("\">");
          out.append(page_number + 1);
          out.append("</a>");
        }
        else {
          out.append(page_number + 1);
        }
      }
    };

    long dsize = data_ob.size();

    // The number of lines,
    long line_count = ((dsize - 1) / LINE_BYTESIZE) + 1;

    StringBuilder b = new StringBuilder();

    StringBuilder pagination_control = new StringBuilder();
    if (line_count > page_size) {
      HTMLUtils.createPaginationControl(pagination_control, pos, page_size,
                                        line_count, renderer);
      b.append(pagination_control);
      b.append("<br/><br/>");
    }

    long dpos = pos * LINE_BYTESIZE;
    long dcap = dpos + (LINE_BYTESIZE * page_size);

    b.append("<table cellspacing=\"12\"><tr>");
    // The line index
    b.append("<td style=\"white-space: nowrap\"><pre>");
    for (long i = dpos; i < dsize && i < dcap; i += LINE_BYTESIZE) {
      b.append(i);
      b.append('\n');
    }
    b.append("</pre></td>");
    // The ascii
    b.append("<td style=\"white-space: nowrap\"><pre>");
    data_ob.position(dpos);
    for (long i = dpos; i < dsize && i < dcap; i += LINE_BYTESIZE) {
      for (long n = i; n < i + LINE_BYTESIZE && n < dsize; ++n) {
        // Turn it into a char,
        byte byt = data_ob.get();
        int codepoint = ((int) (byt)) & 0x0FF;
        char c = (char) codepoint;
        if (codepoint >= 32 && codepoint != 127 && codepoint != 255) {
          if (c == '<') {
            b.append("&lt;");
          }
          else if (c == '&') {
            b.append("&amp;");
          }
          else if (c == '>') {
            b.append("&gt;");
          }
          else {
            b.append(c);
          }
        }
        else {
          b.append('.');
        }
      }
      b.append('\n');
    }
    b.append("</pre></td>");
    // The hexidecimal
    b.append("<td style=\"white-space: nowrap\"><pre>");
    data_ob.position(dpos);
    for (long i = dpos; i < dsize && i < dcap; i += LINE_BYTESIZE) {
      int counter = 0;
      for (long n = i; n < i + LINE_BYTESIZE && n < dsize; ++n) {
        byte byt = data_ob.get();
        String hex = Integer.toHexString(((int) (byt)) & 0x0FF);
        if (hex.length() == 1) {
          b.append('0');
        }
        b.append(hex);
        ++counter;
        if (counter == 4) {
          counter = 0;
          b.append(' ');
        }
      }
      b.append('\n');
    }
    b.append("</pre></td>");

    b.append("</tr></table>");

    b.append("<br/>");
    b.append(pagination_control);

    return b.toString();

  }



  /**
   * Renders a table with a simple pagination function. The query_string is
   * used for state.
   */
  private String listAsHtmlTable(ODBListModel list_model,
                          final String instance_string, String query_string) {

    // The position in the index and the page size,
    int pos = 0;
    int page_size = 100;

    // Parse state from the query string,
    if (query_string != null) {
      String[] args = query_string.split("&");
      for (int i = 0; i < args.length; ++i) {
        int d = args[i].indexOf("=");
        if (d != -1) {
          String key = args[i].substring(0, d);
          String val = args[i].substring(d + 1);
          if (key.equals("pos")) {
            pos = Integer.parseInt(val);
          }
          else if (key.equals("size")) {
            page_size = Integer.parseInt(val);
          }
        }
      }
    }

    ODBClass elem_class = list_model.getElementClass();

    StringBuilder b = new StringBuilder();

    int field_count = elem_class.getFieldCount();

    HTMLUtils.PageLinkRenderer renderer = new HTMLUtils.PageLinkRenderer() {
      public void render(StringBuilder out, long page_number, long page_size,
                         boolean is_current) {
        if (!is_current) {
          out.append("<a href=\"");
          out.append(instance_string);
          out.append("?pos=");
          out.append(page_number * page_size);
          out.append("&size=");
          out.append(page_size);
          out.append("\">");
          out.append(page_number + 1);
          out.append("</a>");
        }
        else {
          out.append(page_number + 1);
        }
      }
    };

    final int list_size = list_model.size();
    
    StringBuilder pagination_control = new StringBuilder();
    if (list_size > page_size) {
      HTMLUtils.createPaginationControl(pagination_control, pos, page_size,
                                        list_size, renderer);
      b.append(pagination_control);
      b.append("<br/><br/>");
    }

    b.append("<table cellspacing=\"0\" cellpadding=\"4\"><tr>");
    b.append("<th>#</th>");
    // The header,
    for (int i = 0; i < field_count; ++i) {
      b.append("<th border=\"1\" style=\"white-space: nowrap\">");
      b.append(HTMLUtils.htmlify(elem_class.getFieldName(i)));
      b.append("</th>");
    }
    b.append("</tr>");

    for (int n = pos; n < pos + page_size && n < list_size; ++n) {
      b.append("<tr>");

      ODBObject ob = list_model.getElement(n);

      b.append("<td align=\"right\" style=\"white-space: nowrap\">");
      if (list_size != 1) {
        b.append(n);
      }
      b.append("</td>");

      for (int i = 0; i < field_count; ++i) {
        b.append("<td border=\"1\" style=\"white-space: nowrap\">");
        String field_type = elem_class.getFieldType(i);
        if (field_type.equals("[S")) {
          String str_val = ob.getString(i);
          if (str_val == null) {
            b.append("null");
          }
          else if(str_val.length() > STRING_MAX_SIZE) {
            b.append(HTMLUtils.htmlify(str_val.substring(0, STRING_MAX_SIZE)));
            String href_text = "...";
            String out = "<a href=\"" + hyperlink_prepend +
                         "instance:" + ob.getODBClass().getInstanceName() +
                         ":" + ob.getReference().toString() + "!" +
                         elem_class.getFieldName(i) +
                         "\">" + href_text + "</a>";
            b.append(out);
          }
          else {
            b.append(str_val);
          }
        }
        else if (field_type.startsWith("[L")) {
          ODBList list = ob.getList(i);
          ODBClass sub_elem_class = list.getElementClass();
          b.append("List&lt;");
          b.append(classHref(sub_elem_class));
          b.append("&gt; ");
          long slist_size = list.size();
          String pl = slist_size == 1 ? " element" : " elements";
          String href_text = HTMLUtils.htmlify("(" + slist_size + pl + ")");
          String out = "<a href=\"" + hyperlink_prepend +
                       "instance:" + ob.getODBClass().getInstanceName() +
                       ":" + ob.getReference().toString() + "!" +
                       elem_class.getFieldName(i) +
                       "\">" + href_text + "</a>";

          b.append(out);
        }
        else if (field_type.startsWith("[D")) {
          ODBData data = ob.getData(i);
          String href_text =
                     HTMLUtils.htmlify("Data <size = " + data.size() + ">");
          String out = "<a href=\"" + hyperlink_prepend +
                       "instance:" + ob.getODBClass().getInstanceName() +
                       ":" + ob.getReference().toString() + "!" +
                       elem_class.getFieldName(i) +
                       "\">" + href_text + "</a>";

          b.append(out);
        }
        else {
          b.append(objectHref(ob.getObject(i)));
        }
        b.append("</td>");
      }

      b.append("</tr>");
    }




    b.append("</table>");

    b.append("<br/>");
    b.append(pagination_control);


    return b.toString();
  }




  private ODBClass getClassFromString(String str) {
    String class_ref_string;
    int d = str.indexOf('#');
    if (d == -1) {
      class_ref_string = str;
    }
    else {
      class_ref_string = str.substring(d + 1);
    }
    Reference class_ref = Reference.fromString(class_ref_string);

    return transaction.getClass(class_ref);
  }





  static String illegalLocation(String location) {

    StringBuilder b = new StringBuilder();
    b.append("<h3>Invalid Location: ");
    b.append(HTMLUtils.htmlify(location));
    b.append("</h3>");

    return b.toString();
  }

}
