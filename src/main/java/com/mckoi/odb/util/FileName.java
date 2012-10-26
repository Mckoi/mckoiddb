/**
 * com.mckoi.appcore.FileName  Oct 25, 2012
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

package com.mckoi.odb.util;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * A universal reference to an object (a file or directory) in a file system
 * (represented by a repository id). A file name will follow one of the
 * formats below;
 * <pre>
 *  /[repository id]/[path]/[file name]
 *  /[repository id]/[path]/
 *  /[repository id]/[file name]
 *  /[repository id]/
 * </pre>
 * <p>
 * A directory will always be represented by a name that ends with a '/'
 * (although, a name that doesn't end with a '/' may also be a directory). If
 * the name starts with a '/' it means the reference is ABSOLUTE, otherwise
 * the reference is RELATIVE. A relative name must be resolved before it can
 * be access.
 *
 * @author Tobias Downer
 */

public class FileName {

  /**
   * The file identifier string.
   */
  private final String file;

  /**
   * The previous directory.
   */
  public static final FileName PREVIOUS_DIR = new FileName("../");

  /**
   * The current directory.
   */
  public static final FileName CURRENT_DIR = new FileName("./");

  /**
   * Constructor.
   */
  public FileName(String file) {
    if (file == null) throw new NullPointerException();
    this.file = file;
  }

  /**
   * Constructor.
   */
  public FileName(String repository_id, String path_file) {

    if (path_file == null) throw new NullPointerException();
    if (repository_id == null) throw new NullPointerException();
    if (repository_id.length() < 1) throw new IllegalArgumentException();

    if (path_file.length() > 0 && !path_file.startsWith("/")) {
      throw new RuntimeException("'path_file' must be absolute");
    }

    StringBuilder b = new StringBuilder();
    b.append("/");
    b.append(repository_id);
    b.append(path_file);

    this.file = b.toString();
  }

  /**
   * Returns true if the file reference is relative (doesn't start with '/').
   */
  public boolean isRelative() {
    return !isAbsolute();
  }

  /**
   * Returns true if the file reference is absolute (starts with a '/').
   */
  public boolean isAbsolute() {
    return file.startsWith("/");
  }

  /**
   * Returns a URI string for this identifier in the MWPFS format.
   */
  public URI toURI() throws URISyntaxException {
    try {
      // Can't convert relative URI's
      if (isRelative()) {
        throw new URISyntaxException(file, "Relative reference");
      }
      return new URI("mwpfs", null, file, null);
    }
    catch (URISyntaxException ex) {
      // Shouldn't ever happen,
      throw new RuntimeException(ex);
    }
  }

  /**
   * Concatenates a file name together with a relative file name, assuming
   * this file is a path reference.
   */
  public FileName concat(FileName relative_fn) {
    if (relative_fn.isAbsolute()) {
      throw new RuntimeException("Can't concatenate with an absolute name");
    }
    if (file.length() == 0) {
      return relative_fn;
    }
    // Build the result file name,
    StringBuilder b = new StringBuilder();
    b.append(file);
//    System.out.println("--");
//    System.out.println(b);
    if (!file.endsWith("/")) {
      b.append("/");
    }
//    System.out.println(b);
    b.append(relative_fn.file);
//    System.out.println(b);
    // Return the concatenated version,
    return new FileName(b.toString());
  }

  /**
   * Normalizes a file name by removing the './' and '../' path references.
   * A '../' removes the previous path or repository id spec. The '../' path
   * reference will not remove more path items than are in the identifier when
   * this is an absolute file name. If it's a relative file name the result
   * will include the '../' references at the start.
   * Some examples;
   * <pre>
   *   "/admin/bin/lib/sys/../../lib2" = "/admin/bin/lib2"
   *   "admin/./../toby/sock/"         = "toby/sock/"
   *   "/admin/../../../../toby/sock"  = "/toby/sock"
   *   "admin/../../../../toby/sock/"  = "../../../toby/sock/"
   * </pre>
   */
  public FileName normalize() {

    String p = file;

    // The output path,
    StringBuilder out = new StringBuilder();

    boolean is_absolute = false;
    boolean is_known_dir = false;
    if (p.startsWith("/")) {
      is_absolute = true;
      out.append("/");
      p = p.substring(1);
    }
    if (p.endsWith("/")) {
      is_known_dir = true;
      p = p.substring(0, p.length() - 1);
    }
    // These are known directory specs
    if (p.endsWith("/.") || p.endsWith("/..")) {
      is_known_dir = true;
    }

    String[] items = p.split("\\/");
    int sz = items.length;
    boolean[] skipped = new boolean[sz];

    for (int i = 0; i < sz; ++i) {
      String item = items[i];
      if (item.equals(".")) {
        skipped[i] = true;
      }
      else if (item.equals("..")) {
        skipped[i] = true;
        // Search back to first unpicked,
        int n = i - 1;
        boolean found_to_skip = false;
        while (n >= 0) {
          if (!skipped[n]) {
            skipped[n] = true;
            found_to_skip = true;
            break;
          }
          --n;
        }
        if (!found_to_skip && !is_absolute) {
          out.append("../");
        }
      }
    }
    // Append the non-skipped items,
    for (int i = 0; i < sz; ++i) {
      if (!skipped[i]) {
        out.append(items[i]);
        out.append("/");
      }
    }

    // If absolute or known dir but the buffer is empty, make sure to append
    // a directory spec.
    if (out.length() == 0 && (is_absolute || is_known_dir)) {
      return new FileName("/");
    }

    // Final form,
    String fn = out.toString();
    if (!is_known_dir && fn.length() > 1 && fn.endsWith("/")) {
      return new FileName(out.substring(0, out.length() - 1));
    }
    return new FileName(fn);
    
  }

  /**
   * Resolves a relative file name against this file name. Examples;
   * <pre>
   *   "/admin/bin/lib/sys/".resolve("../../lib2") = "/admin/bin/lib2"
   *   "/admin/bin/lib/sys".resolve("./../mwp/")   = "/admin/bin/lib/mwp"
   *   "bin/lib/".resolve("../../../tak/")         = "../tak/"
   * </pre>
   */
  public FileName resolve(FileName relative_fn) {

    // If the given file name is absolute then return that,
    if (relative_fn.isAbsolute()) {
      return relative_fn;
    }

    return this.concat(relative_fn).normalize();

  }

  /**
   * Returns the repository id of this file name, or null if there's no
   * repository id in the spec. Throws exception if the file name is not
   * absolute.
   */
  public String getRepositoryId() {
    if (isRelative()) {
      throw new RuntimeException("Relative reference");
    }
    if (file.length() == 0 || file.equals("/")) {
      return null;
    }

    int delim = file.indexOf('/', 1);
    if (delim == -1) {
      delim = file.length();
    }
    return file.substring(1, delim);

  }

  /**
   * Returns the path name/file name part of the file name, or null if there's
   * path/file in the spec. Throws exception if the file name is relative.
   * <p>
   * This will always return an absolute path/file string (the string will
   * always begin with '/').
   */
  public String getPathFile() {
    if (isRelative()) {
      throw new RuntimeException("Relative reference");
    }
    if (file.length() == 0 || file.equals("/")) {
      return null;
    }

    int delim = file.indexOf('/', 1);
    if (delim == -1) {
      return "/";
    }
    return file.substring(delim);

  }

  /**
   * Returns the file part of the file name, or null if there's no file part
   * in the spec. Throws exception if the file name is relative.
   * <p>
   * For example;
   * <pre>
   *   "/admin"               = ""
   *   "/admin/"              = ""
   *   "/admin/toby.txt"      = "toby.txt"
   *   "/admin/data/toby.txt" = "toby.txt"
   *   "/"                    = null
   * </pre>
   */
  public FileName getFile() {
    String path_file = getPathFile();
    if (path_file == null) {
      return null;
    }
    int delim = path_file.lastIndexOf('/');
    return new FileName(path_file.substring(delim + 1));
  }

  /**
   * Returns the repository id and path part of the file name, or null if
   * there's no path part in the spec. Throws exception if the file name is
   * relative.
   */
  public FileName getPath() {
    int delim = file.lastIndexOf('/');
    if (delim == -1) {
      return null;
    }
    return new FileName(file.substring(0, delim + 1));
  }

  /**
   * Returns this FileName as a directory reference. This simply checks if the
   * last character of the file name is '/'. If it is it returns this object,
   * otherwise returns an object with '/' appended.
   */
  public FileName asDirectory() {
    if (file.endsWith("/")) {
      return this;
    }
    return new FileName(file + '/');
  }

  /**
   * Returns this FileName as a file reference. This simple checks if the last
   * character of the file name is '/' and removes it in the returned object.
   * This will not remove the first '/' in the string, therefore if this is
   * an absolute reference then it will stay so.
   */
  public FileName asFile() {
    if (!file.equals("/") && file.endsWith("/")) {
      return new FileName(file.substring(0, file.length() - 1));
    }
    return this;
  }

  /**
   * Returns true if this is a known directory (eg. the file name ends with
   * a '/').
   */
  public boolean isDirectory() {
    String path_file = getPathFile();
    return (path_file != null && 
             ( path_file.endsWith("/") ||
               path_file.endsWith("/.") || 
               path_file.endsWith("/..") ));
  }

  /**
   * Returns true if the file passes validity checks (no double '//' strings).
   */
  public boolean isValid() {
    return (!file.contains("//") && !file.contains("\\"));
  }




  @Override
  public String toString() {
    return file;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final FileName other = (FileName) obj;
    return file.equals(other.file);
  }

  @Override
  public int hashCode() {
    return file.hashCode();
  }


  
  
  
  // -----
  
  public static void main(String[] args) {
    
    FileName fn1 = new FileName("/admin/data/toby/");
    FileName fn2 = new FileName("admin/data/toby");

    FileName fn3 = new FileName("../../bin/./lib/");
    FileName fn4 = new FileName(".././../bin/lib");
    FileName fn5 = new FileName("../.././../bin/lib/");
    FileName fn6 = new FileName("../../.././../bin/lib");
    FileName fn7 = new FileName("../../../.././../bin/lib/");
    FileName fn8 = new FileName("../../../.././../bin");
    FileName fn9 = new FileName("../../../.././../");
    FileName fna = new FileName("../../../.././..");

    FileName fnb = new FileName("../../../");
    FileName fnc = new FileName("../../..");
    FileName fnd = new FileName("../../../a");
    

    System.out.println(fn1);
    System.out.println(fn2);
    System.out.println("--");
    System.out.println(fn1.resolve(fn3));
    System.out.println(fn1.resolve(fn4));
    System.out.println(fn1.resolve(fn5));
    System.out.println(fn1.resolve(fn6));
    System.out.println(fn1.resolve(fn7));
    System.out.println(fn1.resolve(fn8));
    System.out.println(fn1.resolve(fn9));
    System.out.println(fn1.resolve(fna));
    System.out.println(fn1.resolve(fnb));
    System.out.println(fn1.resolve(fnc));
    System.out.println(fn1.resolve(fnd));
    System.out.println("--");
    System.out.println(fn2.resolve(fn3));
    System.out.println(fn2.resolve(fn4));
    System.out.println(fn2.resolve(fn5));
    System.out.println(fn2.resolve(fn6));
    System.out.println(fn2.resolve(fn7));
    System.out.println(fn2.resolve(fn8));
    System.out.println(fn2.resolve(fn9));
    System.out.println(fn2.resolve(fna));
    System.out.println(fn2.resolve(fnb));
    System.out.println(fn2.resolve(fnc));
    System.out.println(fn2.resolve(fnd));
    System.out.println("--");
    
  }

}
