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

package com.mckoi.odb.util;

import com.mckoi.data.DataFile;

/**
 * A FileInfo object is used by FileRepository to encapsulate information about
 * a file object.
 *
 * @author Tobias Downer
 */

public interface FileInfo {

  /**
   * Returns the DataFile for accessing this file within the context of the
   * transaction where the file info came from. This DataFile is only valid
   * if the backed transaction has not been invalidated (by a dispose or
   * commit).
   */
  DataFile getDataFile();

  /**
   * Returns the full absolute name of this file with respect to the root
   * directory.
   */
  String getAbsoluteName();

  String getMimeType();

  long getLastModified();

  /**
   * Updates the 'LastModified' field of this file object to the given value.
   */
  void setLastModified(long last_modified);

  /**
   * Updates the 'MimeType' field of this file object to the given value.
   */
  void setMimeType(String mime_type);

  /**
   * Returns true if this file item represents a directory.
   */
  boolean isDirectory();

  /**
   * Returns true if this file item represents a file.
   */
  boolean isFile();

  /**
   * Returns the file name without the path.
   */
  String getItemName();

  /**
   * Returns the path name without the file name.
   */
  String getPathName();


//
//
//  /**
//   * The name of the file.
//   */
//  private final String name;
//
//  /**
//   * The mime type of the file.
//   */
//  private final String mime_type;
//
//  /**
//   * The last modified timestamp of the file.
//   */
//  private final long last_modified;
//
//  /**
//   * Constructor.
//   */
//  FileInfo(String name, String mime_type, long last_modified) {
//    this.name = name;
//    this.mime_type = mime_type;
//    this.last_modified = last_modified;
//  }
//
//  /**
//   * Returns the full absolute name of this file with respect to the root
//   * directory.
//   */
//  public String getAbsoluteName() {
//    return name;
//  }
//
//  public String getMimeType() {
//    return mime_type;
//  }
//
//  public long getLastModified() {
//    return last_modified;
//  }
//
//  /**
//   * Returns the DataFile for accessing this file within the context of the
//   * transaction where the file info came from. This DataFile is only valid
//   * if the backed transaction has not been invalidated (by a dispose or
//   * commit).
//   */
//  public abstract DataFile getDataFile();
//
//  /**
//   * Updates the 'LastModified' field of this file object to the given value.
//   */
//  public abstract void setLastModified(long last_modified);
//
//  /**
//   * Updates the 'MimeType' field of this file object to the given value.
//   */
//  public abstract void setMimeType(String mime_type);
//
//  /**
//   * Returns true if this file item represents a directory.
//   */
//  public boolean isDirectory() {
//    return mime_type != null && mime_type.equals("$dir");
//  }
//
//  /**
//   * Returns true if this file item represents a file.
//   */
//  public boolean isFile() {
//    return !isDirectory();
//  }
//
//  /**
//   * Returns the file name without the path.
//   */
//  public String getItemName() {
//    int p = name.lastIndexOf("/", name.length() - 2);
//    return name.substring(p + 1);
//  }
//
//  /**
//   * Returns the path name without the file name.
//   */
//  public String getPathName() {
//    int p = name.lastIndexOf("/");
//    return name.substring(0, p);
//  }
//
////  public static Comparator<FileInfo> getNameComparator() {
////    return new NameComparator();
////  }
////
////  /**
////   * Comparator for a sort order on the name field.
////   */
////  private static class NameComparator implements Comparator<FileInfo> {
////    public int compare(FileInfo o1, FileInfo o2) {
////      return o1.getAbsoluteName().compareTo(o2.getAbsoluteName());
////    }
////  }

}
