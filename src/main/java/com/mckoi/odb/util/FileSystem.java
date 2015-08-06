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
import com.mckoi.network.CommitFaultException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * This class maps a file system like interface over an ObjectDatabase path.
 * The file system supports features such as file meta information, directory
 * trees, etc.
 *
 * @author Tobias Downer
 */

public interface FileSystem {

  /**
   * Creates an empty path directory if one doesn't exist. Recurses through
   * the path specification until all the directories have been created.
   */
  void makeDirectory(String path_name);

  /**
   * Deletes an empty path directory. Generates an exception if the directory
   * has files so can not be deleted, otherwise returns true if the directory
   * was deleted, false otherwise.
   */
  boolean removeDirectory(String path_name);

  /**
   * Renames a directory item.
   */
  void renameDirectory(String path_name_current, String path_name_new);

  /**
   * Creates a file from the repository with the given name, mime type and
   * timestamp. The file is created empty. If the file exists, generates an
   * exception. Any sub-directories necessary to support the file
   * name are automatically created by this method.
   */
  void createFile(String file_name, String mime_type, long last_modified);

  /**
   * Deletes a file from the repository with the given name. Returns true if
   * a file with the given name was found and deleted, otherwise returns
   * false.
   */
  boolean deleteFile(String file_name);

  /**
   * Sets the timestamp of the file or directory object.
   */
  void touchFile(String file_name, long last_modified);

  /**
   * Renames a file object. This method will rename the file only - the
   * paths must be the same. For example, '/from/path/file.txt' can not be
   * renamed to '/to/path/file.txt'.
   */
  void renameFile(String file_name_current, String file_name_new);

  /**
   * Copies a file object from its current position to the given path. If the
   * parent directories of the path do not exist, creates the parent path
   * structure. Throws an exception if the file to copy doesn't exist, or the
   * destination exists. The copied file also contains a copy of the mime and
   * last modified time from the original.
   * <p>
   * Note that the destination file name may not be a path, it must be a
   * complete file name.
   */
  void copyFile(String file_name_source, String file_name_dest);

  /**
   * Sets the name, timestamp and mime type of the file or directory object.
   */
  void updateFileInfo(String file_name, String mime_type_new, long last_modified_new);

//  /**
//   * Uploads a local file into the database if it is different that the file
//   * stored. 'remote_file' is the name to call the file remotely. Note that the
//   * 'lastmodified' property is taken from the time the file was uploaded.
//   * <p>
//   * If the local file is the same, no upload takes place.
//   */
//  boolean synchronizeFile(InputStream file_in1,
//          long local_file_len, long local_modified_time, String mime_type,
//                                     String remote_file) throws IOException;

  /**
   * Returns a list of all the files and sub-directories at the given path.
   * This list may be lazily created (meaning the content is materialized as
   * the list is traversed). The returned list is NOT sorted.
   * <p>
   * If the file system is changed such that file items would be added or
   * removed from the scope of the list, accessing the list may return an
   * error or the function of the list object may change unexpectedly.
   * <p>
   * Returns null if the directory does not exist.
   */
  List<FileInfo> getDirectoryFileInfoList(String dir);

  /**
   * Returns the list of all the files at the given path. This list may be
   * lazily created (meaning the content is materialized as the list is
   * traversed). The returned list is sorted lexicographically by the name of
   * the file.
   * <p>
   * If the file system is changed such that file items would be added or
   * removed from the scope of the list, accessing the list may return an
   * error or the function of the list object may change unexpectedly.
   * <p>
   * Returns null if the directory does not exist.
   */
  List<FileInfo> getFileList(String dir);

  /**
   * Returns the list of all the sub-directories at the given path. This list
   * may be lazily created (meaning the content is materialized as the list is
   * traversed). The returned list is sorted lexicographically by the name of
   * the directory.
   * <p>
   * If the file system is changed such that file items would be added or
   * removed from the scope of the list, accessing the list may return an
   * error or the function of the list object may change unexpectedly.
   * <p>
   * Returns null if the directory does not exist.
   */
  List<FileInfo> getSubDirectoryList(String dir);

  /**
   * Returns information about the given file object (file or directory)
   * stored in the repository, or null if the file isn't found.
   */
  FileInfo getFileInfo(String item_name);

  /**
   * Returns a DataFile of the contents of the given file name. Returns null
   * if the file doesn't exist.
   */
  DataFile getDataFile(String file_name);



  /**
   * Commits any changes made to this file repository since it was created.
   * This may throw a commit fault exception if any changes made clash with
   * operations that happened on the file system concurrently (for example,
   * the same file being deleted).
   * <p>
   * In all cases (whether an exception is generated or not), this
   * FileSystem is considered invalidated after this method returns and can
   * not be used again for further file system operations.
   */
  void commit() throws CommitFaultException;

}
