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

import java.io.IOException;
import java.util.List;

/**
 * An abstraction used by the directory synchronizer to represent a repository
 * in which files can be written to and read from.
 *
 * @author Tobias Downer
 */

public interface SynchronizerRepository {

  /**
   * Returns the list of all the files in the given directory, where '/' is
   * the base directory. The returned names do not include the given
   * path.
   */
  List<SynchronizerFile> allFiles(String path);

  /**
   * Returns the list of all the subdirectories in the given directory, where
   * '/' is the base directory. The returned names do not include the given
   * path.
   */
  List<String> allSubDirectories(String path);

  /**
   * Returns the FileObject for the given path + file name.
   */
  SynchronizerFile getFileObject(String path, String file_name);

  /**
   * Copies the entire content of the given file object to this repository at
   * the given path.
   */
  void writeCopyOf(SynchronizerFile file_ob, String path) throws IOException;

  /**
   * Returns true if the given path exists in this repository.
   */
  boolean hasDirectory(String path);

  /**
   * Makes the given path in this repository.
   */
  void makeDirectory(String path);

  /**
   * Removes the given path and all its sub-directories/files.
   */
  void removeDirectory(String path);

}
