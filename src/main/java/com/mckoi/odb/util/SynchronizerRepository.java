/**
 * com.mckoi.odb.util.SynchronizerRepository  Mar 14, 2012
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
 * writeCopyOf to the Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * Change Log:
 *
 *
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
   * the base directory.
   */
  List<SynchronizerFile> allFiles(String path);

  /**
   * Returns the list of all the subdirectories in the given directory, where
   * '/' is the base directory.
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
