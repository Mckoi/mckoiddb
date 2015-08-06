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

/**
 * Receives the ongoing process of a synchronize operation (via
 * DirectorySynchronizer).
 *
 * @author Tobias Downer
 */
public interface DirectorySynchronizerFeedback {

  public final static String SKIPPED = "Skipped";
  public final static String WRITTEN = "Written";
  public final static String TOUCHED = "Touched";
  public final static String DELETED = "Deleted";

  /**
   * Called whenever the synchronizer finishes processing a file, called in
   * the order of files being processed. This method would typically be used
   * to provide feedback to the user on what's happening during a synchronize
   * operation.
   * <p>
   * A file process either results in the file being skipped because the
   * timestamp and hash of the source and destination file is equal, the file
   * being written new or updated because the hash is different, the file being
   * touched because the hash is the same but the timestamp is different, or
   * the file being deleted because it only exists in the destination.
   * 
   * @param sync_type either SKIPPED, WRITTEN, UPDATED, DELETED.
   * @param file_name the relative name of the file.
   * @param file_size the size of the destination file (0 if the file is
   *   deleted).
   */
  void reportFileSynchronized(
                      String sync_type, String file_name, long file_size);

  /**
   * Called whenever the synchronizer deletes a directory because it's no
   * longer present in the source tree.
   * 
   * @param directory_name 
   */
  void reportDirectoryRemoved(String directory_name);
  
}
