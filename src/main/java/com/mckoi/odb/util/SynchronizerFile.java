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
import java.io.InputStream;
import java.io.OutputStream;

/**
 * An abstraction of a file used by the directory synchronizer.
 *
 * @author Tobias Downer
 */

public interface SynchronizerFile {

  /**
   * The name of the file object (eg. 'wibble.txt')
   */
  String getName();

  /**
   * The file size in bytes.
   */
  long getSize();

  /**
   * The time stamp of the file.
   */
  long getTimestamp();

  /**
   * Sets the timestamp of this file (eg. 'touch').
   */
  void setTimestamp(long timestamp) throws IOException;

  /**
   * The SHA-256 hash of the content of the complete file.
   */
  byte[] getSHAHash() throws IOException;

  /**
   * Creates a zero length file.
   */
  void create() throws IOException;

  /**
   * Deletes this file object from the repository it belongs to.
   */
  void delete() throws IOException;

  /**
   * Returns true if the file exists.
   */
  boolean exists();

  /**
   * Input stream reads from the file.
   */
  InputStream getInputStream() throws IOException;

  /**
   * Output stream writes to the file. Calling this will reset the size of the
   * file to zero.
   */
  OutputStream getOutputStream() throws IOException;

}
