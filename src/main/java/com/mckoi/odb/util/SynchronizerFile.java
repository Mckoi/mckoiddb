/**
 * com.mckoi.odb.util.SynchronizerFile  Mar 14, 2012
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
