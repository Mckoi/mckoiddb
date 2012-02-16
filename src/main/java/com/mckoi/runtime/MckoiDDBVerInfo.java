/**
 * com.mckoi.runtime.MckoiDDBVerInfo  Aug 13, 2009
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

package com.mckoi.runtime;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Standard messages displayed in interactive displays to a user about the
 * software.
 *
 * @author Tobias Downer
 */

public class MckoiDDBVerInfo {

  /**
   * The version string.
   */
  public static final String version_string;

  /**
   * The version main string.
   */
  public static final String version_main;

  /**
   * The license message.
   */
  public static final String license_message;

  /**
   * The distribution release number.
   */
  public static final String release_number;

  /**
   * The build type.
   */
  public static final String build_type;


  static {
    // Set the 'version_string' and 'license_message' values to information
    // read from the VersionInfo.resource file.
    try {
      InputStream in_stream =
            MckoiDDBVerInfo.class.getResourceAsStream("VersionInfo.properties");
      Properties p = new Properties();
      p.load(new BufferedInputStream(in_stream));
      in_stream.close();

      version_main = p.getProperty("mckoiddb.version_main");
      release_number = p.getProperty("mckoiddb.release_number");
      version_string = p.getProperty("mckoiddb.version_string");
      license_message = p.getProperty("mckoiddb.license_message");
      build_type = p.getProperty("mckoiddb.build_type");

    }
    catch (IOException e) {
      throw new RuntimeException("VersionInfo.resource not found");
    }
  }

  /**
   * Returns a version string for display.
   */
  public static String displayVersionString() {
    return version_string + " ver " + version_main + "." + release_number + " (" + build_type + ")";
  }

}
