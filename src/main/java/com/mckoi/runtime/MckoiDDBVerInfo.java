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
