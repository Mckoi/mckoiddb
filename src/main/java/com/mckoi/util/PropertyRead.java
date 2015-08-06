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

package com.mckoi.util;

/**
 * Interface for reading properties.
 *
 * @author Tobias Downer
 */

public interface PropertyRead {

  /**
   * Gets a property, or returns null if the property isn't set.
   */
  public String getProperty(String name);

  /**
   * Gets a property, or returns the default value if the property isn't set.
   */
  public String getProperty(String name, String default_value);

  /**
   * Gets an integer value, or returns the default value if the property isn't
   * set.
   */
  public int getIntegerProperty(String name, int default_value);

  /**
   * Gets a long value, or returns the default value if the property isn't
   * set.
   */
  public long getLongProperty(String name, long default_value);

  /**
   * Gets a boolean value, or returns the default value if the property
   * isn't set.
   */
  public boolean getBooleanProperty(String name, boolean default_value);
  
}
