/**
 * com.mckoi.treestore.PropertyRead  Dec 15, 2007
 *
 * Mckoi Database Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2010  Diehl and Associates, Inc.
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
