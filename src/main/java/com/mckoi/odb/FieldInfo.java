/**
 * com.mckoi.odb.FieldInfo  Aug 4, 2010
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

package com.mckoi.odb;

public class FieldInfo {

  private final String name;
  private String type;
  private final boolean mutable;

  public FieldInfo(String name, String type, boolean mutable) {
    // NULL checks,
    name.getClass();
    type.getClass();

    this.name = name;
    this.type = type;
    this.mutable = mutable;
  }

  public boolean isMutable() {
    return mutable;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  void setType(String type) {
    this.type = type;
  }

}
