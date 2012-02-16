/**
 * com.mckoi.data.AtomicKey  Nov 2, 2008
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

package com.mckoi.data;

/**
 * An object that represents the identity of an atomic element in a database.
 * The design of atomic key elements are intended to mirror that of the
 * Key object.
 * <p>
 * All keys have a type, a secondary and primary component.  In combination,
 * the key is 14 bytes of information in total.
 *
 * @author Tobias Downer
 */

public final class AtomicKey extends AbstractKey {

  /**
   * Constructs the key with a key type (16 bits), a secondary key value
   * (32 bits), and a primary key value (64 bits).
   */
  public AtomicKey(short type, int secondary_key, long primary_key) {
    super(type, secondary_key, primary_key);
  }

  /**
   * Returns a string representation of the key.
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("(");
    buf.append(getSecondary());
    buf.append("-");
    buf.append(getType());
    buf.append("-");
    buf.append(getPrimary());
    buf.append(")");
    return buf.toString();
  }

}
