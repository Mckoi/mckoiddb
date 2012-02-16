/**
 * com.mckoi.odb.ODBOrderSpecification  Oct 29, 2010
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

/**
 * An order specification for a list of items.
 *
 * @author Tobias Downer
 */

public class ODBOrderSpecification {

  /**
   * The name of the order function.
   */
  private final String order_function;

  /**
   * The member name in the backed object being sorted against.
   */
  private final String member_name;

  /**
   * If true then the order is inversed.
   */
  private final boolean inverse;


  /**
   * Constructor.
   */
  ODBOrderSpecification(String order_function,
                        String member_name, boolean inverse) {
    this.order_function = order_function;
    this.member_name = member_name;
    this.inverse = inverse;
  }

  /**
   * Returns the order function.
   */
  String getOrderFunction() {
    return order_function;
  }

  /**
   * Returns the member name.
   */
  String getMemberName() {
    return member_name;
  }

  /**
   * Returns true if the order is inversed.
   */
  boolean isInverse() {
    return inverse;
  }

  /**
   * Returns an inverse order of this specification.
   */
  public ODBOrderSpecification inverse() {
    return new ODBOrderSpecification(
                        this.order_function, this.member_name, !this.inverse);
  }

  // ----- Static methods -----

  public static ODBOrderSpecification lexicographic(String member_name) {
    return new ODBOrderSpecification("lexi", member_name, false);
  }

}
