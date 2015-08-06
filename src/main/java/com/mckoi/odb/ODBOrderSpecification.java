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
