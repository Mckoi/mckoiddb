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
