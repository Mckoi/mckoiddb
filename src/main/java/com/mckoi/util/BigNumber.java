/**
 * com.mckoi.util.BigNumber  26 Jul 2002
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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Extends BigDecimal to allow a number to be positive infinity, negative
 * infinity and not-a-number.  This provides compatibility with float and
 * double types.
 *
 * @author Tobias Downer
 */

public final class BigNumber extends Number implements Comparable<BigNumber> {

  static final long serialVersionUID = -8681578742639638105L;

  /**
   * State enumerations.
   */
  private final static byte NEG_INF_STATE = 1;
  private final static byte POS_INF_STATE = 2;
  private final static byte NaN_STATE     = 3;

  /**
   * The state of the number, either 0 for number is the BigDecimal, 1 for
   * negative infinity, 2 for positive infinity and 3 for NaN.
   */
  private byte number_state;
  
  /**
   * The BigDecimal representation.
   */
  private BigDecimal big_decimal_n;
  
  /**
   * A 'long' representation of this number.
   */
  private long long_representation;

  /**
   * If this can be represented as an int or long, this contains the number
   * of bytes needed to represent the number.
   */
  private byte byte_count = 120;
  
  /**
   * Constructs the number.
   */
  private BigNumber(byte number_state, BigDecimal big_decimal) {
    this.number_state = number_state;
    if (number_state == 0) {
      setBigDecimal(big_decimal);
    }
  }

  private BigNumber(byte[] buf, int scale, byte state) {
    this.number_state = state;
    if (number_state == 0) {
      BigInteger bigint = new BigInteger(buf);
      setBigDecimal(new BigDecimal(bigint, scale));
    }
  }

  // Only call this from a constructor!
  private void setBigDecimal(BigDecimal big_decimal) {
    if (big_decimal.scale() == 0) {
      BigInteger bint = big_decimal.toBigInteger();
      int bit_count = big_decimal.toBigInteger().bitLength();
      if (bit_count < 30) {
        this.long_representation = bint.longValue();
        this.byte_count = 4;
        this.big_decimal_n = null;
        return;
      }
      else if (bit_count < 60) {
        this.long_representation = bint.longValue();
        this.byte_count = 8;;
        this.big_decimal_n = null;
        return;
      }
    }
    this.big_decimal_n = big_decimal;
  }

  // Internal big_decimal accessor
  private BigDecimal internalBigDecimal() {
    if (byte_count <= 8) {
      return BigDecimal.valueOf(long_representation);
    }
    else {
      return big_decimal_n;
    }
  }
  
  /**
   * Returns true if this BigNumber can be represented by a 64-bit long (has
   * no scale).
   */
  public boolean canBeRepresentedAsLong() {
    return byte_count <= 8;
  }

  /**
   * Returns true if this BigNumber can be represented by a 32-bit int (has
   * no scale).
   */
  public boolean canBeRepresentedAsInt() {
    return byte_count <= 4;
  }
  
  /**
   * Returns the scale of this number, or -1 if the number has no scale (if
   * it -inf, +inf or NaN).
   */
  public int getScale() {
    if (number_state == 0) {
      return internalBigDecimal().scale();
    }
    else {
      return -1;
    }
  }
  
  /**
   * Returns the state of this number.  Returns either 1 which indicates
   * negative infinity, 2 which indicates positive infinity, or 3 which
   * indicates NaN.
   */
  public byte getState() {
    return number_state;
  }
  
  /**
   * Returns the inverse of the state.
   */
  private byte getInverseState() {
    if (number_state == NEG_INF_STATE) {
      return POS_INF_STATE;
    }
    else if (number_state == POS_INF_STATE) {
      return NEG_INF_STATE;
    }
    else {
      return number_state;
    }
  }
  
  /**
   * Returns this number as a byte array (unscaled).
   */
  public byte[] toByteArray() {
    if (number_state == 0) {
      BigDecimal big_d = internalBigDecimal();
      return big_d.movePointRight(big_d.scale()).toBigInteger().toByteArray();
// [ NOTE: The following code is 1.2+ only but BigNumber should be compatible
//         with 1.1 so we use the above call ]
//    return internalBigDecimal().unscaledValue().toByteArray();
    }
    else {
      return new byte[0];
    }
  }

  /**
   * Returns this big number as a string.
   */
  public String toString() {
    switch (number_state) {
      case(0):
        if (canBeRepresentedAsLong()) {
          return Long.toString(long_representation);
        }
        else {
          return internalBigDecimal().toString();
        }
      case(NEG_INF_STATE):
        return "-Infinity";
      case(POS_INF_STATE):
        return "Infinity";
      case(NaN_STATE):
        return "NaN";
      default:
        throw new Error("Unknown number state");
    }
  }

  /**
   * Returns this big number as a double.
   */
  public double doubleValue() {
    switch (number_state) {
      case(0):
        if (canBeRepresentedAsLong()) {
          return (double) long_representation;
        }
        else {
          return internalBigDecimal().doubleValue();
        }
      case(NEG_INF_STATE):
        return Double.NEGATIVE_INFINITY;
      case(POS_INF_STATE):
        return Double.POSITIVE_INFINITY;
      case(NaN_STATE):
        return Double.NaN;
      default:
        throw new Error("Unknown number state");
    }
  }

  /**
   * Returns this big number as a float.
   */
  public float floatValue() {
    switch (number_state) {
      case(0):
        if (canBeRepresentedAsLong()) {
          return (float) long_representation;
        }
        else {
          return internalBigDecimal().floatValue();
        }
      case(NEG_INF_STATE):
        return Float.NEGATIVE_INFINITY;
      case(POS_INF_STATE):
        return Float.POSITIVE_INFINITY;
      case(NaN_STATE):
        return Float.NaN;
      default:
        throw new Error("Unknown number state");
    }
  }

  /**
   * Returns this big number as a long.
   */
  public long longValue() {
    if (canBeRepresentedAsLong()) {
      return long_representation;
    }
    switch (number_state) {
      case(0):
        return internalBigDecimal().longValue();
      default:
        return (long) doubleValue();
    }
  }

  /**
   * Returns this big number as an int.
   */
  public int intValue() {
    if (canBeRepresentedAsLong()) {
      return (int) long_representation;
    }
    switch (number_state) {
      case(0):
        return internalBigDecimal().intValue();
      default:
        return (int) doubleValue();
    }
  }

  /**
   * Returns this big number as a short.
   */
  public short shortValue() {
    return (short) intValue();
  }

  /**
   * Returns this big number as a byte.
   */
  public byte byteValue() {
    return (byte) intValue();
  }


  /**
   * Returns the big number as a BigDecimal object.  Note that this throws
   * an arith error if this number represents NaN, +Inf or -Inf.
   */
  public BigDecimal asBigDecimal() {
    if (number_state == 0) {
      return internalBigDecimal();
    }
    else {
      throw new ArithmeticException(
          "NaN, +Infinity or -Infinity can't be translated to a BigDecimal");
    }
  }

  /**
   * Compares this BigNumber with the given BigNumber.  Returns 0 if the values
   * are equal, >0 if this is greater than the given value, and &lt; 0 if this
   * is less than the given value.
   */
  public int compareTo(BigNumber number) {
    
    if (this == number) {
      return 0;
    }
    
    // If this is a non-infinity number
    if (number_state == 0) {
      
      // If both values can be represented by a long value
      if (canBeRepresentedAsLong() && number.canBeRepresentedAsLong()) {
        // Perform a long comparison check,
        if (long_representation > number.long_representation) {
          return 1;
        }
        else if (long_representation < number.long_representation) {
          return -1;
        }
        else {
          return 0;
        }
    
      }
      
      // And the compared number is non-infinity then use the BigDecimal
      // compareTo method.
      if (number.number_state == 0) {
        return internalBigDecimal().compareTo(number.internalBigDecimal());
      }
      else {
        // Comparing a regular number with a NaN number.
        // If positive infinity or if NaN
        if (number.number_state == POS_INF_STATE ||
            number.number_state == NaN_STATE) {
          return -1;
        }
        // If negative infinity
        else if (number.number_state == NEG_INF_STATE) {
          return 1;
        }
        else {
          throw new Error("Unknown number state.");
        }
      }
    }
    else {
      // This number is a NaN number.
      // Are we comparing with a regular number?
      if (number.number_state == 0) {
        // Yes, negative infinity
        if (number_state == NEG_INF_STATE) {
          return -1;
        }
        // positive infinity or NaN
        else if (number_state == POS_INF_STATE ||
                 number_state == NaN_STATE) {
          return 1;
        }
        else {
          throw new Error("Unknown number state.");
        }
      }
      else {
        // Comparing NaN number with a NaN number.
        // This compares -Inf less than Inf and NaN and NaN greater than
        // Inf and -Inf.  -Inf < Inf < NaN
        return (int) (number_state - number.number_state);
      }
    }
  }
  
  /**
   * The equals comparison uses the BigDecimal 'equals' method to compare
   * values.  This means that '0' is NOT equal to '0.0' and '10.0' is NOT equal
   * to '10.00'.  Care should be taken when using this method.
   */
  public boolean equals(Object ob) {
    BigNumber bnum = (BigNumber) ob;
    if (number_state != 0) {
      return (number_state == bnum.number_state);
    }
    else {
      if (canBeRepresentedAsLong()) {
        return long_representation == bnum.long_representation;
      }
      else {
        return internalBigDecimal().equals(bnum.internalBigDecimal());
      }
    }
  }




  /**
   * Statics.
   */
  private final static BigDecimal BD_ZERO = BigDecimal.valueOf((long) 0);


  
  // ---- Mathematical functions ----

  public BigNumber bitWiseOr(BigNumber number) {
    if (number_state == 0 && getScale() == 0 &&
        number.number_state == 0 && number.getScale() == 0) {
      BigInteger bi1 = internalBigDecimal().toBigInteger();
      BigInteger bi2 = number.internalBigDecimal().toBigInteger();
      return new BigNumber((byte) 0, new BigDecimal(bi1.or(bi2)));
    }
    else {
      return null;
    }
  }

  public BigNumber add(BigNumber number) {
    if (number_state == 0) {
      if (number.number_state == 0) {
        return new BigNumber((byte) 0,
                   internalBigDecimal().add(number.internalBigDecimal()));
      }
      else {
        return new BigNumber(number.number_state, null);
      }
    }
    else {
      return new BigNumber(number_state, null);
    }
  }

  public BigNumber subtract(BigNumber number) {
    if (number_state == 0) {
      if (number.number_state == 0) {
        return new BigNumber((byte) 0,
                   internalBigDecimal().subtract(number.internalBigDecimal()));
      }
      else {
        return new BigNumber(number.getInverseState(), null);
      }
    }
    else {
      return new BigNumber(number_state, null);
    }
  }
  
  public BigNumber multiply(BigNumber number) {
    if (number_state == 0) {
      if (number.number_state == 0) {
        return new BigNumber((byte) 0,
                   internalBigDecimal().multiply(number.internalBigDecimal()));
      }
      else {
        return new BigNumber(number.number_state, null);
      }
    }
    else {
      return new BigNumber(number_state, null);
    }
  }

  public BigNumber divide(BigNumber number) {
    if (number_state == 0) {
      if (number.number_state == 0) {
        BigDecimal div_by = number.internalBigDecimal();
        if (div_by.compareTo(BD_ZERO) != 0) {
          return new BigNumber((byte) 0,
            internalBigDecimal().divide(div_by, 10, BigDecimal.ROUND_HALF_UP));
        }
      }
    }
    // Return NaN if we can't divide
    return new BigNumber((byte) 3, null);
  }

  public BigNumber abs() {
    if (number_state == 0) {
      return new BigNumber((byte) 0, internalBigDecimal().abs());
    }
    else if (number_state == NEG_INF_STATE) {
      return new BigNumber(POS_INF_STATE, null);
    }
    else {
      return new BigNumber(number_state, null);
    }
  }
  
  public int signum() {
    if (number_state == 0) {
      return internalBigDecimal().signum();
    }
    else if (number_state == NEG_INF_STATE) {
      return -1;
    }
    else {
      return 1;
    }
  }

  public BigNumber setScale(int d, int round_enum) {
    if (number_state == 0) {
      return new BigNumber((byte) 0,
                           internalBigDecimal().setScale(d, round_enum));
    }
    // Can't round -inf, +inf and NaN
    return this;
  }

  public BigNumber sqrt() {
    double d = doubleValue();
    d = Math.sqrt(d);
    return fromDouble(d);
  }





  // ---------- Casting from java types ----------

  /**
   * Creates a BigNumber from a double.
   */
  public static BigNumber fromDouble(double value) {
    if (value == Double.NEGATIVE_INFINITY) {
      return NEGATIVE_INFINITY;
    }
    else if (value == Double.POSITIVE_INFINITY) {
      return POSITIVE_INFINITY;
    }
    else if (value != value) {
      return NaN;
    }
    return new BigNumber((byte) 0, new BigDecimal(Double.toString(value)));
  }
      
  /**
   * Creates a BigNumber from a float.
   */
  public static BigNumber fromFloat(float value) {
    if (value == Float.NEGATIVE_INFINITY) {
      return NEGATIVE_INFINITY;
    }
    else if (value == Float.POSITIVE_INFINITY) {
      return POSITIVE_INFINITY;
    }
    else if (value != value) {
      return NaN;
    }
    return new BigNumber((byte) 0, new BigDecimal(Float.toString(value)));
  }

  /**
   * Creates a BigNumber from a long.
   */
  public static BigNumber fromLong(long value) {
    return new BigNumber((byte) 0, BigDecimal.valueOf(value));
  }

  /**
   * Creates a BigNumber from an int.
   */
  public static BigNumber fromInt(int value) {
    return new BigNumber((byte) 0, BigDecimal.valueOf((long) value));
  }

  /**
   * Creates a BigNumber from a string.
   */
  public static BigNumber fromString(String str) {
    if (str.equals("Infinity")) {
      return POSITIVE_INFINITY;
    }
    else if (str.equals("-Infinity")) {
      return NEGATIVE_INFINITY;
    }
    else if (str.equals("NaN")) {
      return NaN;
    }
    else {
      return new BigNumber((byte) 0, new BigDecimal(str));
    }
  }

  /**
   * Creates a BigNumber from a BigDecimal.
   */
  public static BigNumber fromBigDecimal(BigDecimal val) {
    return new BigNumber((byte) 0, val);
  }

  /**
   * Creates a BigNumber from the given data.
   */
  public static BigNumber fromData(byte[] buf, int scale, byte state) {
    if (state == 0) {
      // This inlines common numbers to save a bit of memory.
      if (scale == 0 && buf.length == 1) {
        if (buf[0] == 0) {
          return BIG_NUMBER_ZERO;
        }
        else if (buf[0] == 1) {
          return BIG_NUMBER_ONE;
        }
      }
      return new BigNumber(buf, scale, state);
    }
    else if (state == NEG_INF_STATE) {
      return NEGATIVE_INFINITY;
    }
    else if (state == POS_INF_STATE) {
      return POSITIVE_INFINITY;
    }
    else if (state == NaN_STATE) {
      return NaN;
    }
    else {
      throw new Error("Unknown number state.");
    }
  }


  /**
   * Statics for negative infinity, positive infinity and NaN.
   */
  public static final BigNumber NEGATIVE_INFINITY =
                                      new BigNumber(NEG_INF_STATE, null);
  public static final BigNumber POSITIVE_INFINITY =
                                      new BigNumber(POS_INF_STATE, null);
  public static final BigNumber NaN = new BigNumber(NaN_STATE, null);

  /**
   * Statics for 0 and 1.
   */
  public static final BigNumber BIG_NUMBER_ZERO = BigNumber.fromLong(0);
  public static final BigNumber BIG_NUMBER_ONE = BigNumber.fromLong(1);

}
