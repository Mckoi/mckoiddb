/**
 * com.mckoi.util.GeneralParser  30 Oct 1998
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

import java.text.CharacterIterator;
import java.text.ParseException;
import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class provides several static convenience functions for parsing
 * various types of character sequences.  In most cases, we use a
 * CharacterIterator to represent the sequence of characters being parsed.
 * <p>
 * @author Tobias Downer
 */

public class GeneralParser {

  /**
   * These statics represent some information about how many milliseconds are
   * in various measures of time.
   */
  private static final BigDecimal MILLIS_IN_WEEK   =
                         BigDecimal.valueOf((long) (7 * 24 * 60 * 60 * 1000));
  private static final BigDecimal MILLIS_IN_DAY    =
                         BigDecimal.valueOf((long) (24 * 60 * 60 * 1000));
  private static final BigDecimal MILLIS_IN_HOUR   =
                         BigDecimal.valueOf((long) (60 * 60 * 1000));
  private static final BigDecimal MILLIS_IN_MINUTE =
                         BigDecimal.valueOf((long) (60 * 1000));
  private static final BigDecimal MILLIS_IN_SECOND =
                         BigDecimal.valueOf((long) 1000);

  /**
   * Parses a string of 0 or more digits and appends the digits into the string
   * buffer.
   */
  public static void parseDigitString(CharacterIterator i, StringBuffer digit_str) {
    char c = i.current();
    while (Character.isDigit(c)) {
      digit_str.append(c);
      c = i.next();
    }
  }

  /**
   * Parses a string of 0 or more words and appends the characters into the
   * string buffer.
   */
  public static void parseWordString(CharacterIterator i,
                                     StringBuffer word_buffer) {
    char c = i.current();
    while (Character.isLetter(c)) {
      word_buffer.append(c);
      c = i.next();
    }
  }

  /**
   * Moves the iterator past any white space.  White space is ' ', '\t', '\n'
   * and '\r'.
   */
  public static void skipWhiteSpace(CharacterIterator i) {
    char c = i.current();
    while (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
      c = i.next();
    }
  }

  /**
   * This assumes there is a decimal number waiting on the iterator.  It
   * parses the decimal and returns the BigDecimal representation.  It throws
   * a GeneralParseException if we are unable to parse the decimal.
   */
  public static BigDecimal parseBigDecimal(CharacterIterator i)
                                                      throws ParseException {
    boolean done_decimal = false;
    StringBuffer str_val = new StringBuffer();

    // We can start with a '-'
    char c = i.current();
    if (c == '-') {
      str_val.append(c);
      c = i.next();
    }
    // We can start or follow with a '.'
    if (c == '.') {
      done_decimal = true;
      str_val.append(c);
      c = i.next();
    }
    // We must be able to parse a digit
    if (!Character.isDigit(c)) {
      throw new ParseException("Parsing BigDecimal", i.getIndex());
    }
    // Parse the digit string
    parseDigitString(i, str_val);
    // Is there a decimal part?
    c = i.current();
    if (!done_decimal && c == '.') {
      str_val.append(c);
      c = i.next();
      parseDigitString(i, str_val);
    }

    return new BigDecimal(new String(str_val));
  }

  /**
   * Parses a time grammer waiting on the character iterator.  The grammer is
   * quite simple.  It allows for us to specify quite precisely some unit of
   * time measure and convert it to a Java understandable form.  It returns the
   * number of milliseconds that the unit of time represents.
   * For example, the string '2.5 hours' would return:
   *   2.5 hours * 60 minutes * 60 seconds * 1000 milliseconds = 9000000
   * <p>
   * To construct a valid time measure, you must supply a sequence of time
   * measurements.  The valid time measurements are 'week(s)', 'day(s)',
   * 'hour(s)', 'minute(s)', 'second(s)', 'millisecond(s)'.  To construct a
   * time, we simply concatinate the measurements together.  For example,
   *   '3 days 22 hours 9.5 minutes'
   * <p>
   * It accepts any number of time measurements, but not duplicates of the
   * same.
   * <p>
   * The time measures are case insensitive.  It is a little lazy how it reads
   * the grammer.  We could for example enter '1 hours 40 second' or even
   * more extreme, '1 houraboutit 90 secondilianit' both of which are
   * acceptable!
   * <p>
   * This method will keep on parsing the string until the end of the iterator
   * is reached or a non-numeric time measure is found.  It throws a
   * ParseException if an invalid time measure is found or a number is invalid
   * (eg. -3 days).
   * <p>
   * LOCALE ISSUE: This will likely be a difficult method to localise.
   */
  public static BigDecimal parseTimeMeasure(CharacterIterator i)
                                                        throws ParseException {
    boolean time_measured = false;
    BigDecimal time_measure = BigDecimal.valueOf((long) 0);
    boolean[] time_parsed = new boolean[6];
    StringBuffer word_buffer = new StringBuffer();
    BigDecimal num;

    while (true) {
      // Parse the number
      skipWhiteSpace(i);
      try {
        num = parseBigDecimal(i);
      }
      catch (ParseException e) {
        // If we can't parse a number, then return with the current time if
        // any time has been parsed.
        if (time_measured) {
          return time_measure;
        }
        else {
          throw new ParseException("No time value found", i.getIndex());
        }
      }
      if (num.signum() < 0) {
        throw new ParseException("Invalid time value: " + num, i.getIndex());
      }

      skipWhiteSpace(i);

      // Parse the time measure
      word_buffer.setLength(0);
      parseWordString(i, word_buffer);

      String str = new String(word_buffer).toLowerCase();
      if ((str.startsWith("week") ||
           str.equals("w")) &&
          !time_parsed[0]) {
        time_measure = time_measure.add(num.multiply(MILLIS_IN_WEEK));
        time_parsed[0] = true;
      }
      else if ((str.startsWith("day") ||
                str.equals("d")) &&
               !time_parsed[1]) {
        time_measure = time_measure.add(num.multiply(MILLIS_IN_DAY));
        time_parsed[1] = true;
      }
      else if ((str.startsWith("hour") ||
                str.startsWith("hr") ||
                str.equals("h")) &&
               !time_parsed[2]) {
        time_measure = time_measure.add(num.multiply(MILLIS_IN_HOUR));
        time_parsed[2] = true;
      }
      else if ((str.startsWith("minute") ||
                str.startsWith("min") ||
                str.equals("m")) &&
               !time_parsed[3]) {
        time_measure = time_measure.add(num.multiply(MILLIS_IN_MINUTE));
        time_parsed[3] = true;
      }
      else if ((str.startsWith("second") ||
                str.startsWith("sec") ||
                str.equals("s")) &&
               !time_parsed[4]) {
        time_measure = time_measure.add(num.multiply(MILLIS_IN_SECOND));
        time_parsed[4] = true;
      }
      else if ((str.startsWith("millisecond") ||
                str.equals("ms")) &&
               !time_parsed[5]) {
        time_measure = time_measure.add(num);
        time_parsed[5] = true;
      }
      else {
        throw new ParseException("Unknown time measure: " + str, i.getIndex());
      }
      time_measured = true;

    }

  }

  /**
   * Parses a size specification. For example, '490' is parsed to 490,
   * '40000' is parsed to 40000, '12KB' is parsed to (12 * 1024),
   * '32MB' is parsed to (32 * 1024 * 1024), etc.
   */
  public static long parseSizeByteFormat(String str) {
    Pattern p1 =
        Pattern.compile("(\\d+)\\s*(KB|MB|GB|TB|B)", Pattern.CASE_INSENSITIVE);
    Pattern p2 =
        Pattern.compile("(\\d+)", Pattern.CASE_INSENSITIVE);
    Matcher m1 = p1.matcher(str);
    Matcher m2 = p2.matcher(str);

    if (m1.matches()) {
      String numerical = m1.group(1);
      String unit = m1.group(2).toUpperCase();
      long num = Long.parseLong(numerical);
      if (unit.equals("KB")) {
        return num * 1024;
      }
      if (unit.equals("MB")) {
        return num * 1024 * 1024;
      }
      if (unit.equals("GB")) {
        return num * 1024 * 1024 * 1024;
      }
      if (unit.equals("TB")) {
        return num * 1024 * 1024 * 1024 * 1024;
      }

      // Must be in bytes
      return num;

    }
    else if (m2.matches()) {
      return Long.parseLong(str);
    }

    // Illegal number,
    throw new NumberFormatException(str);
  }

}
