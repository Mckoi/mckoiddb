/**
 * com.mckoi.util.AnalyticsHistory  Jul 29, 2009
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

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Used to record a set of analytics over some timespan.
 *
 * @author Tobias Downer
 */

public class AnalyticsHistory {

  public static final long DAY = 24 * 60 * 60 * 1000;

  /**
   * Historical information.
   */
  private final LinkedList<long[]> history = new LinkedList();

  /**
   * The number of operations performed in the current timeframe.
   */
  private long ops_count;

  /**
   * Time spent during all operations in the timeframe.
   */
  private long time_in_ops;

  /**
   * The current timeframe start.
   */
  private long timeframe_start;

  /**
   * The minimum size of a timeframe before it's put into the historical list.
   */
  private final long timeframe_size;

  /**
   * The timeframe of recorded events until historical events are purged.
   */
  private final long purge_timeframe;

  /**
   * Constructor.
   */
  public AnalyticsHistory(long timeframe_size, long purge_timeframe) {
    this.timeframe_size = timeframe_size;
    this.purge_timeframe = purge_timeframe;
  }

  /**
   * Constructor, sets timeframe size to a minute, history over a day.
   */
  public AnalyticsHistory() {
    this(1 * 60 * 1000, DAY);
  }

  private void timeframeCheck(long timestamp_recorded) {
    // Go to the next timeframe?
    if ((timestamp_recorded - timeframe_start) > timeframe_size) {
      // Record the timeframe in the history
      long[] record = new long[4];
      record[0] = timeframe_start;     // start
      record[1] = timestamp_recorded;  // end
      record[2] = time_in_ops;         // time in ops during this timeframe
      record[3] = ops_count;           // number of ops during timeframe
      history.addLast(record);
      // Should we clear early events?
      long clear_before = timestamp_recorded - purge_timeframe;
      Iterator<long[]> i = history.iterator();
      while (i.hasNext()) {
        long[] arr = i.next();
        if (arr[1] < clear_before) {
          i.remove();
        }
        else {
          break;
        }
      }
      // Reset the current timeframe stats
      timeframe_start = timestamp_recorded;
      time_in_ops = 0;
      ops_count = 0;
    }
  }

  
  
  /**
   * Refreshes the statistics without changing the statistics.
   */
  public void refresh(long timestamp_now) {
    synchronized (this) {
      timeframeCheck(timestamp_now);
    }
  }

  /**
   * Records an event in the current timeframe.
   */
  public void addEvent(long timestamp_recorded, long time_spent) {
    synchronized (this) {
      time_in_ops += time_spent;
      ops_count += 1;
      timeframeCheck(timestamp_recorded);
    }
  }

  /**
   * Fetch the current historical information. The result long[] array is
   * formatted in groups of 4 long values for each timeframe;
   * [timeframe_start], [timeframe_end], [time_in_ops], [op_count]
   */
  public long[] getStats() {
    // Copy the history,
    synchronized (this) {
      int sz = history.size();
      long[] out = new long[sz * 4];
      int p = 0;
      for (long[] v : history) {
        out[p + 0] = v[0];
        out[p + 1] = v[1];
        out[p + 2] = v[2];
        out[p + 3] = v[3];
        p += 4;
      }
      return out;
    }
  }

  /**
   * Outputs a textural representation of an analytics report (from getStats)
   * to the given PrintWriter. 'item_count' is the range (timescale) of stats
   * to report on, for example 'item_count == 1' will write the stats for the
   * earliest timescale.
   */
  public static void printStatItem(PrintWriter out, long[] stats, int item_count) {
    long op_count = 0;
    long op_time = 0;
    int c = 0;
    for (int i = stats.length - 4; i >= 0 && c < item_count; i -= 4) {
      op_time += stats[i + 2];
      op_count += stats[i + 3];
      ++c;
    }

    if (op_count != 0) {
      BigDecimal avg = BigDecimal.valueOf((double) op_time / (double) op_count);
      avg = avg.setScale(2, BigDecimal.ROUND_UP);

      out.print(op_count);
      out.print("(" + avg.toString() + " ms)");
    }
    else {
      out.print("0(0 ms)");
    }

  }

}
