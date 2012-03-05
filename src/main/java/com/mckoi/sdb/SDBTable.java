/**
 * com.mckoi.sdb.SDBTable  Jul 6, 2009
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

package com.mckoi.sdb;

import com.mckoi.data.*;
import com.mckoi.util.StringUtil;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.Collator;
import java.util.*;

/**
 * SDBTable is a table structure with a single cell value type (strings), and
 * supports ordered column indexes and a single row index ordered by
 * insert order.
 * <p>
 * The table structure is primarily supported by a single dimension row index
 * that is an append only list of row keys. Each row key entry references a
 * DataFile that contains formatted row data information. Column indexes are
 * each supported by a sorted (by column value) list of row keys.
 * <p>
 * Row updates require creating a duplicate of the row and deleting the
 * original. All inserts, updates and deletes are recorded in a transaction
 * log, and will fail during commit only when the change is incompatible (for
 * example, two transactions delete the same row). New row inserts will not
 * fail for any reason at commit. Concurrent structural changes to a single
 * table will fail at commit.
 * <p>
 * This is a simple implementation of a table structure that may not be
 * optimal for all uses, such as intensive column index queries. Information
 * is grouped by row which offers good performance for many types of online
 * applications.
 *
 * @author Tobias Downer
 */

public class SDBTable implements Iterable<SDBRow> {

  /**
   * The unique table_id given this table within this transaction.
   */
  private final int table_id;

  /**
   * The backed transaction object.
   */
  private final SDBTransaction transaction;

  /**
   * The table properties file.
   */
  private final DataFile properties_file;

  /**
   * The key for the row index.
   */
  private final Key row_index_key;

  // ----- Locally cached info

  /**
   * A Map of column name to columnid, cached for improved performance.
   */
  private HashMap<String, Long> column_id_map = null;

  private String[] cached_column_list = null;
  private String[] cached_index_list = null;

  // ----- Mutation state

  /**
   * The row buffer contains the values inserted or updated.
   */
  private HashMap<String, String> row_buffer = null;

  /**
   * The rowid of the element being update, or -1 if rowid being inserted,
   * or 0 if a row is not being updated or inserted.
   */
  private long row_buffer_id = 0;

  /**
   * The current version, updated whenever a mutation made to the table, used
   * as a fast fail mechanism on the iterators.
   */
  private long current_version = 0;

  /**
   * The current id generation key.
   */
  private long current_id_gen = -1;


  /**
   * The set of rowids inserted into the table during the lifespan of this
   * object.
   */
  private ArrayList<Long> add_row_list = new ArrayList();

  /**
   * The set of rowids deleted from the table during the lifespan of this
   * object.
   */
  private ArrayList<Long> delete_row_list = new ArrayList();

  /**
   * Set to true if a structural modification happens to this table.
   */
  private boolean structural_modification = false;



  /**
   * Constructor.
   */
  SDBTable(SDBTransaction transaction, DataFile properties_file, int table_id) {
    if (table_id < 1) {
      throw new RuntimeException("table_id out of range.");
    }

    this.transaction = transaction;
    this.table_id = table_id;
    this.properties_file = properties_file;

    // Setup various key objects,

    row_index_key =              new Key((short) 1, table_id, 1);
  }

  Key getTransactionPropertiesLog() {
    return new Key((short) 1, table_id, 2);
  }
  Key getTransactionAddLog() {
    return new Key((short) 1, table_id, 3);
  }
  Key getTransactionRemoveLog() {
    return new Key((short) 1, table_id, 4);
  }

  boolean wasModified() {
    return (current_version > 0);
  }

  // ----- Event log

  boolean hasStructuralModification() {
    return structural_modification;
  }

  void prepareForCommit() {
    // Write the transaction log for this table,
    DataFile df = getDataFile(getTransactionAddLog());
    df.delete();
    OrderedList64Bit addlist = new OrderedList64Bit(df);
    for (Long v : add_row_list) {
      addlist.insertSortKey(v);
    }
    df = getDataFile(getTransactionRemoveLog());
    df.delete();
    OrderedList64Bit deletelist = new OrderedList64Bit(df);
    for (Long v : delete_row_list) {
      if (addlist.containsSortKey(v)) {
        addlist.removeSortKey(v);
      }
      else {
        deletelist.insertSortKey(v);
      }
    }
    // Set the id gen key
    if (current_id_gen != -1) {
      PropertySet p = getTableProperties();
      p.setLongProperty("k", current_id_gen);
    }
  }

  OrderedList64Bit getDeleteSet() {
    DataFile df = getDataFile(getTransactionRemoveLog());
    return new OrderedList64Bit(df);
  }

  OrderedList64Bit getAddSet() {
    DataFile df = getDataFile(getTransactionAddLog());
    return new OrderedList64Bit(df);
  }

  private void addTransactionEvent(String cmd, String arg) {
    structural_modification = true;
  }

  private void addTransactionEvent(String cmd, long arg) {
    if (cmd.equals("insertRow")) {
      add_row_list.add(arg);
    }
    else if (cmd.equals("deleteRow")) {
      delete_row_list.add(arg);
    }
    else {
      throw new RuntimeException("Unknown transaction command: " + cmd);
    }
  }

  // ----- Transactional

  private static void copyDF(DataFile s, DataFile d) {
//    d.delete();
//    s.position(0);
//    d.position(0);
//    s.copyTo(d, s.size());
    s.replicateTo(d);
  }

  /**
   * Merges the changes in the given table into this table. If
   * structural_change is true, it means the 'from' table is a structurally
   * modified version of this table. If historic_data_change is true, it means
   * the version of this table is not an immediate descendant of the 'from'
   * table which means certain optimizations can't be performed.
   */
  void mergeFrom(SDBTable from, boolean structural_change,
                 boolean historic_data_change) {

    // If structural_change is true, this can only happen if 'from' is the
    // immediate child of this table.
    // If 'historic_data_change' is false, this can only happen if 'from' is
    // the immediate child of this table.

    // Handle structural change,
    if (structural_change == true || historic_data_change == false) {
      // Fetch all the indexes,
      String[] from_indexes = from.getIndexedColumnList();
      ArrayList<Long> from_index_ids = new ArrayList(from_indexes.length);
      for (String findex : from_indexes) {
        from_index_ids.add(from.getColumnId(findex));
      }
      // Copy them into here,
      copyDF(from.getDataFile(from.row_index_key),
             getDataFile(row_index_key));
      for (long index_id : from_index_ids) {
        // Copy all the indexes here,
        copyDF(from.getDataFile(from.getIndexIdKey(index_id)),
               getDataFile(getIndexIdKey(index_id)));
      }
      // Move the column and index information into this table,
//      copyDF(from.getDataFile(from.table_properties_key),
//             getDataFile(table_properties_key));
      copyDF(from.properties_file,
             properties_file);

      // Copy the transaction logs
      copyDF(from.getDataFile(from.getTransactionAddLog()),
             getDataFile(getTransactionAddLog()));
      copyDF(from.getDataFile(from.getTransactionRemoveLog()),
             getDataFile(getTransactionRemoveLog()));

      // Replay the add and remove transaction events
      OrderedList64Bit add_events = new OrderedList64Bit(
                            from.getDataFile(from.getTransactionAddLog()));
      OrderedList64Bit remove_events = new OrderedList64Bit(
                            from.getDataFile(from.getTransactionRemoveLog()));
      // Adds
      {
        Iterator64Bit i = add_events.iterator();
        while (i.hasNext()) {
          long rowid = i.next();
          copyDF(from.getDataFile(from.getRowIdKey(rowid)),
                 getDataFile(getRowIdKey(rowid)));
        }
      }
      // Removes
      {
        Iterator64Bit i = remove_events.iterator();
        while (i.hasNext()) {
          long rowid = i.next();
          // Delete the row data file,
          getDataFile(getRowIdKey(rowid)).delete();
        }
      }
    }
    else {
      // If we are here, then we are merging a change that isn't a structural
      // change, and there are historical changes. Basically this means we
      // need to replay the add and remove events only, but more strictly,

      // Replay the add and remove transaction events
      OrderedList64Bit add_events = new OrderedList64Bit(
                            from.getDataFile(from.getTransactionAddLog()));
      OrderedList64Bit remove_events = new OrderedList64Bit(
                            from.getDataFile(from.getTransactionRemoveLog()));
      // Adds
      {
        Iterator64Bit i = add_events.iterator();
        while (i.hasNext()) {
          long from_rowid = i.next();
          // Generate a new id for the row,
          long to_rowid = generateId();
          // Copy record to the new id in this table,
          copyDF(from.getDataFile(from.getRowIdKey(from_rowid)),
                 getDataFile(getRowIdKey(to_rowid)));
          // Update indexes,
          addRowToRowSet(to_rowid);
          addRowToIndexSet(to_rowid);
          // Add this event to the transaction log,
          addTransactionEvent("insertRow", to_rowid);
        }
      }
      // Removes
      {
        Iterator64Bit i = remove_events.iterator();
        while (i.hasNext()) {
          long from_rowid = i.next();
          // Update indexes,
          removeRowFromRowSet(from_rowid);
          removeRowFromIndexSet(from_rowid);

          // Delete the row data file,
          getDataFile(getRowIdKey(from_rowid)).delete();

          // Add this event to the transaction log,
          addTransactionEvent("deleteRow", from_rowid);
        }
      }
      // Write out the transaction logs,
      prepareForCommit();
    }

    // Invalidate all the cached info,
    column_id_map = null;
    cached_column_list = null;
    cached_index_list = null;

    ++current_version;

  }

  // -----

  long getCurrentVersion() {
    return current_version;
  }

  /**
   * Get the row key for the given rowid.
   */
  private Key getRowIdKey(long rowid) {
    // Sanity check to prevent corruption of the table state
    if (rowid <= 12) {
      throw new RuntimeException("rowid value out of bounds.");
    }
    return new Key((short) 1, table_id, rowid);
  }

  /**
   * Get the index structure key for the given column id.
   */
  private Key getIndexIdKey(long columnid) {
    // Sanity check to prevent corruption of the table state
    if (columnid <= 12) {
      throw new RuntimeException("rowid value out of bounds.");
    }
    return new Key((short) 1, table_id, columnid);
  }



  /**
   * Returns the DataFile from the backed transaction, given its key.
   */
  private DataFile getDataFile(Key k) {
    return transaction.getKeyObjectTransaction().getDataFile(k, 'w');
  }

  /**
   * Returns the PropertySet for properties of this table.
   */
  private PropertySet getTableProperties() {
    return new PropertySet(properties_file);
  }

  /**
   * Return a new unique identifier.
   */
  private long generateId() {
    if (current_id_gen == -1) {
      PropertySet p = getTableProperties();
      long v = p.getLongProperty("k", 16);
      current_id_gen = v;
    }
    ++current_id_gen;
    return current_id_gen - 1;
  }

  /**
   * Checks the name of the
   */
  private void checkColumnNameValid(String column_name) {
    if (column_name.length() <= 0 || column_name.length() > 1024) {
      throw new RuntimeException("Invalid column name size");
    }
    int sz = column_name.length();
    for (int i = 0; i < sz; ++i) {
      char c = column_name.charAt(i);
      if (c == '.' || c == ',' || Character.isSpaceChar(c)) {
        throw new RuntimeException("Invalid character in column name");
      }
    }
  }

  /**
   * Checks that the given Locale is valid, and returns it if it is. The
   * encoding accepted is 2 char ISO language/2 char ISO Country. For example,
   * "enUK" (English UK), "frFR" (French France), "frCA" (French Canada), etc
   */
  private Locale getAndCheckLocale(String encoded_str) {
    Locale l;

    if (encoded_str.length() == 2) {
      String lang = encoded_str;
      l = new Locale(lang);
    }
    else if (encoded_str.length() == 4) {
      String lang = encoded_str.substring(0, 2);
      String country = encoded_str.substring(2);
      l = new Locale(lang, country);
    }
    else {
      throw new RuntimeException("Invalid locale encoding");
    }

    // Do a test compare with the collator,
    Collator c = Collator.getInstance(l);
    c.compare("a", "b");

    return l;
  }

  /**
   * Returns the IndexObjectCollator for the given column index.
   */
  private IndexObjectCollator getIndexCollatorFor(
                                           String column_name, long columnid) {
    PropertySet p = getTableProperties();
    String collator_str = p.getProperty(column_name + "collator", null);
    if (collator_str == null) {
      return new SDBLexiStringCollator(columnid);
    }
    else {
      Locale picked_locale = getAndCheckLocale(collator_str);
      return new SDBLocaleStringCollator(columnid, picked_locale);
    }
  }

  /**
   * Builds an index for the given column. If an index already exists it is
   * deleted and rebuilt from scratch. This method iterates over every row in
   * the table so should be used carefully.
   */
  private void buildIndex(long columnid, IndexObjectCollator index_collator) {
    // Get the index object
    DataFile df = getDataFile(getIndexIdKey(columnid));
    // Get the index and clear it,
    OrderedList64Bit index = new OrderedList64Bit(df);
    index.clear();
//    // Create an index collator,
//    IndexObjectCollator index_collator = new SDBLexiStringCollator(columnid);
    // For each row in this table,
    for(SDBRow row : this) {
      // Get the column value and the rowid
      String column_value = row.getValue(columnid);
      long rowid = row.getRowIdValue();
      // Add it into the index,
      index.insert(column_value, rowid, index_collator);
    }
    // Done.
  }

  /**
   * Adds a row to the set of index lists defined on this table (if any).
   */
  private void addRowToIndexSet(long rowid) {
    // Get the set of columns that are indexed in this table,
    String[] indexed_cols = getIndexedColumnList();
    for (String col : indexed_cols) {
      // Resolve the column name to an id, turn it into a OrderedList64Bit,
      // and insert the row in the correct location in the index,
      long columnid = getColumnId(col);
      DataFile df = getDataFile(getIndexIdKey(columnid));
      OrderedList64Bit index = new OrderedList64Bit(df);
      IndexObjectCollator index_collator = getIndexCollatorFor(col, columnid);
//      IndexObjectCollator index_collator = new SDBLexiStringCollator(columnid);
      index.insert(getCellValue(rowid, columnid), rowid, index_collator);
    }
  }

  /**
   * Removes a row from the set of index lists defined on this table (if any).
   */
  private void removeRowFromIndexSet(long rowid) {
    // Get the set of columns that are indexed in this table,
    String[] indexed_cols = getIndexedColumnList();
    // For each index,
    for (String col : indexed_cols) {
      // Resolve the column name to an id, turn it into a OrderedList64Bit,
      // and insert the row in the correct location in the index,
      long columnid = getColumnId(col);
      DataFile df = getDataFile(getIndexIdKey(columnid));
      OrderedList64Bit index = new OrderedList64Bit(df);
      IndexObjectCollator index_collator = getIndexCollatorFor(col, columnid);
//      IndexObjectCollator index_collator = new SDBLexiStringCollator(columnid);
      index.remove(getCellValue(rowid, columnid), rowid, index_collator);
    }
  }


  private static String debugList(OrderedList64Bit list) {
    Iterator64Bit i = list.iterator();
    StringBuilder b = new StringBuilder();
    while (i.hasNext()) {
      long v = i.next();
      b.append(v);
      b.append(",");
    }
    return b.toString();
  }


  /**
   * Adds a rowid to the main index.
   */
  private void addRowToRowSet(long rowid) {
    // Get the index object
    DataFile df = getDataFile(row_index_key);
    OrderedList64Bit row_set = new OrderedList64Bit(df);
    // Add the row in rowid value sorted order
    row_set.insertSortKey(rowid);
  }

  /**
   * Removes a rowid from the main index.
   */
  private void removeRowFromRowSet(long rowid) {
    // Get the index object
    DataFile df = getDataFile(row_index_key);
    OrderedList64Bit row_set = new OrderedList64Bit(df);
    // Remove the row in rowid value sorted order
    row_set.removeSortKey(rowid);
  }

  /**
   * Performs a column_name to columnid lookup.
   */
  long getColumnId(String column_name) {
    // Maps a column name to the id assigned it. This method is backed by a
    // local cache to improve frequent lookup operations.
    checkColumnNameValid(column_name);
    if (column_id_map == null) {
      column_id_map = new HashMap();
    }
    Long column_id = column_id_map.get(column_name);
    if (column_id == null) {
      PropertySet p = getTableProperties();
      column_id = p.getLongProperty(column_name + ".id", -1);
      if (column_id == -1) {
        throw new RuntimeException("Column '" + column_name + "' not found");
      }
      column_id_map.put(column_name, column_id);
    }
    return column_id;
  }

  /**
   * Returns the value of the given cell of the table addressed at the given
   * rowid/columnid
   */
  String getCellValue(long rowid, long columnid) {
    Key row_key = getRowIdKey(rowid);
    DataFile row_file = getDataFile(row_key);
    RowBuilder row_builder = new RowBuilder(row_file);
    return row_builder.getValue(columnid);
  }

  /**
   * Provides a hint to prefetch the record with the given id.
   */
  void prefetchValueHint(long rowid, long columnid) {
    // Create the key object,
    Key row_key = getRowIdKey(rowid);
    // Pass the hint on to the backed transaction,
    transaction.getKeyObjectTransaction().prefetchKeys(new Key[] { row_key });
  }

  /**
   * Adds a string to a column set string.
   */
  String addToColumnSet(String name, String set) {
    if (set == null || set.equals("")) {
      return name;
    }
    else {
      ArrayList cols = new ArrayList(StringUtil.explode(set, ","));
      cols.add(name);
      return StringUtil.implode(cols, ",");
    }
  }

  /**
   * Removes a string from a column set string.
   */
  String removeFromColumnSet(String name, String set) {
    if (set != null && !set.equals("")) {
      ArrayList cols = new ArrayList(StringUtil.explode(set, ","));
      boolean removed = cols.remove(name);
      if (removed) {
        return StringUtil.implode(cols, ",");
      }
    }
    throw new RuntimeException("Column '" + name + "' not found");
  }


  /**
   * Fully deletes all data associated with this object from the backed
   * transaction. This must iterate through every row in the table, so it may
   * take some time to complete.
   */
  void deleteFully() {
    // Get the range of keys stored for this table, and delete it.
    DataRange data_range =
              transaction.getKeyObjectTransaction().getDataRange(
                         new Key((short)1, table_id, 0),
                         new Key((short)1, table_id, Long.MAX_VALUE));
    data_range.delete();

//    DataFile df;
//    // Get the index list,
//    String[] cols = getIndexedColumnList();
//    // Delete all the indexes,
//    for (String col : cols) {
//      long columnid = getColumnId(col);
//      df = getDataFile(getIndexIdKey(columnid));
//      df.delete();
//    }
//    // Delete all the rows in reverse,
//    for (SDBRow row : this) {
//      long rowid = row.getRowIdValue();
//      df = getDataFile(getRowIdKey(rowid));
//      df.delete();
//    }
//    // Delete the main index,
//    df = getDataFile(row_index_key);
//    df.delete();
////    // Delete the properties,
////    df = getDataFile(table_properties_key);
////    df.delete();
//    // Delete the transaction info,
//    df = getDataFile(getTransactionAddLog());
//    df.delete();
//    df = getDataFile(getTransactionRemoveLog());
//    df.delete();
  }

  // ----- Structural changes/queries

  /**
   * Returns the number of rows currently stored in this table.
   */
  public long getRowCount() {
    // Get the main index file
    DataFile df = getDataFile(row_index_key);
    // Get the index,
    OrderedList64Bit index = new OrderedList64Bit(df);
    // Return the row count,
    return index.size();
  }

  /**
   * Returns the number of columns currently defined on this table.
   */
  public long getColumnCount() {
    return getColumnList().length;
  }

  /**
   * Return the columns defined on this table in the order they were added.
   */
  public String[] getColumnList() {
    if (cached_column_list == null) {
      // Return the column list
      PropertySet p = getTableProperties();
      String column_list = p.getProperty("column_list", "");
      String[] col_arr;
      if (!column_list.equals("")) {
        List<String> l = StringUtil.explode(column_list, ",");
        col_arr = l.toArray(new String[l.size()]);
      }
      else {
        col_arr = new String[0];
      }
      cached_column_list = col_arr;
    }
    return cached_column_list.clone();
  }

  /**
   * Returns the list of columns that have an index defined on them.
   */
  public String[] getIndexedColumnList() {
    if (cached_index_list == null) {
      // Return the column list
      PropertySet p = getTableProperties();
      String column_list = p.getProperty("index_column_list", "");
      String[] col_arr;
      if (!column_list.equals("")) {
        List<String> l = StringUtil.explode(column_list, ",");
        col_arr = l.toArray(new String[l.size()]);
      }
      else {
        col_arr = new String[0];
      }
      cached_index_list = col_arr;
    }
    return cached_index_list.clone();
//    // PENDING: CACHE this?
//    // Return the column list
//    PropertySet p = getTableProperties();
//    String column_list = p.getProperty("index_column_list", "");
//    if (!column_list.equals("")) {
//      List<String> l = StringUtil.explode(column_list, ",");
//      return l.toArray(new String[l.size()]);
//    }
//    else {
//      return new String[0];
//    }
  }

  /**
   * Returns true if the column is indexed.
   */
  public boolean isColumnIndexed(String column_name) {
    // PENDING: CACHE this?
    checkColumnNameValid(column_name);
    PropertySet p = getTableProperties();
    return p.getBooleanProperty(column_name + ".index", false);
  }


  /**
   * Adds an index on the column with the given name. This builds the index
   * on the column so may take a long time to perform if there is a lot of
   * information stored in the table.
   */
  public void addColumn(String column_name) {
    checkColumnNameValid(column_name);

    // Generate a column id
    long columnid = generateId();
    PropertySet p = getTableProperties();
    // Add to the column list,
    String column_list = p.getProperty("column_list", "");
    column_list = addToColumnSet(column_name, column_list);
    p.setProperty("column_list", column_list);
    // Set a column name to columnid map,
    p.setLongProperty(column_name + ".id", columnid);

    cached_column_list = null;
    // Add this event to the transaction log,
    addTransactionEvent("addColumn", column_name);
    ++current_version;
  }

  /**
   * Removes a column with the given name from the table.
   */
  public void removeColumn(String column_name) {
    checkColumnNameValid(column_name);

    PropertySet p = getTableProperties();
    // Add to the column list,
    String column_list = p.getProperty("column_list", "");
    column_list = removeFromColumnSet(column_name, column_list);
    // Check if column is indexed, generate error if it is,
    boolean is_indexed = p.getBooleanProperty(column_name + ".index", false);
    if (is_indexed) {
      throw new RuntimeException("Can't remove column " + column_name +
                                 " because it has an index");
    }
    // Otherwise update and remove the column
    p.setProperty("column_list", column_list);
    // Set a column name to columnid map,
    p.setProperty(column_name + ".id", null);
    // Remove from column_id cache,
    if (column_id_map != null) {
      column_id_map.remove(column_name);
    }

    // PENDING: Remove the column data?


    cached_column_list = null;
    // Add this event to the transaction log,
    addTransactionEvent("removeColumn", column_name);
    ++current_version;
  }

  /**
   * Adds an index of the given column in the table with the given locale used
   * as the source of collation for the index. The 'collator_locale' encoded
   * string is formatted as 2 char ISO language/2 char ISO Country. For example,
   * "enUK" (English UK), "frFR" (French France), "frCA" (French Canada), etc.
   * If 'collator_locale' is null then the index is ordered lexicographically.
   */
  public void addIndex(String column_name, String collator_locale) {
    checkColumnNameValid(column_name);

    PropertySet p = getTableProperties();
    // Check the column name exists,
    long columnid = p.getLongProperty(column_name + ".id", -1);
    if (columnid == -1) {
      throw new RuntimeException("Column " + column_name + " not found");
    }
    // Check if index property set,
    if (p.getBooleanProperty(column_name + ".index", false)) {
      throw new RuntimeException("Index already on column " + column_name);
    }
    // Check the collator encoded string,
    if (collator_locale != null) {
      getAndCheckLocale(collator_locale);
    }

    // Add to the column list,
    String column_list = p.getProperty("index_column_list", "");
    column_list = addToColumnSet(column_name, column_list);
    p.setProperty("index_column_list", column_list);
    // Set the index property,
    p.setBooleanProperty(column_name + ".index", true);
    if (collator_locale != null) {
      p.setProperty(column_name + ".collator", collator_locale);
    }

    // Build the index,
    IndexObjectCollator index_collator =
                                   getIndexCollatorFor(column_name, columnid);
    buildIndex(columnid, index_collator);

    cached_index_list = null;
    // Add this event to the transaction log,
    addTransactionEvent("addIndex", column_name);
    ++current_version;
  }

  /**
   * Adds an index of the given column in the table with a lexicographical
   * ordering.
   */
  public void addIndex(String column_name) {
    addIndex(column_name, null);
  }

  /**
   * Removes a column index from the table.
   */
  public void removeIndex(String column_name) {
    checkColumnNameValid(column_name);

    PropertySet p = getTableProperties();
    // Check the column name index property,
    boolean is_indexed = p.getBooleanProperty(column_name + ".index", false);
    if (!is_indexed) {
      throw new RuntimeException("Column " + column_name + " not indexed");
    }
    long columnid = p.getLongProperty(column_name + ".id", -1);
    if (columnid == -1) {
      // For this error to occur here would indicate some sort of data model
      // corruption.
      throw new RuntimeException("Column " + column_name + " not found");
    }
    // Remove from the index column list
    String column_list = p.getProperty("index_column_list", "");
    column_list = removeFromColumnSet(column_name, column_list);
    p.setProperty("index_column_list", column_list);
    // Remove the index property,
    p.setProperty(column_name + ".index", null);
    p.setProperty(column_name + ".collator", null);
    // Delete the index file,
    DataFile index_file = getDataFile(getIndexIdKey(columnid));
    index_file.delete();

    cached_index_list = null;
    // Add this event to the transaction log,
    addTransactionEvent("removeIndex", column_name);
    ++current_version;
  }

  /**
   * Returns an iterator of all rows in the table. The last row added/updated
   * in the table is the last item in the iterator. The iterator is immutable.
   * <p>
   * Note that if a change happens to the table while an iterator is being
   * used, the behavior of the iterator is not defined.
   */
  @Override
  public RowCursor iterator() {
    DataFile df = getDataFile(row_index_key);
    OrderedList64Bit list = new OrderedList64Bit(df);
    return new RowCursor(this, current_version, list.iterator());
  }

  /**
   * Returns a reverse iterator of all rows in the table. The last row
   * added/updated in the table is the first item in the iterator. The iterator
   * is immutable.
   * <p>
   * Note that if a change happens to the table while an iterator is being
   * used, the behavior of the iterator is not defined.
   */
  public RowCursor reverseIterator() {
    DataFile df = getDataFile(row_index_key);
    OrderedList64Bit list = new OrderedList64Bit(df);
    return new RowCursor(this, current_version,
                           new SDBIndex.ReverseIterator64Bit(list.iterator()));
  }

  /**
   * Returns an SDBIndex of the index on the given column.
   * <p>
   * Note that if a change happens to the table while the SDBIndex is being
   * used, the behavior of the index is not defined.
   */
  public SDBIndex getIndex(String column_name) {
    checkColumnNameValid(column_name);
    PropertySet p = getTableProperties();
    long column_id = p.getLongProperty(column_name + ".id", -1);
    if (column_id == -1) {
      throw new RuntimeException("Column '" + column_name + "' not found");
    }
    boolean has_index = p.getBooleanProperty(column_name + ".index", false);
    if (!has_index) {
      throw new RuntimeException("Column '" + column_name + "' is not indexed");
    }
    // Fetch the index object,
    DataFile df = getDataFile(getIndexIdKey(column_id));
    OrderedList64Bit list = new OrderedList64Bit(df);
    // And return it,
    IndexObjectCollator index_collator =
                                  getIndexCollatorFor(column_name, column_id);
    return new SDBIndex(this, current_version,
                        index_collator, column_id, list);
  }

  // ----- Updating/Inserting

//  /**
//   * Deletes all data from the table.
//   */
//  public void deleteAll() {
//    // Add this event to the transaction log,
//    addTransactionEvent("deleteAll", table_id, -1);
//    ++current_version;
//  }

  /**
   * Deletes the given row in the table, updating all indexes as is
   * appropriate.
   */
  public void delete(SDBRow row) {
    long rowid = row.getRowIdValue();
    DataFile df = getDataFile(row_index_key);
    OrderedList64Bit row_set = new OrderedList64Bit(df);
    if (!row_set.containsSortKey(rowid)) {
      throw new RuntimeException("Row being deleted is not in the table");
    }
    // Remove the row from the main index,
    removeRowFromRowSet(rowid);
    // Remove the row from any indexes defined on the table,
    removeRowFromIndexSet(rowid);
    // Delete the row file
    DataFile row_df = getDataFile(getRowIdKey(rowid));
    row_df.delete();

    // Add this event to the transaction log,
    addTransactionEvent("deleteRow", rowid);
    ++current_version;
  }

  /**
   * Creates an empty row in the row buffer ready to be populated and inserted
   * into the table. To insert a new row in a table, a call is made to this
   * method, then 'setValue' is used to modify the row buffer content, and
   * then 'complete' is called to finish the insert procedure.
   */
  public void insert() {
    if (row_buffer_id != 0) {
      throw new RuntimeException(
                       "State error: previous table operation not completed");
    }
    if (row_buffer == null) {
      row_buffer = new HashMap();
    }
    row_buffer_id = -1;
  }

  /**
   * Copies the given row object to the row update buffer ready to be updated.
   * To update a row in a table, a call is made to this method, then 'setValue'
   * is used to modify the row buffer content, and then 'complete' is called to
   * finish the update procedure.
   */
  public void update(SDBRow row) {
    if (row_buffer_id != 0) {
      throw new RuntimeException(
                       "State error: previous table operation not completed");
    }
    // Check row is currently indexed,
    long rowid = row.getRowIdValue();
    DataFile df = getDataFile(row_index_key);
    OrderedList64Bit row_set = new OrderedList64Bit(df);
    if (!row_set.containsSortKey(rowid)) {
      throw new RuntimeException("Row being updated is not in the table");
    }
    if (row_buffer == null) {
      row_buffer = new HashMap();
    }
    row_buffer_id = rowid;
    // Copy from the existing data in the row,
    String[] cols = getColumnList();
    for (String col : cols) {
      String val = row.getValue(col);
      if (val != null) {
        row_buffer.put(col, val);
      }
    }
  }

  /**
   * Changes the value of a column of the insert/update row buffer.
   */
  public void setValue(String column, String value) {
    if (row_buffer_id == 0) {
      throw new RuntimeException("State error: not in insert or update state");
    }
    if (value != null) {
      row_buffer.put(column, value);
    }
    else {
      row_buffer.remove(column);
    }
  }

  /**
   * Called after an insert or update operation, to complete the modification
   * to the table. All table indexes will be changed appropriately by this
   * method.
   */
  public void complete() {
    if (row_buffer_id == 0) {
      throw new RuntimeException("State error: not in insert or update state");
    }

    // Create a new rowid
    long rowid = generateId();
    DataFile df = getDataFile(getRowIdKey(rowid));
    // Build the row,
    RowBuilder builder = new RowBuilder(df);
    Set<String> cols = row_buffer.keySet();
    for (String col : cols) {
      builder.setValue(getColumnId(col), row_buffer.get(col));
    }
    // If the operation is insert or update,
    if (row_buffer_id == -1) {
      // Insert,
      // Update the indexes
      addRowToRowSet(rowid);
      addRowToIndexSet(rowid);
      // Add this event to the transaction log,
      addTransactionEvent("insertRow", rowid);
      ++current_version;
    }
    else {
      // Update,
      // Update the indexes
      removeRowFromRowSet(row_buffer_id);
      removeRowFromIndexSet(row_buffer_id);
      addRowToRowSet(rowid);
      addRowToIndexSet(rowid);
      // Add this event to the transaction log,
      addTransactionEvent("deleteRow", row_buffer_id);
      addTransactionEvent("insertRow", rowid);
      DataFile row_df = getDataFile(getRowIdKey(row_buffer_id));
      row_df.delete();
      ++current_version;
    }

    // Clear the row buffer, etc
    row_buffer_id = 0;
    row_buffer.clear();
  }


  /**
   * Outputs some information about the table to System.out,
   * used for debugging.
   */
  public void infoDump(PrintWriter out) {
    out.println(getTableProperties());
//    System.out.println("add:    " + add_row_list);
//    System.out.println("delete: " + delete_row_list);
//    System.out.println("structural_modification = " + structural_modification);
  }

  // ----- Inner classes

  /**
   * A wrapper on a DataFile that enables the building and querying of row
   * objects. The DataFile is formatted as a series of columnid values mapped
   * to an offset pointer and length of where the UTF8 encoded form of the
   * string is.
   */
  private static class RowBuilder {

    private final DataFile file;

    RowBuilder(DataFile file) {
      this.file = file;
    }

    /**
     * Returns the value assigned to the given columnid. If the value isn't
     * set for the column, returns null.
     */
    String getValue(long columnid) {
      file.position(0);
      DataInputStream din = DataFileUtils.asDataInputStream(file);

      try {
        // If no size, return null
        int size = Math.min((int) file.size(), Integer.MAX_VALUE);
        if (size == 0) {
          return null;
        }

        // The number of columns stored in this row,
        int hsize = din.readInt();

        for (int i = 0; i < hsize; ++i) {
          long sid = din.readLong();
          int coffset = din.readInt();
          if (sid == columnid) {
            file.position(4 + (hsize * 12) + coffset);
            din = DataFileUtils.asDataInputStream(file);
            byte t = din.readByte();
            // Types (currently only supports string types (UTF8 encoded)).
            if (t == 1) {
              // Read the UTF value and return
              return din.readUTF();
            }
            else {
              throw new RuntimeException("Unknown cell type: " + t);
            }
          }
        }
        // Otherwise not found, return null.
        return null;
      }
      catch (IOException e) {
        // Wrap IOException around a runtime exception
        throw new RuntimeException(e);
      }
    }

    /**
     * Sets the value of a column. Generates an error if the column value is
     * already set.
     */
    void setValue(long columnid, String value) {
      file.position(0);
      DataInputStream din = DataFileUtils.asDataInputStream(file);

      try {
        int size = Math.min((int) file.size(), Integer.MAX_VALUE);
        // If file is not empty,
        if (size != 0) {
          // Check if the columnid already set,
          int hsize = din.readInt();

          for (int i = 0; i < hsize; ++i) {
            long sid = din.readLong();
            int coffset = din.readInt();
            if (sid == columnid) {
              // Yes, so generate error,
              throw new RuntimeException("Column value already set.");
            }
          }
        }

        // Ok to add column,
        file.position(0);
        if (size == 0) {
          file.putInt(1);
          file.putLong(columnid);
          file.putInt(0);
        }
        else {
          int count = file.getInt();
          ++count;
          file.position(0);
          file.putInt(count);
          file.shift(12);
          file.putLong(columnid);
          file.putInt((int) (file.size() - (count * 12) - 4));
          file.position(file.size());
        }
        // Write the string
        DataOutputStream dout = DataFileUtils.asDataOutputStream(file);
        dout.writeByte(1);
        dout.writeUTF(value);
        dout.flush();
        dout.close();

      }
      catch (IOException e) {
        // Wrap IOException around a runtime exception
        throw new RuntimeException(e);
      }
    }

  }

  /**
   * A lexicographical string collator for an index of a column of this table.
   * Uses the Java String.compareTo method for the ordering method.
   */
  private class SDBLexiStringCollator implements IndexObjectCollator {

    private final long columnid;

    public SDBLexiStringCollator(long columnid) {
      this.columnid = columnid;
    }

    @Override
    public int compare(long ref, Object val) {
      // Nulls are ordered at the beginning
      String v = getCellValue(ref, columnid);
      if (val == null && v == null) {
        return 0;
      }
      else if (val == null) {
        return 1;
      }
      else if (v == null) {
        return -1;
      }
      else {
        return v.compareTo((String) val);
      }
    }

  }

  /**
   * A Locale specific string collator for an index of a column of this table.
   * Uses the java.text.Collator class to perform the locale specific
   * ordering.
   */
  private class SDBLocaleStringCollator implements IndexObjectCollator {

    private final long columnid;
    private final Collator collator;

    public SDBLocaleStringCollator(long columnid, Locale locale) {
      this.columnid = columnid;
      this.collator = Collator.getInstance(locale);
    }

    @Override
    public int compare(long ref, Object val) {
      // Nulls are ordered at the beginning
      String v = getCellValue(ref, columnid);
      if (val == null && v == null) {
        return 0;
      }
      else if (val == null) {
        return 1;
      }
      else if (v == null) {
        return -1;
      }
      else {
        return collator.compare(v, (String) val);
      }
    }

  }

}
