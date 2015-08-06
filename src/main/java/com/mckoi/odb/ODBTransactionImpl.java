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

import com.mckoi.data.*;
import com.mckoi.data.KeyObjectTransaction.Scope;
import com.mckoi.network.CommitFaultException;
import com.mckoi.network.DataAddress;
import com.mckoi.network.MckoiDDBAccess;
import com.mckoi.util.ByteArrayUtil;
import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.MessageFormat;
import java.util.*;

/**
 * ODBTransaction implementation.
 *
 * @author Tobias Downer
 */

class ODBTransactionImpl implements ODBTransaction {

    /**
   * The session object this transaction is part of.
   */
  private final ODBSession session;

  /**
   * The base root DataAddress of this transaction. This is the base root as
   * returned when the transaction was originally created. Used when building
   * the commit log.
   */
  private final DataAddress base_root;

  /**
   * The KeyObjectTransaction object.
   */
  private final KeyObjectTransaction transaction;

  /**
   * The log of storage operations during this transaction.
   */
  private ObjectLog log;

  /**
   * If true, the transaction does not permit changes or commits.
   */
  private final boolean read_only;

  /**
   * Set to true when this transaction object is invalidated.
   */
  private boolean invalidated = false;

  /**
   * Local cache of bucket lists.
   */
  private HashMap<Key, OrderedSetData> bucket_cache = new HashMap();

  /**
   * Local class cache.
   */
  private HashMap<Reference, ODBClass> class_cache = new HashMap();

//  private HashMap<Reference, ODBObject> TEST_cache = new HashMap();

  /**
   * Cached class reference map set.
   */
  private OrderedSetData cached_class_reference_map;
  

  /**
   * The Key that contains static information about the data model, such as
   * the magic value, etc.
   */
  static final Key MAGIC_KEY = new Key((short) 0, 0, 0);


//  static final Key BUCKET_PROPERTIES_KEY = new Key((short) 0, 1, 30);
//  static final Key RESOURCE_PROPERTIES_KEY = new Key((short) 0, 1, 31);
  static final Key CLASS_REFERENCE_MAP_KEY = new Key((short) 0, 1, 32);
  static final Key RESOURCE_LOOKUP_KEY = new Key((short) 0, 1, 33);
  static final Key RESOURCE_COUNTER_KEY = new Key((short) 0, 1, 34);

  /**
   * System classes key object.
   */
  static final Key SYS_CLASS_BUCKET = new Key((short) 0, 1, 36);
  static final Key SYS_CLASSES_LIST = new Key((short) 0, 1, 37);
  static final Key SYS_NAMER_BUCKET = new Key((short) 0, 1, 38);
  static final Key SYS_NAMER_LIST = new Key((short) 0, 1, 39);

  /**
   * The resource key for the system class.
   */
  static final Reference SYS_CLASS_REFERENCE = new Reference(0, 5);

  /**
   * The resource key for the named item database.
   */
  static final Reference SYS_NAMER_REFERENCE = new Reference(0, 6);

  /**
   * The Keys that contains the log information for this transaction.
   */
  static final Key TRANSACTION_LOG_KEY = new Key((short) 0, 1, 11);
  static final Key TRANSACTION_PROPERTIES_KEY = new Key((short) 0, 1, 12);


  /**
   * Constructor.
   */
  ODBTransactionImpl(ODBSession session,
                     DataAddress base_root, KeyObjectTransaction transaction,
                     boolean read_only) {

    this.session = session;
    this.base_root = base_root;
    this.transaction = transaction;
    this.read_only = read_only;

  }

  /**
   * Returns read or write mode depending on the 'read_only' flag.
   */
  private char getReadWriteMode() {
    return read_only ? 'r' : 'w';
  }

  /**
   * Returns an OrderedReferenceList that is the currently stored class
   * list.
   */
  OrderedReferenceList getSystemClassList() {

    char rw_mode = getReadWriteMode();
    DataFile data = transaction.getDataFile(SYS_CLASSES_LIST, rw_mode);

    SimpleClass sys_class_ob = new SimpleClass("SYS CLASS LIST",
                                      "#SYS CLASS LIST", SYS_CLASS_REFERENCE);
    OrderedReferenceList orl = new OrderedReferenceList(
                                 this, SYS_CLASS_REFERENCE,
                                 sys_class_ob, data, SYS_CLASSES_ORDER_SPEC);
    return orl;
  }

  /**
   * Returns an OrderedReferenceList that is the currently stored named item
   * list.
   */
  OrderedReferenceList getNamedItemList() {

    char rw_mode = getReadWriteMode();
    DataFile data = transaction.getDataFile(SYS_NAMER_LIST, rw_mode);

    SimpleClass sys_class_ob = new SimpleClass("SYS NAMED ITEM LIST",
                                 "#SYS NAMED ITEM LIST", SYS_NAMER_REFERENCE);
    OrderedReferenceList orl = new OrderedReferenceList(
                                 this, SYS_NAMER_REFERENCE,
                                 sys_class_ob, data, SYS_NAMER_ORDER_SPEC);
    return orl;
  }

  /**
   * Returns the list log for this transaction.
   */
  ObjectLog getObjectLog() {
    if (log == null) {
      log = new ObjectLog(transaction, false, getReadWriteMode());
    }
    return log;
  }

  /**
   * Returns the compiled list log in a transaction being proposed in a commit.
   */
  ObjectLog getProposedObjectLog() {
    if (log == null) {
      log = new ObjectLog(transaction, true, getReadWriteMode());
    }
    return log;
  }


  /**
   * Returns an OrderedSetData object representing the class reference map.
   */
  private OrderedSetData getClassReferenceMap() {
    OrderedSetData map = cached_class_reference_map;
    if (map == null) {
      DataFile cr_map =
          transaction.getDataFile(CLASS_REFERENCE_MAP_KEY, getReadWriteMode());
      map = new OrderedSetData(cr_map, dictionary_collator);
      map.useSectionsCache(true);
      cached_class_reference_map = map;
    }
    return map;
  }
  
  /**
   * Looks up a class name is the system class dictionary and returns false
   * if the name is not defined.
   */
  boolean hasClassDirectoryEntry(String class_str) {

    OrderedSetData map = getClassReferenceMap();
//    DataFile cr_map =
//          transaction.getDataFile(CLASS_REFERENCE_MAP_KEY, getReadWriteMode());
//    OrderedSetData map = new OrderedSetData(cr_map, dictionary_collator);

    try {

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      DataOutputStream dout = new DataOutputStream(bout);
      dout.writeByte(0x001);
      dout.writeUTF(class_str);
      dout.writeLong(0);
      dout.writeLong(0);
      dout.flush();

      JavaByteArray barr = new JavaByteArray(bout.toByteArray());

      map = map.tailSet(barr);
      if (!map.isEmpty()) {
        ByteArray read_val = map.first();

        // Is the first of the tail set the class we are looking for?
        DataInputStream din = read_val.getDataInputStream();
        if (din.readByte() == 0x001) {
          String str = din.readUTF();
          if (str.equals(class_str)) {
            return true;
          }
        }

      }

      // Not found,
      return false;

    }
    catch (IOException e) {
      // Shouldn't be possible,
      throw new RuntimeException(e);
    }

  }

  /**
   * Looks up a class name in the system class dictionary and returns the
   * Reference object for it.
   */
  Reference getReferenceFromClassDictionary(String class_str) {

    OrderedSetData map = getClassReferenceMap();
//    DataFile cr_map =
//          transaction.getDataFile(CLASS_REFERENCE_MAP_KEY, getReadWriteMode());
//    OrderedSetData map = new OrderedSetData(cr_map, dictionary_collator);

    try {

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      DataOutputStream dout = new DataOutputStream(bout);
      dout.writeByte(0x001);
      dout.writeUTF(class_str);
      dout.writeLong(0);
      dout.writeLong(0);
      dout.flush();

      JavaByteArray barr = new JavaByteArray(bout.toByteArray());

      map = map.tailSet(barr);
      if (!map.isEmpty()) {
        ByteArray read_val = map.first();
        // Is the first of the tail set equal?
        DataInputStream din = read_val.getDataInputStream();
        if (din.readByte() == 0x001) {
          String str = din.readUTF();
          if (str.equals(class_str)) {
            long ref_hval = din.readLong();
            long ref_lval = din.readLong();
            return new Reference(ref_hval, ref_lval);
          }
        }
      }

      // Not found,
      throw new RuntimeException(MessageFormat.format(
                         "Class ''{0}'' not found in dictionary", class_str));

    }
    catch (IOException e) {
      // Shouldn't be possible,
      throw new RuntimeException(e);
    }

  }

  /**
   * Looks up a Reference in the system class dictionary and returns the
   * class name for it.
   */
  String getTypeStringFromClassDictionary(Reference ref) {

    OrderedSetData map = getClassReferenceMap();
//    DataFile cr_map =
//          transaction.getDataFile(CLASS_REFERENCE_MAP_KEY, getReadWriteMode());
//    OrderedSetData map = new OrderedSetData(cr_map, dictionary_collator);

    try {

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      DataOutputStream dout = new DataOutputStream(bout);
      dout.writeByte(0x002);
      dout.writeLong(ref.getHighLong());
      dout.writeLong(ref.getLowLong());
      dout.writeUTF("");
      dout.flush();

      JavaByteArray barr = new JavaByteArray(bout.toByteArray());

      map = map.tailSet(barr);
      if (!map.isEmpty()) {
        ByteArray read_val = map.first();
        // Is the first of the tail set equal?
        DataInputStream din = read_val.getDataInputStream();
        if (din.readByte() == 0x002) {
          long rr_high = din.readLong();
          long rr_low = din.readLong();
          Reference rr = new Reference(rr_high, rr_low);
          if (rr.equals(ref)) {
            return din.readUTF();
          }
        }
      }

      // Not found,
      throw new RuntimeException(MessageFormat.format(
                         "Reference ''{0}'' not found in dictionary", ref));

    }
    catch (IOException e) {
      // Shouldn't be possible,
      throw new RuntimeException(e);
    }

  }

  /**
   * Adds an entry to the class/reference dictionary.
   */
  void addToClassDictionary(String type_str, Reference ref) {

    OrderedSetData map = getClassReferenceMap();
//    DataFile cr_map =
//          transaction.getDataFile(CLASS_REFERENCE_MAP_KEY, getReadWriteMode());
//    OrderedSetData map = new OrderedSetData(cr_map, dictionary_collator);

    try {

      // Create two map items, one that is the type -> ref tuple, the other
      // which is the ref -> type tuple.

      ByteArrayOutputStream mapval = new ByteArrayOutputStream();
      DataOutputStream dout = new DataOutputStream(mapval);
      dout.writeByte(0x001);
      dout.writeUTF(type_str);
      dout.writeLong(ref.getHighLong());
      dout.writeLong(ref.getLowLong());
      dout.flush();
      JavaByteArray mval1_arr = new JavaByteArray(mapval.toByteArray());

      mapval = new ByteArrayOutputStream();
      dout = new DataOutputStream(mapval);
      dout.writeByte(0x002);
      dout.writeLong(ref.getHighLong());
      dout.writeLong(ref.getLowLong());
      dout.writeUTF(type_str);
      dout.flush();
      JavaByteArray mval2_arr = new JavaByteArray(mapval.toByteArray());

      // Adds the values to the map,

//      long sz;
//      cr_map.position(0);
//      sz = cr_map.size();
//      for (int i = 0; i < sz; ++i) {
//        byte b = cr_map.get();
//        System.out.print(Integer.toHexString(((int) b) & 0x0FF));
//        System.out.print(", ");
//      }
//      System.out.println();

      map.add(mval1_arr);
      map.add(mval2_arr);

      // Log that we made this map,
      getObjectLog().logDictionaryAddition(ref);

    }
    catch (IOException e) {
      // Shouldn't be possible,
      throw new RuntimeException(e);
    }

  }



  /**
   * Returns the path name of the session.
   */
  @Override
  public String getSessionPathName() {
    return session.getPathName();
  }

  /**
   * Returns an ODBClassCreator for creating a class schema in this
   * transaction. The classes created with a creator object must first be
   * validated before they may be accessed within the transaction. As with all
   * data operations, the transaction must be committed before changes are
   * made permanent.
   */
  @Override
  public ODBClassCreator getClassCreator() {
    return new ODBClassCreator(this);
  }




  /**
   * Returns the class instance currently defined with the given name, or null
   * if no class currently defined with that name.
   */
  @Override
  public ODBClass findClass(String class_name) {
    // Query the system class list and get the class with the given name,
    Reference class_ref = getSystemClassList().get(class_name);
    if (class_ref == null) {
      return null;
    }
    // Return it as a class object,
    return getClass(class_ref);
  }

  /**
   * Given an encoded list string (eg.
   * '[L&lt;Person#423232&gt;(unique,name,lexi)') returns an ODBClass object
   * for the list class with a reference id. Note that the reference of the
   * returned class is unique however the system makes no guarantees that there
   * are not multiple lists of the same type with different references. The
   * reference is stored in a dictionary and it is possible for there to be
   * multiple dictionary entries with the same key.
   */
  private ODBClass getListClass(String type_str) {
    Reference ref = getReferenceFromClassDictionary(type_str);
    SimpleClass list_class = new SimpleClass("User List", type_str, ref);
    list_class.setImmutable();
    return list_class;
  }

  /**
   * Internal method that defines an object (with a unique reference) and
   * returns a materialization of the object. Note that the given arguments
   * may be only either a java.lang.String, a com.mckoi.odb.Reference or null.
   */
  private ODBObject constructObject(
                           ODBClass clazz, Reference obj_ref, Object... args) {

    // Records the external resource constructions,
    ArrayList external_data_alloc = new ArrayList(4);
    ArrayList external_list_alloc = new ArrayList(4);

    Object[] content_copy = args.clone();
    // Convert any ODBObject objects to Reference,
    for (int i = 0; i < content_copy.length; ++i) {
      if (content_copy[i] instanceof ODBObject) {
        content_copy[i] = ((ODBObject) content_copy[i]).getReference();
      }
    }


    // Check arg count,
    int field_count = clazz.getFieldCount();
    if (content_copy.length != field_count) {
      throw new RuntimeException(MessageFormat.format(
             "Argument count does not match field count in class {0}", clazz));
    }
    // Make a list of external resources we need to create for this class,
    // Check types,
    for (int i = 0; i < field_count; ++i) {
      String field_type = clazz.getFieldType(i);
      if (field_type.startsWith("[D")) {
        // If it's a data type,
        Reference data_resource_ref = createUniqueReference(null);
        content_copy[i] = data_resource_ref;
        // Note this resource construction in the log,
        external_data_alloc.add(data_resource_ref);
      }
      else if (field_type.startsWith("[L")) {
        // If it's a list type,
        Reference list_resource_ref = createUniqueReference(null);
        content_copy[i] = list_resource_ref;
        // Note this resource construction in the log,
        external_list_alloc.add(list_resource_ref);
      }
      else {
        checkType(content_copy[i], field_type);
      }
    }

    // We can make various decisions here about how and where we are going to
    // store objects and how they are referenced. For example, an object with
    // no inline strings can be stored in a fixed size bucket.

    final OrderedSetData bucket_set = getBucketSetData(clazz);

    try {
      // A serialization of the object as a byte array,
      ByteArray object_ser = createObjectSer(obj_ref, clazz, content_copy);

      // Write it to the bucket,
      boolean success = bucket_set.add(object_ser);
      if (!success) {
        // Oops, key clash! This should never happen but there's always the
        // remote possibility that it could.
        throw new RuntimeException("UID clash");
      }

      // Go through any external resource allocations if there are any,
      int sz = external_data_alloc.size();
      if (sz > 0) {
        for (int i = 0; i < sz; ++i) {
          Reference data_ref = (Reference) external_data_alloc.get(i);
          Key data_key = constructEmptyData(data_ref);
        }
      }
      sz = external_list_alloc.size();
      if (sz > 0) {
        for (int i = 0; i < sz; ++i) {
          Reference list_ref = (Reference) external_list_alloc.get(i);
          Key list_key = constructEmptyList(list_ref);
        }
      }

      // Update the log,
      getObjectLog().logObjectChange(clazz.getReference(), obj_ref);

      // Return the object
      return new ODBTransactionImpl.MaterializedODBObject(
                                                 clazz, obj_ref, content_copy);

    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  /**
   * Defines an object and returns the object. An object must resolve
   * against the given class type. Note that the given arguments may be
   * only either a java.lang.String, a com.mckoi.odb.Reference or null.
   */
  @Override
  public ODBObject constructObject(ODBClass clazz, Object... args) {

    // Create a unique 128-bit reference id for this object,
    Reference ref = createUniqueReference(clazz);
    return constructObject(clazz, ref, args);

  }

  /**
   * Returns a class from a reference. The returned class is immutable.
   * <p>
   * Throws NoSuchReferenceException if the reference is invalid.
   */
  @Override
  public ODBClass getClass(Reference ref) {

    ODBClass odb_class = class_cache.get(ref);
    if (odb_class == null) {

      // The class reference,
      ODBObject class_ob = getObject(ODBClasses.CLASS, ref);

      // Decode,
      String class_name = class_ob.getString(0);
      String serialization = class_ob.getString(1);

      odb_class = new ODBTransactionImpl.MaterializedODBClass(
                                               ref, class_name, serialization);
      class_cache.put(ref, odb_class);

    }
    return odb_class;
  }



  /**
   * Returns an ODBObject from a reference. Returns null if the reference is
   * to a null object. The returned object is selectively mutable and changes
   * made are published on commit. Fields defined as immutable can not be
   * changed.
   * <p>
   * Throws NoSuchReferenceException if the reference is invalid.
   */
  @Override
  public ODBObject getObject(ODBClass type, Reference ref) {

    ODBObject ob;

    // See if it's in the cache,
    GeneralCacheKey gen_key = new ReferenceCacheKey(1, ref);
    GeneralCache gen_cache = transaction.getGeneralCache(Scope.TRANSACTION);
    Object[] content = (Object[]) gen_cache.get(gen_key);

    if (content == null) {

      // Get the bucket set for system class definitions,
      OrderedSetData class_bucket_set = getBucketSetData(type);

      try {

        // The key being searched for,
        ByteArray search_key = createSearchKey(ref);
        // The tail set from the search key
        OrderedSetData found_set = class_bucket_set.tailSet(search_key);
        // If the tail set is empty or the found entry doesn't match, generate
        // an error.
        if (found_set.isEmpty()) {
          throw new RuntimeException(
                    MessageFormat.format("Class for ref {0} not found", ref));
        }
        ByteArray class_ser = found_set.first();
        DataInputStream din = class_ser.getDataInputStream();

        // Check the ref of the found record matches,
        long rh = din.readLong();
        long rl = din.readLong();

        if (rh != ref.getHighLong() || rl != ref.getLowLong()) {
          throw new RuntimeException(
                    MessageFormat.format("Class for ref {0} not found", ref));
        }

        // Content of the object,
        content = new Object[type.getFieldCount()];

        // Decode the body,
        try {

          int i = 0;
          while (true) {
            byte val_type = din.readByte();
            if (val_type == 0) {
              // NULL
              content[i] = null;
            }
            else if (val_type == 1) {
              // Inline string
              String str_item = din.readUTF();
              content[i] = str_item;
            }
            else if (val_type == 2) {
              // Reference
              long ref_h = din.readLong();
              long ref_l = din.readLong();
              Reference ref_item = new Reference(ref_h, ref_l);
              content[i] = ref_item;
            }
//            else if (val_type == 3) {
//              // Resource key
//              long ref_h = din.readLong();
//              long ref_l = din.readLong();
//              ResourceKey ref_item = new ResourceKey(ref_h, ref_l);
//              content[i] = ref_item;
//            }
            else {
              throw new RuntimeException("Object entry type error");
            }
            ++i;
          }
        }
        catch (EOFException e) {
          // Finished,
        }

      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }

      // Put it in the cache,
      gen_cache.put(gen_key, content);

    }

    // Return an instance of the object,
    ob = new ODBTransactionImpl.MaterializedODBObject(type, ref, content);
    
    // Note that anyone who changes the object within this transaction will
    // change the cached 'content' object also. This is intentional. This
    // means you will always see the correct content of the object.
    
    return ob;

  }

  /**
   * Adds a named item to the database. A named item is a 'window' or starting
   * point of the object graph.
   */
  @Override
  public void addNamedItem(String name, ODBObject item) {

    // Null check,
    if (name == null) throw new NullPointerException("name");

    Reference named_item_class_ref = item.getODBClass().getReference();
    Reference named_item_ref = item.getReference();

    // Serialize the item,
    Reference item_ref = this.createUniqueReference(ODBClasses.NAMER);
    ODBObject item_obj = constructObject(
            ODBClasses.NAMER, item_ref,
            name, named_item_class_ref.toString(), named_item_ref.toString());

    // Assert the references are equal,
    assert(item_ref == item_obj.getReference());

    try {

      // Insert it into the namer list,
      getNamedItemList().insert(item_ref);

    }
    catch (ConstraintViolationException e) {
      // If we get a constraint violation, delete the class reference

      // Safe to remove this since we just created it and the reference has
      // been kept in scope.
      unsafeDelete(item_ref);

      throw new RuntimeException(MessageFormat.format(
                           "Named item ''{0}'' is already defined", name), e);
    }

  }

  /**
   * Removes a named item from the database. If the named item isn't defined
   * then returns false, otherwise returns true if the named item was deleted.
   */
  @Override
  public boolean removeNamedItem(String name) {

    return getNamedItemList().remove(name);

  }

  /**
   * Returns a named item from the database, or null if the named item isn't
   * defined.
   */
  @Override
  public ODBObject getNamedItem(String name) {

    // Query the system named item list,
    Reference item_ref = getNamedItemList().get(name);
    if (item_ref == null) {
      return null;
    }
    // The object
    ODBObject item_ob = getObject(ODBClasses.NAMER, item_ref);

    // Decode the class and object from the named item list,
    Reference class_ref = Reference.fromString(item_ob.getString(1));
    Reference ob_ref = Reference.fromString(item_ob.getString(2));

    // Return the object,
    return getObject(getClass(class_ref), ob_ref);

  }

  /**
   * Scans the object graph and creates a set of reachable objects, and then
   * finds the difference between the entire set of references and the set of
   * reachable objects. References that are not in the graph are deleted.
   * <p>
   * This process may take time to complete (it is recommended be done in an
   * offline process). A garbage collection operation will never cause a fault
   * on commit.
   * <p>
   * The contract does not guarantee all unreachable objects will be deleted
   * by this operation since the administrator may define policy on the maximum
   * amount of time a garbage collection cycle can take. This operation should
   * operate incrementally if desired.
   */
  @Override
  public void doGarbageCollection() {
    throw new UnsupportedOperationException("TODO");
  }

  /**
   * Returns the set of all named items as a List of Java String objects.
   * Changes to the list of named items is immediately reflected in the
   * returned object, however the returned list itself is immutable.
   */
  @Override
  public List<String> getNamedItemsList() {
    return new ODBTransactionImpl.FirstFieldObjectList(ODBClasses.NAMER, getNamedItemList());
  }

  /**
   * Returns the set of all class names as a List of Java String objects.
   * Changes to the classes defined is immediately reflected in the
   * returned object, however the returned list itself is immutable.
   */
  @Override
  public List<String> getClassNamesList() {
    return new ODBTransactionImpl.FirstFieldObjectList(
                                       ODBClasses.CLASS, getSystemClassList());
  }


  /**
   * Commits any changes made in this transaction.
   */
  @Override
  public ODBRootAddress commit() throws CommitFaultException {

    // Read only check,
    if (read_only) throw new UnsupportedOperationException();

    // We prepare the transaction data log for the consensus function as
    // follows;
    // 1) Any lists that have changed are noted,


//    System.out.println("Committing...");
//    this.getObjectLog().printDebug();
//    System.out.println();


    // Flush the compiled object log out with the base_root,
    ObjectLog object_log = getObjectLog();
    object_log.flush(base_root);

    // The database client,
    MckoiDDBAccess db_client = session.getDatabaseClient();

    // Flush the transaction to the network
    DataAddress proposal = db_client.flushTransaction(transaction);
    // Perform the commit operation,
    return new ODBRootAddress(session,
                db_client.performCommit(session.getPathName(), proposal));

  }

  // -----

  /**
   * Updates the lookup table status with the max key.
   */
  void updateResourceLookupMaxKey(Key max_key) {

    DataFile counter =
            transaction.getDataFile(RESOURCE_COUNTER_KEY, getReadWriteMode());

    // Update the counter file,
    counter.position(0);
    counter.putLong(max_key.getSecondary() - 10);
    counter.putLong(max_key.getPrimary());

  }

  /**
   * Returns the current maximum key in the lookup table.
   */
  Key getResourceLookupMaxKey() {
    
    DataFile counter =
            transaction.getDataFile(RESOURCE_COUNTER_KEY, getReadWriteMode());

    counter.position(0);
    long val1 = counter.getLong();
    long val2 = counter.getLong();

    // Turn it into a key,
    return new Key((short) 0, (int) (10 + val1), val2);

  }

  /**
   * Copies a dictionary addition to the destination transaction.
   */
  void copyDictionaryAdditionTo(ODBTransactionImpl dst_transaction,
                                DictionaryEvent evt) {

    Reference dictionary_ref = evt.getDictionaryReference();

    String type_str = getTypeStringFromClassDictionary(dictionary_ref);
    // Add to the class dictionary.
    // Note that this method also updates the log for the destination
    // transaction.
    dst_transaction.addToClassDictionary(type_str, dictionary_ref);

  }

  /**
   * Copies the resource from this transaction to the given transaction
   * with a new key, and updates the lookup table appropriately. Returns the
   * new key.
   */
  Key copyResourceAsNewKeyTo(ODBTransactionImpl destination,
                             Reference resource_ref, Key resource_key) {

    // Make a new key for this resource,
    Key new_key = destination.allocateKeyForReference(resource_ref);

    // Copy the DataFile content,
    DataFile src_file =
              transaction.getDataFile(resource_key, getReadWriteMode());
    DataFile dst_file =
              destination.transaction.getDataFile(new_key, getReadWriteMode());

    // Assert that the destination is empty,
    if (dst_file.size() != 0) {
      throw new RuntimeException("Destination resource not empty: " + new_key);
    }

    // The copy
    dst_file.replicateFrom(src_file);

    // Update the log in the destination,
    destination.getObjectLog().logAllocateResource(new_key, resource_ref);

    // Return the key,
    return new_key;

  }

  /**
   * Copies the resource from this transaction to the given transaction
   * with the same key, and updates the lookup table appropriately. Returns the
   * key.
   */
  Key copyResourceTo(ODBTransactionImpl destination,
                     Reference resource_ref, Key resource_key) {

    // Copy the DataFile content,
    DataFile src_file =
         transaction.getDataFile(resource_key, getReadWriteMode());
    DataFile dst_file =
         destination.transaction.getDataFile(resource_key, getReadWriteMode());

    // Assert that the destination is empty,
    if (dst_file.size() != 0) {
      throw new RuntimeException(
                            "Destination resource not empty: " + resource_key);
    }

    // The copy
    dst_file.replicateFrom(src_file);

    // Update the lookup table in the destination,
    DataFile lookup_table = destination.transaction.getDataFile(
                                      RESOURCE_LOOKUP_KEY, getReadWriteMode());
    // The structure for recording lookup data,
    ReferenceKeyLookup lookup_sdata = new ReferenceKeyLookup(lookup_table);
    // Put the association in the map,
    lookup_sdata.put(resource_ref, resource_key);

    // Update the log in the destination,
    destination.getObjectLog().logAllocateResource(resource_key, resource_ref);

    // Return the key,
    return resource_key;

  }

  /**
   * Copies the entire list content with the given reference from this
   * transaction to the destination transaction using a fast copy process.
   * In addition, this copies the log entries for the list to the destination
   * transaction.
   */
  void copyListTo(ODBTransactionImpl destination, Reference list_reference,
                  ObjectLog object_log) {

    // The source list data,
    Key src_list_key = dereferenceToList(list_reference);
    DataFile src_list_data =
                     transaction.getDataFile(src_list_key, getReadWriteMode());

    // The destination list data,
    Key dst_list_key = destination.dereferenceToList(list_reference);
    DataFile dst_list_data = destination.transaction.getDataFile(
                                             dst_list_key, getReadWriteMode());

    // Copy the list data,
    dst_list_data.replicateFrom(src_list_data);

    // Update the log in the destination,
    object_log.copyAllListOperationsTo(list_reference,
                                       destination.getObjectLog());

  }

  /**
   * Replays an object change event and updates the log in this transaction
   * as appropriate.
   */
  void replayObjectChange(ODBTransactionImpl from_transaction,
                          ObjectChangeEvent evt) {

    Reference object_class_ref = evt.getClassReference();
    Reference obj = evt.getObjectReference();

    Key bucket_key = dereferenceToBucket(object_class_ref);
    // Turn it into an OrderedSetData object,
    OrderedSetData dst_bucket_set = new OrderedSetData(
               transaction.getDataFile(bucket_key, getReadWriteMode()),
                                                           object_comparator);

    // The source bucket set,
    Key src_bucket_key = from_transaction.dereferenceToBucket(object_class_ref);
    OrderedSetData src_bucket_set = new OrderedSetData(
                from_transaction.transaction.getDataFile(
                      src_bucket_key, getReadWriteMode()), object_comparator);

    try {

      // The key being searched for,
      ByteArray search_key = createSearchKey(obj);
      // The tail set from the search key
      OrderedSetData found_set = src_bucket_set.tailSet(search_key);
      // If the tail set is empty or the found entry doesn't match, generate
      // an error.
      if (found_set.isEmpty()) {
        throw new RuntimeException(
                     MessageFormat.format("Class for ref {0} not found", obj));
      }
      ByteArray class_ser = found_set.first();
      DataInputStream din = class_ser.getDataInputStream();

      // Check the ref of the found record matches,
      long rh = din.readLong();
      long rl = din.readLong();

      if (rh != obj.getHighLong() || rl != obj.getLowLong()) {
        throw new RuntimeException(
                     MessageFormat.format("Class for ref {0} (class {1}) not found", obj, object_class_ref));
      }

      // Put it in the destination,
      dst_bucket_set.replaceOrAdd(class_ser);

    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Update the log,
    getObjectLog().logObjectChange(object_class_ref, obj);

  }

  /**
   * Replays an data change event and updates the log in this transaction
   * as appropriate.
   */
  void replayDataChange(ODBTransactionImpl from_transaction,
                        DataChangeEvent evt) {

    // The data reference,
    Reference data_reference = evt.getDataReference();

    // The source data,
    Key src_list_key = from_transaction.dereferenceToList(data_reference);
    DataFile src_data = from_transaction.transaction.getDataFile(
                                            src_list_key, getReadWriteMode());

    // The destination data,
    Key dst_data_key = dereferenceToList(data_reference);
    DataFile dst_data = transaction.getDataFile(
                                            dst_data_key, getReadWriteMode());

    // Copy the data,
    dst_data.replicateFrom(src_data);

    // Update the log,
    getObjectLog().logDataChange(data_reference);

  }

  /**
   * Replays a list item add event and updates the log in this transaction
   * as appropriate.
   */
  void replayListItemAdd(ODBTransaction from_transaction,
                         Reference list_reference, Reference object_reference,
                         Reference list_class_ref)
                                          throws ConstraintViolationException {

    // Get the list type from the dictionary,
    String list_type_str = getTypeStringFromClassDictionary(list_class_ref);

    // Get the source and destination list object,
    ODBList list_ob = createODBList(list_type_str, list_reference);

    list_ob.add(object_reference);

    // Update the log,
    getObjectLog().logListAddition(list_reference, object_reference, list_class_ref);

  }

  /**
   * Replays a list item remove event and updates the log in this transaction
   * as appropriate.
   */
  void replayListItemRemove(ODBTransaction from_transaction,
                         Reference list_reference, Reference object_reference,
                         Reference list_class_ref)
                                          throws ConstraintViolationException {

    // Get the list type from the dictionary,
    String list_type_str = getTypeStringFromClassDictionary(list_class_ref);

    // Get the source and destination list object,
    ODBList list_ob = createODBList(list_type_str, list_reference);

    boolean removed = list_ob.remove(object_reference);

    // If not removed, then it signifies a constraint violation (entry
    // double removed from the list).
    if (!removed) {
      throw new ConstraintViolationException(
                                "Duplicate remove object from list operation");
    }

    // Update the log,
    getObjectLog().logListRemoval(list_reference, object_reference, list_class_ref);

  }

  // -----

  /**
   * Given a reference, allocates a unique Key object that represents a
   * place in the database key space to store data for the resource. This
   * is achieved through the management of a map that maintains the resource
   * allocation space.
   * <p>
   * Note that it is possible for concurrent clients to allocate the same key.
   * In this case, the consensus function handles the disparity by copying the
   * content to a new key and updating the lookup table.
   */
  Key allocateKeyForReference(Reference ref) {

    DataFile lookup_table =
            transaction.getDataFile(RESOURCE_LOOKUP_KEY, getReadWriteMode());
    DataFile counter =
            transaction.getDataFile(RESOURCE_COUNTER_KEY, getReadWriteMode());

    // The structure for recording lookup data,
    ReferenceKeyLookup lookup_sdata = new ReferenceKeyLookup(lookup_table);

    long val1, val2;
    if (counter.size() == 0) {
      val1 = 0;
      val2 = 0;
    }
    else {
      counter.position(0);
      val1 = counter.getLong();
      val2 = counter.getLong();
    }

    // Add a random amount to the last value and hope we can avoid clashes,
    val2 = val2 + RND.nextInt(128) + 1;
    // If there's an overflow,
    if (val2 < 0) {
      val1 = val1 + 1;
      val2 = RND.nextInt(128);
    }

    // Update the counter file,
    counter.position(0);
    counter.putLong(val1);
    counter.putLong(val2);

    // Turn it into a key,
    Key key = new Key((short) 0, (int) (10 + val1), val2);

    // Put the association in the map,
    lookup_sdata.put(ref, key);

    // Return the key,
    return key;
  }

  /**
   * Dereferences to a using the lookup map. Generations an exception if the
   * reference can not be dereferenced.
   */
  private Key dereference(Reference ref) {

    ReferenceCacheKey gen_key = new ReferenceCacheKey(2, ref);
    GeneralCache gen_cache = transaction.getGeneralCache(Scope.TRANSACTION);
    Key key = (Key) gen_cache.get(gen_key);

    if (key == null) {

      DataFile lookup_table =
              transaction.getDataFile(RESOURCE_LOOKUP_KEY, getReadWriteMode());
      ReferenceKeyLookup lookup_sdata = new ReferenceKeyLookup(lookup_table);
      key = lookup_sdata.get(ref);

      if (key == null) {
        throw new RuntimeException("Unable to dereference: " + ref);
      }

      gen_cache.put(gen_key, key);

    }
    return key;

//    DataFile lookup_table =
//              transaction.getDataFile(RESOURCE_LOOKUP_KEY, getReadWriteMode());
//    ReferenceKeyLookup lookup_sdata = new ReferenceKeyLookup(lookup_table);
//    Key key = lookup_sdata.get(ref);
//    
//    if (key == null) {
//      throw new RuntimeException("Unable to dereference: " + ref);
//    }
//    return key;
  }

  /**
   * Constructs an empty class bucket with the given reference.
   */
  private void constructClassBucket(Reference ref) {
    // Add the reference to the data resource list,

    // Use a hash function and db lookup to turn the reference into a unique
    // Key,
    Key key = allocateKeyForReference(ref);

    // Log the resource allocation,
    getObjectLog().logAllocateClassBucket(key, ref);

  }

  /**
   * Constructs an empty data resource with the given reference.
   */
  private Key constructEmptyData(Reference ref) {
    // Add the reference to the data resource list,

    // Use a hash function and db lookup to turn the reference into a unique
    // Key,
    Key key = allocateKeyForReference(ref);

    // Log the resource allocation,
    getObjectLog().logAllocateResource(key, ref);

    return key;
  }

  /**
   * Constructs an empty list resource with the given reference.
   */
  private Key constructEmptyList(Reference ref) {
    // Add the reference to the list resource list,

    // Use a hash function and db lookup to turn the reference into a unique
    // Key,
    Key key = allocateKeyForReference(ref);

    // Log the list allocation,
    getObjectLog().logAllocateList(key, ref);

    return key;
  }

  /**
   * Dereferences to a Key used to store serializations of objects of this
   * class, given a class reference.
   */
  Key dereferenceToBucket(Reference ref) {
    // Special case for the system class list,
    if (ref.equals(SYS_CLASS_REFERENCE)) {
      return SYS_CLASS_BUCKET;
    }
    else if (ref.equals(SYS_NAMER_REFERENCE)) {
      return SYS_NAMER_BUCKET;
    }

    return dereference(ref);
  }

  /**
   * Dereference to a Key used to store a list of serialized objects, given a
   * reference to the list.
   */
  Key dereferenceToList(Reference ref) {
    return dereference(ref);
  }

  /**
   * References to a Key containing the content of the data resource, given
   * a reference to the data resource.
   */
  Key dereferenceToResource(Reference ref) {
    return dereference(ref);
  }


  /**
   * Creates a globally unique reference for an object of the given class.
   */
  Reference createUniqueReference(ODBClass clazz) {

    // The reference is composed of the time stamp + a random number
    long ts_long = System.currentTimeMillis();
    long rnd_long = Math.abs(RND.nextLong());

    return new Reference(ts_long, rnd_long);
  }

  /**
   * Returns an OrderedSetData component for storing objects with the given
   * class.
   */
  private OrderedSetData getBucketSetData(ODBClass clazz) {

    // Dereference it into a Key bucket,
    Reference reference = clazz.getReference();
    Key bucket_key = dereferenceToBucket(reference);

    // Fetch from the cache,
    OrderedSetData list = bucket_cache.get(bucket_key);
    if (list == null) {
      DataFile list_df =
                      transaction.getDataFile(bucket_key, getReadWriteMode());
      list = new OrderedSetData(list_df, object_comparator);
      list.useSectionsCache(true);
      bucket_cache.put(bucket_key, list);
    }
    return list;

  }


  /**
   * Defines a class in the database, called by ODBClassCreator when a class
   * set is finalized.
   */
  void defineClass(ODBClassDefinition class_def, Reference class_ref) {

    // The class name,
    String class_name = class_def.getClassName();

    // A serialization of the fields of the class,
    StringBuilder b = new StringBuilder();
    List<FieldInfo> fields = class_def.getFields();
    for (FieldInfo f : fields) {
      b.append(f.getName());
      b.append(' ');
      b.append(f.getType());
      b.append(' ');
      b.append(f.isMutable());
      b.append('\n');
    }
    String class_serialization = b.toString();

    // Construct a serialization of the class object,
    ODBObject class_obj = constructObject(
                 ODBClasses.CLASS, class_ref, class_name, class_serialization);

    // Assert the class references are equal,
    assert(class_ref == class_obj.getReference());

    try {

      // Insert it into the system classes list,
      getSystemClassList().insert(class_ref);

      // Allocate an object bucket for this class,
      constructClassBucket(class_ref);

    }
    catch (ConstraintViolationException e) {
      // If we get a constraint violation, delete the class reference

      // Safe to remove this since we just created it and the reference has
      // been kept in scope.
      unsafeDelete(class_ref);

      throw new RuntimeException(MessageFormat.format(
                           "Class ''{0}'' is already defined", class_name), e);
    }
  }




  /**
   * Given a type string, returns an ODBClass object for the type. This
   * handles both system types and user defined types (eg.
   * "Person#0000012a5091cea57...")
   */
  private ODBClass resolveClassFromType(String type_str) {

    if (type_str.startsWith("[")) {
      // This is a list type, we look it up in the system class dictionary,
      if (type_str.startsWith("[L<")) {
        return getListClass(type_str);
      }

      throw new Error("PENDING: " + type_str);
    }
    else if (type_str.equals("$Class")) {
      return ODBClasses.CLASS;
    }
    else {

      // Extract the reference from the type string,
      int ref_delim = type_str.indexOf("#");
      String high_hex = type_str.substring(ref_delim + 1, ref_delim + 1 + 16);
      String low_hex = type_str.substring(ref_delim + 1 + 16);

      long high_long = Long.parseLong(high_hex, 16);
      long low_long = Long.parseLong(low_hex, 16);

      Reference ref = new Reference(high_long, low_long);
      return getClass(ref);
    }

  }

  /**
   * Checks if the object referenced is of the given type. Generates a type
   * exception if not.
   */
  void checkObjectType(ODBClass type, Reference ref) {

    // This finds the bucket for the given class type and looks up
    // the reference in the bucket. If the reference isn't found then
    // an exception is generated.

    // Get the bucket set for system class definitions,
    OrderedSetData class_bucket_set = getBucketSetData(type);

    try {
      // The key being searched for,
      ByteArray search_key = createSearchKey(ref);
      // The tail set from the search key
      OrderedSetData found_set = class_bucket_set.tailSet(search_key);
      // If the tail set is empty or the found entry doesn't match, generate
      // an error.
      if (found_set.isEmpty()) {
        throw new RuntimeException(
                  MessageFormat.format("Reference {0} is not type {1}",
                                       ref, type));
      }
      ByteArray class_ser = found_set.first();
      DataInputStream din = class_ser.getDataInputStream();

      // Check the ref of the found record matches,
      long rh = din.readLong();
      long rl = din.readLong();

      if (rh != ref.getHighLong() || rl != ref.getLowLong()) {
        throw new RuntimeException(
                  MessageFormat.format("Reference {0} is not type {1}",
                                       ref, type));
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  /**
   * Checks the type of the field (as an encoded string) against the Java
   * object. If the field type is a reference then the Reference is confirmed
   * by looking up the object in the database. If the type is invalid an
   * exception is thrown.
   */
  private void checkType(Object obj, String field_type) {

    // Nulls,
    if (obj == null) {
      // Any type not allow nulls?
    }

    // Inline string type,
    else if (obj instanceof String) {
      if (!field_type.equals("[S")) {
        throw new RuntimeException("Invalid type");
      }
    }

    // Reference to an object,
    else if (obj instanceof Reference) {
      // '[' indicates an inline type, which would be an invalid type for a
      // reference.
      if (field_type.startsWith("[")) {
        throw new RuntimeException("Invalid type");
      }
      // A reference can either be a reference to an internal type such as a
      // "$Class" or "$String", or a user defined object which does not start
      // with "$".

      // Resolve the type reference to a class,
      ODBClass field_class = resolveClassFromType(field_type);

      // Validate the object against the class,
      checkObjectType(field_class, (Reference) obj);

    }

    // Unknown type,
    else {
      throw new RuntimeException("Unrecognized type");
    }
  }

  /**
   * Creates a ByteArray that is a serialization of the given class and
   * arguments for the given reference.
   */
  private ByteArray createObjectSer(Reference ref,
                            ODBClass clazz, Object[] args) throws IOException {

    ODBTransactionImpl.IntByteArrayStream bout =
                                new ODBTransactionImpl.IntByteArrayStream(512);
    DataOutputStream dout = new DataOutputStream(bout);
    int field_count = clazz.getFieldCount();

    // Write the unique reference id,
    dout.writeLong(ref.getHighLong());
    dout.writeLong(ref.getLowLong());

    // Write the field data,
    for (int i = 0; i < field_count; ++i) {
      Object val = args[i];
      if (val == null) {
        // Nulls
        dout.writeByte(0);
      }
      else if (val instanceof String) {
        // A string primitive
        dout.writeByte(1);
        dout.writeUTF(val.toString());
      }
      else if (val instanceof Reference) {
        // An object reference
        Reference obj_ref = (Reference) val;
        dout.writeByte(2);
        dout.writeLong(obj_ref.getHighLong());
        dout.writeLong(obj_ref.getLowLong());
      }
//      else if (val instanceof ResourceKey) {
//        // An object reference
//        ResourceKey obj_ref = (ResourceKey) val;
//        dout.writeByte(3);
//        dout.writeLong(obj_ref.getHighLong());
//        dout.writeLong(obj_ref.getLowLong());
//      }
      else {
        throw new RuntimeException("Unrecognized class");
      }
    }

    // Turn it into a binary array
    return bout.toByteArrayObject();
  }


  /**
   * Replaces an object at the given reference with the given arguments. This
   * is an in-place replace operation - the reference of the object does not
   * change.
   * <p>
   * It is assumed that any checks that prevent changing immutable fields in
   * an object has already happened previous to calling this method.
   */
  private void replaceInDB(Reference ref, ODBClass clazz, Object[] args) {

    // Get the storage bucket for this class
    final OrderedSetData bucket_set = getBucketSetData(clazz);

    try {
      // Create a serialization of the object as a byte array,
      ByteArray object_ser = createObjectSer(ref, clazz, args);

      // Replace in the bucket,
      boolean success = bucket_set.replace(object_ser);
      if (!success) {
        // Oops, key not found!
        throw new RuntimeException("Ref not found.");
      }

      // Update the log,
      getObjectLog().logObjectChange(clazz.getReference(), ref);

    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  /**
   * Immediately deletes the object with the given reference. This is an
   * unsafe operation and should only be used when it's known the reference
   * is out of scope.
   */
  void unsafeDelete(Reference ref) {
//    throw new Error("PENDING");
  }

  /**
   * Creates a ByteArray that is a search key for the given referenced object.
   */
  private static ByteArray createSearchKey(Reference ref) {

    byte[] buf = new byte[16];
    ByteArrayUtil.setLong(ref.getHighLong(), buf, 0);
    ByteArrayUtil.setLong(ref.getLowLong(), buf, 8);

    return new JavaByteArray(buf, 0, 16);

  }


  private ODBList createODBList(String list_type_str,
                                Reference list_reference) {

    // Parse the field type string
    int delim = list_type_str.indexOf(">(");
    String element_class_string = list_type_str.substring(3, delim);
    String arg_str = list_type_str.substring(delim + 2,
                                             list_type_str.length() - 1);
    String[] args = arg_str.split(",");

    // Parse the qualified class name,
    ODBClass element_class = resolveClassFromType(element_class_string);

    // Create the order spec object for the list,
    boolean allow_duplicates = !args[0].equals("unique");
    OrderSpec list_order_spec;
    if (args.length > 1) {
      // Key order specification,
      String list_key = args.length > 1 ? args[1] : null;
      String collat = args.length > 2 ? args[2] : null;
      list_order_spec = new OrderSpec(allow_duplicates,
                element_class, element_class.indexOfField(list_key), collat);
    }
    else {
      // None key order specification,
      list_order_spec = new OrderSpec(allow_duplicates, element_class);
    }

    // The list data file,
    Key list_key = dereferenceToList(list_reference);
    DataFile list_data = transaction.getDataFile(list_key, getReadWriteMode());
    ODBClass list_class = getListClass(list_type_str);

    // The list object,
    return new OrderedReferenceList(
                            ODBTransactionImpl.this,
                            list_reference,
                            list_class,
                            list_data, list_order_spec);

  }


  private ODBData createODBData(Reference data_reference) {

    // The data file,
    Key data_key = dereferenceToResource(data_reference);
    AddressableDataFile data =
                         transaction.getDataFile(data_key, getReadWriteMode());

    // The data object,
    return new Data(ODBTransactionImpl.this, data_reference, data);

  }



  // -----

  private static class IntByteArrayStream extends ByteArrayOutputStream {

    public IntByteArrayStream(int size) {
      super(size);
    }

    public IntByteArrayStream() {
      super();
    }

    public ByteArray toByteArrayObject() {
      return new JavaByteArray(buf, 0, count);
    }

  }

  /**
   * An implementation of List<String> that represents a list of objects whose
   * first field is the string.
   */
  private class FirstFieldObjectList extends AbstractList<String> {

    private ODBClass element_class;
    private OrderedReferenceList list;

    private FirstFieldObjectList(
                          ODBClass element_class, OrderedReferenceList list) {
      this.element_class = element_class;
      this.list = list;
    }

    @Override
    public String get(int index) {
      Reference item_ref = list.get(index);
      if (item_ref == null) {
        return null;
      }
      // The object
      ODBObject item_ob = getObject(element_class, item_ref);
      // Return the name,
      return item_ob.getString(0);
    }

    @Override
    public int size() {
      long sz = list.size();
      if (sz > Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      }
      else {
        return (int) sz;
      }
    }

  }

  /**
   * An implementation of ODBObject where the content is materialized in
   * memory.
   */
  private class MaterializedODBObject implements ODBObject {

    private final ODBClass clazz;
    private final Reference reference;
    private final Object[] content;

    MaterializedODBObject(ODBClass clazz,
                          Reference reference, Object[] content) {
      this.clazz = clazz;
      this.reference = reference;
      this.content = content;
    }

    /**
     * Convenience that throws an error if the field of the class is not
     * mutable.
     */
    private void checkFieldMutable(int i) {
      if (!clazz.isFieldMutable(i)) {
        throw new RuntimeException(
                   MessageFormat.format("{0}.{1} is not a mutable", clazz, i));
      }
    }

    /**
     * Sets the object of the given field number in this object. Throws an
     * exception if obj is the wrong type as specified, or references an
     * incompatible class, or if the field is not mutable.
     */
    private void setObject(int i, Object obj) {
      checkFieldMutable(i);

      if (obj instanceof ODBObject) {
        obj = ((ODBObject) obj).getReference();
      }

      // Check the reference against the type of the field,
      checkType(obj, clazz.getFieldType(i));

      Object old_content_i = content[i];
      boolean success = false;
      try {
        content[i] = obj;

        // Replace this item,
        replaceInDB(reference, clazz, content);

        success = true;
      }
      finally {
        // Revert content value if we failed,
        if (!success) {
          content[i] = old_content_i;
        }
      }
    }



    // ---------- Implemented ----------

    @Override
    public ODBClass getODBClass() {
      return clazz;
    }

    @Override
    public Reference getReference() {
      return reference;
    }

    // ----- Content getters

    @Override
    public int size() {
      return content.length;
    }

//    public Reference getReference(int i) {
//      return (Reference) content[i];
//    }

    @Override
    public ODBData getData(int i) {

      // Get the reference for the data object,
      Reference data_reference = (Reference) content[i];

      return createODBData(data_reference);

    }

    @Override
    public ODBList getList(int i) {

      // Get the reference for the list,
      Reference list_reference = (Reference) content[i];

      // The list type
      final String list_type_str = clazz.getFieldType(i);

      return createODBList(list_type_str, list_reference);

//      System.out.println("List obj_string = " + obj_string);
//      System.out.println("List args = " + args);
//
//      System.out.println("Fetching list: " + list_type_str);
//      System.out.println("Key = " + key);

    }

    @Override
    public ODBObject getObject(int i) {
      Reference ref = (Reference) content[i];

      if (ref == null) {
        return null;
      }

      // Resolve the class from the type description of the field,
      ODBClass field_class = resolveClassFromType(clazz.getFieldType(i));
      // Get the class of the type,
      return ODBTransactionImpl.this.getObject(field_class, ref);
    }

    @Override
    public String getString(int i) {
      return (String) content[i];
    }

    @Override
    public ODBData getData(String field_name) {
      return getData(clazz.indexOfField(field_name));
    }

    @Override
    public ODBList getList(String field_name) {
      return getList(clazz.indexOfField(field_name));
    }

    @Override
    public ODBObject getObject(String field_name) {
      return getObject(clazz.indexOfField(field_name));
    }

//    public Reference getReference(String field_name) {
//      return getReference(clazz.indexOfField(field_name));
//    }

    @Override
    public String getString(String field_name) {
      return getString(clazz.indexOfField(field_name));
    }

    // ----- Content setters

    private void setReference(int i, Reference ref) {
      setObject(i, ref);
    }

//    public void setData(int i, ODBData data) {
//      checkFieldMutable(i);
//
//      throw new UnsupportedOperationException("Not supported yet.");
//    }
//
//    public void setList(int i, ODBList list) {
//      checkFieldMutable(i);
//
//      throw new UnsupportedOperationException("Not supported yet.");
//    }

    @Override
    public void setObject(int i, ODBObject obj) {
      Reference ref = obj.getReference();
      setReference(i, ref);
    }

    @Override
    public void setString(int i, String str) {
      setObject(i, str);
    }

//    public void setData(String field_name, ODBData data) {
//      setData(this.clazz.indexOfField(field_name), data);
//    }
//
//    public void setList(String field_name, ODBList list) {
//      setList(this.clazz.indexOfField(field_name), list);
//    }

    @Override
    public void setObject(String field_name, ODBObject obj) {
      setObject(this.clazz.indexOfField(field_name), obj);
    }

//    public void setReference(String field_name, Reference ref) {
//      setReference(this.clazz.indexOfField(field_name), ref);
//    }

    @Override
    public void setString(String field_name, String str) {
      setString(this.clazz.indexOfField(field_name), str);
    }


    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("Object ");
      b.append(getReference().toString());
      b.append(" (");
      b.append(clazz.toString());
      b.append(") [ ");
      for (int i = 0; i < content.length; ++i) {
        b.append(content[i]);
        b.append(" ");
      }
      b.append("]");
      return b.toString();
    }


  }

  /**
   * An implementation of ODBClass for user defined objects.
   */
  private class MaterializedODBClass implements ODBClass {

    private final Reference reference;
    private final String class_name;
    private final String field_serialization;

    private String cached_instance_name;

    private FieldInfo[] decoded_fields = null;

    MaterializedODBClass(Reference reference,
                         String class_name, String field_serialization) {
      this.reference = reference;
      this.class_name = class_name;
      this.field_serialization = field_serialization;
    }

    private void ensureFieldsDeserialized() {
      if (decoded_fields == null) {
        String[] field_strs = field_serialization.split("\n");
        decoded_fields = new FieldInfo[field_strs.length];
        for (int i = 0; i < field_strs.length; ++i) {
          String str = field_strs[i];
          String[] parts = str.split(" ");
          boolean mutable = parts[2].equals("true");
          decoded_fields[i] = new FieldInfo(parts[0], parts[1], mutable);
        }
      }
    }

    // ----- Implemented

    @Override
    public ODBClass getODBClass() {
      return ODBClasses.CLASS;
    }

    @Override
    public Reference getReference() {
      return reference;
    }

    @Override
    public int getFieldCount() {
      ensureFieldsDeserialized();
      return decoded_fields.length;
    }

    @Override
    public String getFieldName(int n) {
      ensureFieldsDeserialized();
      return decoded_fields[n].getName();
    }

    @Override
    public String getFieldType(int n) {
      ensureFieldsDeserialized();
      return decoded_fields[n].getType();
    }

    @Override
    public String getName() {
      return class_name;
    }

    @Override
    public String getInstanceName() {
      if (cached_instance_name == null) {
        cached_instance_name = class_name + "#" + reference.toString();
      }
      return cached_instance_name;
    }

    @Override
    public int indexOfField(String field_name) {
      ensureFieldsDeserialized();
      for (int i = 0; i < decoded_fields.length; ++i) {
        if (decoded_fields[i].getName().equals(field_name)) {
          return i;
        }
      }
      return -1;
    }

    @Override
    public boolean isFieldMutable(int n) {
      ensureFieldsDeserialized();
      return decoded_fields[n].isMutable();
    }

    @Override
    public String toString() {
      return getInstanceName();
    }

  }

  /**
   * A comparator for finding objects with a particular 128-bit key as the
   * immediate header of the ByteArray record.
   */
  private static Comparator<ByteArray> object_comparator =
                                                new Comparator<ByteArray>() {

    /**
     * A comparator where the reference is the first 16 bytes of the record.
     */
    @Override
    public int compare(ByteArray o1, ByteArray o2) {

      try {
        // Order based on first 16 bytes which is the value key
        DataInputStream din1 = o1.getDataInputStream();
        DataInputStream din2 = o2.getDataInputStream();

        // Read the 16 byte reference key in o1
        long o1_h = din1.readLong();
        long o1_l = din1.readLong();
        Reference ref1 = new Reference(o1_h, o1_l);
        // Read the 16 byte reference key in o2
        long o2_h = din2.readLong();
        long o2_l = din2.readLong();
        Reference ref2 = new Reference(o2_h, o2_l);

        return ref1.compareTo(ref2);

      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }

    }

  };


  /**
   * A collator for the class/reference map.
   */
  private static final Comparator<ByteArray> dictionary_collator =
                                                new Comparator<ByteArray>() {


    private int compareRefs(DataInputStream din1, DataInputStream din2)
                                                           throws IOException {

      long r1_high = din1.readLong();
      long r1_low = din1.readLong();
      Reference ref1 = new Reference(r1_high, r1_low);
      long r2_high = din2.readLong();
      long r2_low = din2.readLong();
      Reference ref2 = new Reference(r2_high, r2_low);
      return ref1.compareTo(ref2);

    }

    private int compareTypes(DataInputStream din1, DataInputStream din2)
                                                           throws IOException {
      String type1 = din1.readUTF();
      String type2 = din2.readUTF();
      return type1.compareTo(type2);
    }

    /**
     * A comparator for the map encoded values.
     */
    @Override
    public int compare(ByteArray o1, ByteArray o2) {

      try {
        // Order based on first 16 bytes which is the value key
        DataInputStream din1 = o1.getDataInputStream();
        DataInputStream din2 = o2.getDataInputStream();

        byte code1 = din1.readByte();
        byte code2 = din2.readByte();
        if (code1 < code2) {
          return -1;
        }
        else if (code1 > code2) {
          return 1;
        }
        // If the codes are the same,
        else {
          // The string type is first,
          if (code1 == 0x001) {
            int c = compareTypes(din1, din2);
            if (c == 0) {
              c = compareRefs(din1, din2);
            }
            return c;
          }
          // The reference is first,
          else if (code1 == 0x002) {
            int c = compareRefs(din1, din2);
            if (c == 0) {
              c = compareTypes(din1, din2);
            }
            return c;
          }
          // Otherwise generate an exception,
          else {
            throw new RuntimeException("Unknown code type: " + code1);
          }
        }

      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }

    }

  };



  /**
   * The GC finalize method for this object.
   */
  @Override
  public void finalize() throws Throwable {
    session.getDatabaseClient().disposeTransaction(transaction);
    super.finalize();
  }


  // ---------- Statics ----------

  /**
   * A secure random generator used for key generation.
   */
  private final static SecureRandom RND;

  static {
    // We seed an SHA1PRNG generator with a 128 byte seed created from
    // the system default generator. The aim of doing it this way is to
    // produce a generator with predictible performance across
    // systems/implementations.

    // For our use, it's not necessary for the generated numbers to
    // be cryptographically secure.

    SecureRandom r;
    // 2048 bits of randomness hopefully!
    byte[] rseed = SecureRandom.getSeed(256);
    try {
      r = SecureRandom.getInstance("SHA1PRNG");
    }
    catch (NoSuchAlgorithmException e) {
      r = new SecureRandom();
      System.err.println("SHA1PRNG random number instance not available");
      System.err.println("Using system default");
    }
    r.setSeed(rseed);
    RND = r;
  }


  /**
   * The order specification for the system classes list.
   */
  private final static OrderSpec SYS_CLASSES_ORDER_SPEC;

  /**
   * The order specification for the named item list.
   */
  private final static OrderSpec SYS_NAMER_ORDER_SPEC;


  static {
    // 'false' to duplicates.
    // 0 is field number of the class name.
    // Lexicographical order.
    SYS_CLASSES_ORDER_SPEC = new OrderSpec(false,
             ODBClasses.CLASS, ODBClasses.CLASS.indexOfField("name"), "lexi");

    // 'false' to duplicates.
    // 0 is field number of the item name.
    // Lexicographical order.
    SYS_NAMER_ORDER_SPEC = new OrderSpec(false,
             ODBClasses.NAMER, ODBClasses.NAMER.indexOfField("name"), "lexi");

  }

}
