/**
 * com.mckoi.odb.util.FileSystemImpl  May 16, 2010
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

package com.mckoi.odb.util;

import com.mckoi.data.DataFile;
import com.mckoi.data.DelegateAddressableDataFile;
import com.mckoi.network.CommitFaultException;
import com.mckoi.odb.*;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * This class maps a file system like interface over an ObjectDatabase path.
 * The file system supports features such as file meta information, directory
 * trees, etc.
 * <p>
 * Use the static 'defineSchema' method to create the classes to support the
 * data. Use 'setup' to initiate the data.
 *
 * @author Tobias Downer
 */

public class FileSystemImpl implements FileSystem {

  /**
   * The transaction.
   */
  private final ODBTransaction transaction;

  /**
   * The named item that is the root object of the file system.
   */
  private final String named_root;

  /**
   * True when the repository is invalidated.
   */
  private transient boolean invalidated = false;

  /**
   * Constructs the file repository over the given ODBTransaction. The
   * 'named_root' string is the name of the file system in the path to store
   * the file system data. Multiple file systems can be created in a MckoiDDB
   * path by using a different named_root for each.
   */
  public FileSystemImpl(ODBTransaction transaction, String named_root) {
    this.transaction = transaction;
    this.named_root = named_root;
  }

  /**
   * Defines the class schema for the file system at the source location of
   * the ODBTransaction. This will generate an exception if the classes
   * already exist.
   */
  public static void defineSchema(ODBTransaction t)
                                              throws ClassValidationException {

    ODBClassCreator class_creator = t.getClassCreator();

    // The root object of the file system,
    ODBClassDefinition root_class = class_creator.createClass("FS.Root");
    root_class.defineString("version", true);
    root_class.defineString("name", true);
    root_class.defineString("description", true);
    root_class.defineMember("root", "FS.Directory", true);
    root_class.defineList("directories", "FS.Directory",
                       ODBOrderSpecification.lexicographic("fullname"), false);

    // The directory object of the file system,
    ODBClassDefinition directory_class =
                                     class_creator.createClass("FS.Directory");
    directory_class.defineString("name");
    directory_class.defineString("fullname");
    directory_class.defineString("meta", true);
    directory_class.defineMember("parent", "FS.Directory");
    directory_class.defineList("directories", "FS.Directory",
                           ODBOrderSpecification.lexicographic("name"), false);
    directory_class.defineList("files", "FS.File",
                           ODBOrderSpecification.lexicographic("name"), false);

    // The file object of the file system,
    ODBClassDefinition file_class = class_creator.createClass("FS.File");
    file_class.defineString("name");
    file_class.defineString("meta", true);
    file_class.defineData("content");

    // Validate and complete the class assignments,
    class_creator.validateAndComplete();

  }

  /**
   * Sets up file system to an initial know (empty) state. The
   * repository must be committed after this is called. If the filesystem
   * is already setup then an exception is generated.
   */
  public void setup(String filesystem_name, String filesystem_description) {

    // Invalidation check,
    checkInvalidated();

    ODBObject root = transaction.getNamedItem(named_root);
    // Must be null
    if (root != null) {
      throw new FileSystemException(
                           "File repository {0} is already setup", named_root);
    }

    // Make a meta string for the root directory,
    String meta_string = createDirectoryMeta();

    // Make a root path object,
    ODBClass directory_class = transaction.findClass("FS.Directory");

    // (name, fullname, meta, parent, directories, files)
    ODBObject root_path = transaction.constructObject(directory_class,
                                      "/", "/", meta_string, null, null, null);

    // The root class,
    ODBClass root_class = transaction.findClass("FS.Root");

    // (version, name, description, root_directory, directories)
    ODBObject root_ob = transaction.constructObject(root_class,
            "1.0", filesystem_name, filesystem_description,
            root_path, null);

    // Add the root to the directories list,
    ODBList dir_list = root_ob.getList("directories");
    dir_list.add(root_path);

    // Set the named object,
    transaction.addNamedItem(named_root, root_ob);

    // Done.

  }





  /**
   * Returns a meta string for a path.
   */
  private String createDirectoryMeta() {
    String timestamp = Long.toString(System.currentTimeMillis());
    HashMap<String, String> meta_values = new HashMap();
    meta_values.put("mime", "$dir");
    meta_values.put("create_timestamp", timestamp);
    meta_values.put("last_modified", timestamp);
    return createMetaString(meta_values);
  }


  /**
   * Invalidate this file repository.
   */
  protected void invalidate() {
    invalidated = true;
  }

  /**
   * Checks if this file repository has been invalidated, and generates an
   * exception if it has.
   */
  protected void checkInvalidated() {
    if (invalidated) {
      throw new FileSystemException("Invalidated");
    }
  }

  /**
   * Checks the file name is valid.
   */
  private void checkValidDBFile(String db_file) {
    if (!db_file.startsWith("/")) {
      throw new FileSystemException("File name must starts with ''/''");
    }
    // File names may not contain double /
    if (db_file.contains("//")) {
      throw new FileSystemException("File name may not contain ''//''");
    }
    // File names may not contain \
    if (db_file.contains("\\")) {
      throw new FileSystemException("File name may not contain ''\\''");
    }
  }

  /**
   * Checks the path name is valid.
   */
  private void checkValidDBPath(String db_path) {
    if (!db_path.startsWith("/")) {
      throw new FileSystemException("Path name must starts with ''/''");
    }
    if (!db_path.endsWith("/")) {
      throw new FileSystemException("Path name must end with ''/''");
    }
    if (db_path.contains("//")) {
      throw new FileSystemException("File name may not contain ''//''");
    }
    if (db_path.contains("\\")) {
      throw new FileSystemException("File name may not contain ''\\''");
    }
  }

  private ODBTransaction getTransaction() {
    return transaction;
  }

  /**
   * Internal method that fetches the content file from the transaction.
   */
  private DataFile secureGetFile(
                     ODBTransaction t, ODBObject file_ob, String file_name) {
//    System.out.println("$$$ secureGetFile(" + file_name + ")");
//    if (file_name.endsWith("web.xml")) {
//      new Error().printStackTrace();
//    }

    ODBData content = file_ob.getData("content");
    // Return a wrapped content to protect the ODBData content.
    return new DelegateAddressableDataFile(content);

  }

  /**
   * Internal method that sets the time stamp of the given file object.
   */
  private void secureTouchFile(ODBObject file_ob, long last_modified) {
    // Get the meta string,
    String meta_string = file_ob.getString("meta");
    Map<String, String> meta_values = createMetaMap(meta_string);

    // Update the 'last_modified' timestamp
    meta_values.put("last_modified", Long.toString(last_modified));

    // Update the meta string,
    file_ob.setString("meta", createMetaString(meta_values));
  }

  /**
   * Internal method that sets the mime type of the given file object.
   */
  private void secureSetMimeType(ODBObject file_ob, String mime_type) {
    // Get the meta string,
    String meta_string = file_ob.getString("meta");
    Map<String, String> meta_values = createMetaMap(meta_string);

    // Update the mime type
    meta_values.put("mime", mime_type);

    // Update the meta string,
    file_ob.setString("meta", createMetaString(meta_values));
  }

  /**
   * Secure method for fetching the size of a file list object.
   */
  private int secureGetFileListSize(ODBList list) {
    long sz = list.size();
    if (sz > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) sz;
  }

  /**
   * Secure method for fetching the content of a file list object.
   */
  private FileInfo secureGetFileListItem(
                 ODBList list, int index, ODBObject path_ob, boolean is_file) {

    ODBObject entry = list.getObject(index);

    // Is a file
    if (is_file) {
      String file_name = entry.getString("name");
      String meta_string = entry.getString("meta");
      String full_name = path_ob.getString("fullname") + file_name;
      return createFileInfo(full_name, entry, meta_string);
    }
    // Is a directory,
    else {
      String full_name = entry.getString("fullname");
      String meta_string = entry.getString("meta");
      return createFileInfo(full_name, null, meta_string);
    }

  }

  /**
   * Returns the path string of the given file name. eg, "/path/to/file.txt"
   * -> "/path/to/"
   */
  private static String getPathOf(String file_name) {
    int p = file_name.lastIndexOf('/');
    return file_name.substring(0, p + 1);
  }

  /**
   * Returns the name part (minus the path spec) of the given file name.
   * eg, "/path/to/file.txt" -> "file.txt"
   */
  private static String getFileOf(String file_name) {
    int p = file_name.lastIndexOf('/');
    return file_name.substring(p + 1);
  }

  /**
   * Parses a meta string and returns a String->String map containing the
   * values.
   */
  private Map<String, String> createMetaMap(String meta_string) {
    try {
      // Parse the meta data,
      BufferedReader meta_in = new BufferedReader(new StringReader(meta_string));

      HashMap<String, String> value_map = new HashMap();

      String line;
      while ((line = meta_in.readLine()) != null) {
        int delim = line.indexOf("=");
        if (delim > 0) {
          String key = line.substring(0, delim);
          String value = line.substring(delim + 1);
          value_map.put(key, value);
        }
      }

      return value_map;
    }
    catch (IOException e) {
      throw new FileSystemException(e);
    }
  }

  /**
   * Given a String->String map, creates a string of meta values.
   */
  private String createMetaString(Map<String, String> values) {

    StringBuilder meta_out = new StringBuilder();

    // Write out the meta property keys,
    for (String key : values.keySet()) {
      meta_out.append(key);
      meta_out.append('=');
      meta_out.append(values.get(key));
      meta_out.append('\n');
    }

    return new String(meta_out);
  }


  /**
   * Recursive method that traverses a path string and creates the directories
   * if they don't exist. Returns the FS.Directory object for the path.
   */
  private ODBObject createDirectoryIfNotExists(
                       final ODBTransaction transaction,
                       final ODBObject root_path, final String path,
                       final String full_path) {

    // Recurse end condition
    if (path.length() < 1) {
      return root_path;
    }

    // The directory to traverse,
    int path_delim = path.indexOf('/');
    String child_path = path;
    String next_path = "";
    if (path_delim != -1) {
      child_path = path.substring(0, path_delim);
      next_path = path.substring(path_delim + 1);
    }

    final String absolute_path = full_path + child_path + "/";

    // Does the child exist?
    ODBList dirs = root_path.getList("directories");
    ODBObject child_ob = dirs.getObject(child_path);
    if (child_ob == null) {
      // Doesn't exist, so create it

      // The meta information for the entry,
      String meta_string = createDirectoryMeta();

      // FS.Directory(name, fullname, meta, parent, directories, files)
      child_ob = transaction.constructObject(
             transaction.findClass("FS.Directory"),
             child_path, absolute_path,
             meta_string, root_path, null, null);
      // Add to the list,
      dirs.add(child_ob);

      // Add to the central directory index,
      ODBList central_dir =
                   transaction.getNamedItem(named_root).getList("directories");
      central_dir.add(child_ob);

    }

    // Recurse,
    return createDirectoryIfNotExists(transaction, child_ob,
                                      next_path, absolute_path);

  }

  /**
   * Creates a directory entry if one doesn't exist for the path given.
   */
  private ODBObject createDirectoryIfNotExists(
                         final ODBTransaction transaction, final String path) {

    // Go to the root,
    ODBObject root_path =
                       transaction.getNamedItem(named_root).getObject("root");
    // Recurse,
    return createDirectoryIfNotExists(transaction, root_path,
                                      path.substring(1), path.substring(0, 1));
  }

  /**
   * Traverses to a directory entry if one exists for the path given. If the
   * path does not exist, returns null.
   */
  private ODBObject traverseToDirectory(
                         final ODBTransaction transaction, final String path) {

    ODBObject root_ob = transaction.getNamedItem(named_root);

    // Look for the path in the central directory,
    ODBList central_list = root_ob.getList("directories");

    // Get the entry, or null if not found,
    ODBObject dir_entry = central_list.getObject(path);
    if (dir_entry == null) {
      return null;
    }

    return dir_entry;
  }

  /**
   * Creates an empty path directory if one doesn't exist. Recurses through
   * the path specification until all the directories have been created.
   */
  @Override
  public void makeDirectory(String path_name) {

    // Invalidation check,
    checkInvalidated();

    // Check the input is a valid path,
    checkValidDBPath(path_name);

    // Perform the operation,
    createDirectoryIfNotExists(getTransaction(), path_name);
  }

  /**
   * Deletes an empty path directory. Generates an exception if the directory
   * has files so can not be deleted, otherwise returns true if the directory
   * was deleted, false otherwise.
   */
  @Override
  public boolean removeDirectory(String path_name) {

    // Invalidation check,
    checkInvalidated();

    // Check the input is a valid path,
    checkValidDBPath(path_name);

    ODBTransaction t = getTransaction();

    ODBObject root_ob = t.getNamedItem(named_root);
    // Look for the path in the central directory,
    ODBList central_list = root_ob.getList("directories");

    // Get the entry, or return false if not found,
    ODBObject dir_entry = central_list.getObject(path_name);
    if (dir_entry == null) {
//      System.out.println("dir_entry == null");
      return false;
    }

    // If the path has sub-directories,
    if (dir_entry.getList("directories").size() > 0 ||
        dir_entry.getList("files").size() > 0) {
      throw new FileSystemException(
                     "Remove failed; Directory {0} is not empty", path_name);
    }

    // Go to the parent
    ODBObject parent_ob = dir_entry.getObject("parent");
    // If no parent (trying to remove root!)
//    System.out.println("parent_ob == null");
    if (parent_ob == null) {
      return false;
    }

    int delim = path_name.lastIndexOf("/", path_name.length() - 2);
//    System.out.println(path_name);
//    System.out.println(delim);
    String dir_name = path_name.substring(delim + 1, path_name.length() - 1);

//    System.out.println("dir_name to removed = " + dir_name);

    // Remove from the directories,
    boolean removed = parent_ob.getList("directories").remove(dir_name);
//    System.out.println("removed = " + removed);
    if (removed) {
      // Remove from the central directory index,
      central_list.remove(path_name);
    }

    return removed;

  }

  /**
   * Creates a file from the repository with the given name, mime type and
   * timestamp. The file is created empty. If the file doesn't exist,
   * generates an exception.
   */
  @Override
  public void createFile(String file_name,
                         String mime_type, long last_modified) {

    // Invalidation check,
    checkInvalidated();

    // Check the remote_file is valid,
    checkValidDBFile(file_name);

    ODBTransaction t = getTransaction();

    // Fetch the path meta first,
    String file_name_path = getPathOf(file_name);
    // Create the directory if it doesn't already exist,
    ODBObject dir_object = createDirectoryIfNotExists(t, file_name_path);

    // The file name,
    String top_file_name = getFileOf(file_name);

    // Get the files list,
    ODBList files_list = dir_object.getList("files");
    // Does the file already exist?
    if (files_list.contains(top_file_name)) {
      throw new FileSystemException("File {0} already exists", file_name);
    }

    // Form the meta object,
    HashMap<String, String> meta_values = new HashMap();
    String timestamp_str = Long.toString(last_modified);
    meta_values.put("create_timestamp", timestamp_str);
    meta_values.put("mime", mime_type);
    meta_values.put("last_modified", timestamp_str);

    // Create a file object,
    // FS.File(name, meta, content)
    ODBObject file_object = t.constructObject(t.findClass("FS.File"),
                 getFileOf(file_name), createMetaString(meta_values), null);
    // Add it to the list,
    files_list.add(file_object);

  }

  /**
   * Deletes a file from the repository with the given name. Returns true if
   * a file with the given name was found and deleted, otherwise returns
   * false.
   */
  @Override
  public boolean deleteFile(String file_name) {

    // Invalidation check,
    checkInvalidated();

    // Check the remote_file is valid,
    checkValidDBFile(file_name);

    // Can not delete directory references,
    if (file_name.endsWith("/")) {
      return false;
    }

    ODBTransaction t = getTransaction();

    // Go to the parent and fetch the child meta,
    int p = file_name.lastIndexOf('/', file_name.length() - 1);
    String parent_path = file_name.substring(0, p + 1);
    String child_item = file_name.substring(p + 1);

    // Traverse to the directory of the file,
    ODBObject path_ob = traverseToDirectory(t, parent_path);
    if (path_ob == null) {
      // No directory so nothing to delete,
      return false;
    }

    // Get the list of files in the directory,
    ODBList file_list = path_ob.getList("files");
    // Get the file object,
    ODBObject file_ob = file_list.getObject(child_item);
    if (file_ob == null) {
      // Not found,
      return false;
    }

    // Remove the file from the list,
    boolean removed = file_list.remove(child_item);
    // Delete the content of the file,
    file_ob.getData("content").delete();

    return removed;

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void touchFile(String file_name, long last_modified) {

    // Invalidation check,
    checkInvalidated();

    ODBTransaction t = getTransaction();

    ODBObject meta_ob;
//    boolean is_directory = false;

    if (file_name.endsWith("/")) {
      // It's a directory,
      checkValidDBPath(file_name);

      // Traverse to the directory entry,
      ODBObject path_ob = traverseToDirectory(t, file_name);
      if (path_ob == null) {
        throw new FileSystemException("Path not found: {0}", file_name);
      }

      meta_ob = path_ob;
//      is_directory = true;

    }
    else {
      // It's a file,
      // Check the remote_file is valid,
      checkValidDBFile(file_name);

      // Go to the parent and fetch the child meta,
      int p = file_name.lastIndexOf('/', file_name.length() - 1);
      String parent_path = file_name.substring(0, p + 1);
      String child_item = file_name.substring(p + 1);

      // Traverse to the directory entry,
      ODBObject path_ob = traverseToDirectory(t, parent_path);
      if (path_ob == null) {
        throw new FileSystemException("Path not found: {0}", parent_path);
      }

      // Get the list of files in the directory,
      ODBList file_list = path_ob.getList("files");
      meta_ob = file_list.getObject(child_item);

      // If the file doesn't exist,
      if (meta_ob == null) {
        throw new FileSystemException("File not found: {0}", child_item);
      }

    }

    // Perform the touch operation,
    secureTouchFile(meta_ob, last_modified);

//    // Get the meta string,
//    String meta_string = meta_ob.getString("meta");
//    Map<String, String> meta_values = createMetaMap(meta_string);
//
//    meta_values.put("last_modified", Long.toString(last_modified));
//
//    // Update the meta string,
//    meta_ob.setString("meta", createMetaString(meta_values));

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateFileInfo(String file_name, String mime_type,
                             long last_modified) {

    // Invalidation check,
    checkInvalidated();

    ODBTransaction t = getTransaction();
    ODBObject meta_ob;
    boolean is_directory = false;

    if (file_name.endsWith("/")) {
      // It's a directory,
      checkValidDBPath(file_name);

      // Traverse to the directory entry,
      ODBObject path_ob = traverseToDirectory(t, file_name);
      if (path_ob == null) {
        throw new FileSystemException("Path not found: {0}", file_name);
      }

      meta_ob = path_ob;
      is_directory = true;

    }
    else {
      // It's a file,
      // Check the remote_file is valid,
      checkValidDBFile(file_name);

      // Go to the parent and fetch the child meta,
      int p = file_name.lastIndexOf('/', file_name.length() - 1);
      String parent_path = file_name.substring(0, p + 1);
      String child_item = file_name.substring(p + 1);

      // Traverse to the directory entry,
      ODBObject path_ob = traverseToDirectory(t, parent_path);
      if (path_ob == null) {
        throw new FileSystemException("Path not found: {0}", parent_path);
      }

      // Get the list of files in the directory,
      ODBList file_list = path_ob.getList("files");
      meta_ob = file_list.getObject(child_item);

      // If the file doesn't exist,
      if (meta_ob == null) {
        throw new FileSystemException("File not found: {0}", child_item);
      }

    }

    // Get the meta string,
    String meta_string = meta_ob.getString("meta");
    Map<String, String> meta_values = createMetaMap(meta_string);

    if (!is_directory) {
      meta_values.put("mime", mime_type);
    }
    meta_values.put("last_modified", Long.toString(last_modified));

    // Update the meta string,
    meta_ob.setString("meta", createMetaString(meta_values));

  }


  /**
   * Uploads a local file into the database if it is different that the file
   * stored. 'remote_file' is the name to call the file remotely. Note that the
   * 'lastmodified' property is taken from the time the file was uploaded.
   * <p>
   * If the local file is the same, no upload takes place.
   */
  @Override
  public boolean synchronizeFile(InputStream file_in1,
          long local_file_len, long local_modified_time, String mime_type,
                                     String remote_file) throws IOException {

    // Invalidation check,
    checkInvalidated();

    // Check the remote_file is valid,
    checkValidDBFile(remote_file);

//    ODBTransaction t = getTransaction();

    // Determine if we skip or not,
    boolean skip = false;
    FileInfo file_info = getFileInfo(remote_file);
    if (file_info != null) {
      long mod_long = file_info.getLastModified();
      // If the file is significantly different (allow for rounding
      // differences),
      if (local_modified_time < (mod_long + 500) &&
          local_modified_time > (mod_long - 500)) {
        // Comare the file size,
        DataFile file_df = file_info.getDataFile();
        if (file_df.size() == local_file_len) {
          // File sizes are the same, so skip this file,
          skip = true;
        }
      }
    }

    // Do we skip? (only true if there's a file with the same name, size and
    // timestamp).
    if (!skip) {

      // If file_info is null, create the file,
      if (file_info == null) {
        createFile(remote_file, mime_type, local_modified_time);
        file_info = getFileInfo(remote_file);
      }
      // Otherwise update the existing file info,
      else {
        updateFileInfo(remote_file, mime_type, local_modified_time);
      }

      // Perform the copy,
      DataFile remote_dfile = file_info.getDataFile();
      // Clean it,
      remote_dfile.delete();
      // Write the content
      BufferedInputStream bin = new BufferedInputStream(file_in1);
      byte[] buf = new byte[2048];
      while (true) {
        int read_count = bin.read(buf, 0, buf.length);
        if (read_count == -1) {
          break;
        }
        remote_dfile.put(buf, 0, read_count);
      }

      // File copied, so return true
      return true;

    }

    // No copy necessary, so return false
    return false;
  }

  /**
   * Uploads a local file into the database if it is different that the file
   * stored. 'remote_file' is the name to call the file remotely. Note that the
   * 'lastmodified' property is taken from the time the file was uploaded.
   * <p>
   * If the local file is the same, no upload takes place.
   */
  public boolean synchronizeFile(File local_file, String remote_file)
                                                          throws IOException {

    // Invalidation check,
    checkInvalidated();

    // Get the mime type for the local file,
    URL file_url = local_file.toURI().toURL();
    URLConnection c = file_url.openConnection();
    String mime_type = c.getContentType();

    FileInputStream fin = new FileInputStream(local_file);

    return synchronizeFile(fin,
            local_file.length(), local_file.lastModified(),
            mime_type, remote_file);

  }

  /**
   * Create a FileInfo object given a meta object that references a 'f.*'
   * type object.
   */
  private FileInfo createFileInfo(String item_name,
                                  ODBObject item_ob, String meta_string) {

    // Parse the meta string,
    Map<String, String> meta_map = createMetaMap(meta_string);
    String mime_type = meta_map.get("mime");
    long last_mod = Long.parseLong(meta_map.get("last_modified"));

    return new FRFileInfo(item_name, mime_type, last_mod, item_ob);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<FileInfo> getDirectoryFileInfoList(String dir) {

    // Invalidation check,
    checkInvalidated();

    checkValidDBPath(dir);

    ODBTransaction t = getTransaction();

    // Traverse to the directory entry,
    ODBObject path_ob = traverseToDirectory(t, dir);
    if (path_ob == null) {
      // Returns null if not found,
      return null;
    }

    // The list of directories,
    ODBList directories = path_ob.getList("directories");
    // The list of files,
    ODBList files = path_ob.getList("files");

    // Return the lazily created list,
    return new DirectoryAndFileInfoList(directories, files, path_ob);

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<FileInfo> getFileList(String dir) {

    // Invalidation check,
    checkInvalidated();

    checkValidDBPath(dir);

    ODBTransaction t = getTransaction();

    // Traverse to the directory entry,
    ODBObject path_ob = traverseToDirectory(t, dir);
    if (path_ob == null) {
      // Returns null if not found,
      return null;
    }

    // The list of files,
    ODBList files = path_ob.getList("files");
    return new FileInfoList(files, path_ob, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<FileInfo> getSubDirectoryList(String dir) {

    // Invalidation check,
    checkInvalidated();

    checkValidDBPath(dir);

    ODBTransaction t = getTransaction();

    // Traverse to the directory entry,
    ODBObject path_ob = traverseToDirectory(t, dir);
    if (path_ob == null) {
      // Returns null if not found,
      return null;
    }

    // The list of sub-directories,
    ODBList dirs = path_ob.getList("directories");
    return new FileInfoList(dirs, path_ob, false);
  }

  /**
   * Returns information about the given file object (file or directory)
   * stored in the repository, or null if the file isn't found.
   */
  @Override
  public FileInfo getFileInfo(String item_name) {

    // Invalidation check,
    checkInvalidated();

    // Special case, handle the root,
    if (item_name.equals("/")) {
      return new FRFileInfo("/", "$dir", 0, null);
    }

    String parent_path;
    String child_item;

    String meta_string;
    ODBObject file_ob;
    ODBTransaction t = getTransaction();

    if (item_name.endsWith("/")) {
      // It's a directory,
      checkValidDBPath(item_name);

      // Traverse to the directory entry,
      ODBObject path_ob = traverseToDirectory(t, item_name);
      if (path_ob == null) {
        return null;
      }
      // Get the meta string,
      meta_string = path_ob.getString("meta");
      file_ob = null;

    }
    else {
      // It's a file,
      // Check the remote_file is valid,
      checkValidDBFile(item_name);

      ODBObject path_ob;

      // Go to the parent and fetch the child meta,
      int p = item_name.lastIndexOf('/', item_name.length() - 1);
      parent_path = item_name.substring(0, p + 1);
      child_item = item_name.substring(p + 1);

      // Traverse to the directory entry,
      path_ob = traverseToDirectory(t, parent_path);
      if (path_ob == null) {
        return null;
      }

      // Get the list of files in the directory,
      ODBList file_list = path_ob.getList("files");
      file_ob = file_list.getObject(child_item);

      // If the file doesn't exist,
      if (file_ob == null) {
        return null;
      }

      // Get the meta string,
      meta_string = file_ob.getString("meta");

    }

    // Create the file info object from the meta string,
    return createFileInfo(item_name, file_ob, meta_string);

  }

  /**
   * Returns a DataFile of the contents of the given file name.
   */
  @Override
  public DataFile getDataFile(String file_name) {

    // Invalidation check,
    checkInvalidated();

    // Check the file name is valid,
    checkValidDBFile(file_name);

    ODBTransaction t = getTransaction();

    // Go to the parent and fetch the child meta,
    int p = file_name.lastIndexOf('/', file_name.length() - 1);
    String parent_path = file_name.substring(0, p + 1);
    String child_item = file_name.substring(p + 1);

    // Traverse to the directory of the file,
    ODBObject path_ob = traverseToDirectory(t, parent_path);
    if (path_ob == null) {
      return null;
    }

    // Get the list of files in the directory,
    ODBList file_list = path_ob.getList("files");
    ODBObject file_ob = file_list.getObject(child_item);

    // If the file doesn't exist,
    if (file_ob == null) {
      return null;
    }

    return secureGetFile(t, file_ob, file_name);
  }

  @Override
  public void renameDirectory(String path_name_current, String path_name_new) {

    // Invalidation check,
    checkInvalidated();

    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   * Renames a file object. This method will rename the file only - the
   * paths must be the same. For example, '/from/path/file.txt' can not be
   * renamed to '/to/path/file.txt'.
   */
  @Override
  public void renameFile(String file_name_current, String file_name_new) {

    // Invalidation check,
    checkInvalidated();

    // Check the file names are valid,
    checkValidDBFile(file_name_current);
    checkValidDBFile(file_name_new);

    // Go to the parent and fetch the child meta,
    // Current
    int p = file_name_current.lastIndexOf('/', file_name_current.length() - 1);
    String parent_path_cur = file_name_current.substring(0, p + 1);
    String child_item_cur = file_name_current.substring(p + 1);

    // New
    p = file_name_new.lastIndexOf('/', file_name_new.length() - 1);
    String parent_path_new = file_name_new.substring(0, p + 1);
    String child_item_new = file_name_new.substring(p + 1);

    // Paths must be the same,
    if (!parent_path_cur.equals(parent_path_new)) {
      throw new FileSystemException(
              "Paths in specification are different: {0}, {1}",
              file_name_current, file_name_new);
    }

    String parent_path = parent_path_cur;

    ODBTransaction t = getTransaction();

    // Traverse to the directory of the file,
    ODBObject path_ob = traverseToDirectory(t, parent_path);
    if (path_ob == null) {
      throw new FileSystemException("Path not found: {0}", parent_path);
    }

    // Get the list of files in the directory,
    ODBList file_list = path_ob.getList("files");
    ODBObject file_cur_ob = file_list.getObject(child_item_cur);
    ODBObject file_new_ob = file_list.getObject(child_item_new);

    // Source file must exist and destination file must not exist,
    if (file_cur_ob == null) {
      throw new FileSystemException("File not found: {0}", file_name_current);
    }
    if (file_new_ob != null) {
      throw new FileSystemException(
                     "Rename destination already exists: {0}", file_name_new);
    }

    // Get the meta and content data from the current file object,
    String meta = file_cur_ob.getString("meta");
    ODBData content = file_cur_ob.getData("content");

    // Make a new file object,
    file_new_ob = t.constructObject(t.findClass("FS.File"),
                                    child_item_new, meta, null);
    file_new_ob.getData("content").replicateFrom(content);

    // Remove the old entry and put in the new,
    file_list.remove(child_item_cur);
    file_list.add(file_new_ob);

    // Delete the content from the original file,
    content.delete();

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void copyFile(String file_name_source, String file_name_dest) {

    // Invalidation check,
    checkInvalidated();

    // Get the file info source and destination,
    FileInfo finfo_src = getFileInfo(file_name_source);
    FileInfo finfo_dst = getFileInfo(file_name_dest);

    // Get the source file
    if (finfo_src == null) {
      throw new FileSystemException("File not found: {0}", file_name_source);
    }
    if (finfo_dst != null) {
      throw new FileSystemException("Copy destination already exists: {0}",
                                    file_name_dest);
    }

    // Create the destination file with the same file details as the source,
    createFile(file_name_dest,
               finfo_src.getMimeType(), finfo_src.getLastModified());
    // Get the source contents,
    DataFile src_data = finfo_src.getDataFile();
    // The destination,
    DataFile dst_data = getDataFile(file_name_dest);

    // Copy the contents,
//    src_data.copyTo(dst_data, src_data.size());
    dst_data.replicateFrom(src_data);
  }

  /**
   * Returns true if the given File does NOT contain path entries '..' and '.'.
   */
  public boolean checkFileValid(File f) {

    // Invalidation check,
    checkInvalidated();

    File p = f.getParentFile();
    while (p != null) {
      if (p.getName().equals("..")) {
        return false;
      }
      else if (p.getName().equals(".")) {
        return false;
      }
      p = p.getParentFile();
    }
    return true;
  }

  /**
   * Recursively descends through a directory hierarchy uploading any files
   * it finds to the remote location.
   */
  public void uploadFromLocalDirectoryTo(PrintStream msg_out,
                           File local_dir,
                           String repository_path) throws IOException {

    // Invalidation check,
    checkInvalidated();

//    String remote_file_path = "/apps/" + app_context_name + "/";

    recurseUpload(msg_out, local_dir, repository_path); //remote_file_path);

  }



  private void recurseUpload(PrintStream msg_out,
                            File local_dir,
                            String remote_file_path) throws IOException {

    File[] files = local_dir.listFiles();
    for (File f : files) {

      if (f.isDirectory()) {
        recurseUpload(msg_out, f,
                      remote_file_path + f.getName() + "/");
      }
      else {
        // Upload the file,
        // Check it doesn't escape from the current directory,
        if (!checkFileValid(f)) {
          throw new FileSystemException("Invalid file: {0}", f.getName());
        }

        String file_name = f.getName();

        // Find the mime_type for the given file name
        String mime_type = FileUtilities.findMimeType(file_name);
        // Get the size of the entry,
        long size = f.length();
        // The modified type of the entry,
        long modified_time = f.lastModified();

        // The remote file name,
        String remote_file_name = remote_file_path + file_name;

        // The input stream of the file
        InputStream fin = new BufferedInputStream(new FileInputStream(f));

        // Sync the file,
        boolean synched =
                synchronizeFile(fin, size, modified_time, mime_type,
                                remote_file_name);

        if (msg_out != null) {
          if (synched) {
            msg_out.println("WROTE: " + remote_file_name);
          }
          else {
            msg_out.println("SKIPPED: " + remote_file_name);
          }
        }

      }
    }

  }

  /**
   * Uploads a .war formatted InputStream into the repository. If the .war
   * file doesn't exist in the repository then it is copied. If the .war
   * file does exist, a binary comparison is performed between the data on the
   * input stream and currently stored, and only the changed files are
   * uploaded.
   */
  public void uploadWarApp(PrintStream msg_out,
                           InputStream war_stream,
                           String app_context_name) throws IOException {

    // Invalidation check,
    checkInvalidated();

//    FileNameMap fileNameMap = URLConnection.getFileNameMap();
    // As a .JAR stream,
    JarInputStream jin = new JarInputStream(war_stream);
    JarEntry entry = jin.getNextJarEntry();

    while (entry != null) {
      String file_name = entry.getName();

      // Ignore any entries ending in '/' (these are directory entries).
      if (!file_name.endsWith("/")) {
        // Turn it into a file object,
        File f = new File(file_name);
        // Check it doesn't escape from the current directory,
        if (!checkFileValid(f)) {
          throw new FileSystemException("Invalid file: {0}", f.getName());
        }

        // Find the mime_type for the given file name
//        String mime_type = fileNameMap.getContentTypeFor(file_name);
        String mime_type = FileUtilities.findMimeType(file_name);
        // Get the size of the entry,
        long size = entry.getSize();
        // The modified type of the entry,
        long modified_time = entry.getTime();

        // The remote file name,
        String remote_file_name = "/apps/" + app_context_name + "/" + file_name;

        // Sync the file,
        boolean synched =
                synchronizeFile(jin, size, modified_time, mime_type,
                                remote_file_name);

        if (msg_out != null) {
          if (synched) {
            msg_out.println("WROTE: " + remote_file_name);
          }
          else {
            msg_out.println("SKIPPED: " + remote_file_name);
          }
        }

      }

      entry = jin.getNextJarEntry();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void commit() throws CommitFaultException {
    checkInvalidated();
    invalidate();
    transaction.commit();
  }


  // ----- Inner classes -----





  /**
   * The lazily created file info list. Note that this is
   * implemented in such a way that it inherits the security of the backed
   * file repository.
   */
  private class FileInfoList extends AbstractList<FileInfo>
                                                      implements RandomAccess {

    /**
     * The list object.
     */
    private final ODBList list;

    /**
     * The path object.
     */
    private final ODBObject path_ob;

    /**
     * True if the list contains file objects, false if it contains directory
     * objects.
     */
    private final boolean contains_files;

    /**
     * Constructor.
     */
    private FileInfoList(ODBList list,
                         ODBObject path_ob, boolean contains_files) {
      this.list = list;
      this.path_ob = path_ob;
      this.contains_files = contains_files;
    }

    @Override
    public FileInfo get(int index) {
      // Delegate to a secure method in the parent class
      return secureGetFileListItem(list, index, path_ob, contains_files);
    }

    @Override
    public int size() {
      // Delegate to a secure method in the parent class
      return secureGetFileListSize(list);
    }

  }

  /**
   * The lazily created directory and file info list. Note that this is
   * implemented in such a way that it inherits the security of the backed
   * file repository.
   */
  private class DirectoryAndFileInfoList extends AbstractList<FileInfo>
                                                      implements RandomAccess {

    /**
     * The list object.
     */
    private final ODBList dir_list;
    private final ODBList file_list;

    /**
     * The path object.
     */
    private final ODBObject path_ob;

    /**
     * Constructor.
     */
    private DirectoryAndFileInfoList(ODBList dir_list, ODBList file_list,
                                     ODBObject path_ob) {
      this.dir_list = dir_list;
      this.file_list = file_list;
      this.path_ob = path_ob;
    }

    @Override
    public FileInfo get(int index) {
      int dir_size = secureGetFileListSize(dir_list);

      // Directories
      if (index < dir_size) {
        // Delegate to a secure method in the parent class
        return
            secureGetFileListItem(dir_list, index, path_ob, false);
      }
      else {
        // Delegate to a secure method in the parent class
        return
            secureGetFileListItem(file_list, index - dir_size, path_ob, true);
      }

    }

    @Override
    public int size() {
      // Delegate to a secure method in the parent class
      return secureGetFileListSize(dir_list) +
             secureGetFileListSize(file_list);
    }

  }

  private class FRFileInfo implements FileInfo {

    /**
     * The name of the file.
     */
    private final String name;

    /**
     * The mime type of the file.
     */
    private String mime_type;

    /**
     * The last modified timestamp of the file.
     */
    private long last_modified;

    /**
     * The backed ODB file object.
     */
    private final ODBObject item_ob;

    /**
     * Constructor.
     */
    FRFileInfo(String name,
               String mime_type, long last_modified, ODBObject item_ob) {
      this.name = name;
      this.mime_type = mime_type;
      this.last_modified = last_modified;
      this.item_ob = item_ob;
    }

    /**
     * Returns the full absolute name of this file with respect to the root
     * directory.
     */
    @Override
    public String getAbsoluteName() {
      return name;
    }

    @Override
    public String getMimeType() {
      return mime_type;
    }

    @Override
    public long getLastModified() {
      return last_modified;
    }

    /**
     * Returns true if this file item represents a directory.
     */
    @Override
    public boolean isDirectory() {
      return mime_type != null && mime_type.equals("$dir");
    }

    /**
     * Returns true if this file item represents a file.
     */
    @Override
    public boolean isFile() {
      return !isDirectory();
    }

    /**
     * Returns the file name without the path.
     */
    @Override
    public String getItemName() {
      int p = name.lastIndexOf("/", name.length() - 2);
      return name.substring(p + 1);
    }

    /**
     * Returns the path name without the file name.
     */
    @Override
    public String getPathName() {
      int p = name.lastIndexOf("/");
      return name.substring(0, p);
    }






    @Override
    public DataFile getDataFile() {
      if (item_ob == null) {
        throw new FileSystemException(
                              "getDataFile not supported for this file type");
      }
      // Quickly construct the DataFile from the item object,
      return secureGetFile(getTransaction(), item_ob, getAbsoluteName());
    }

    @Override
    public void setLastModified(long last_modified) {
      if (item_ob == null) {
        throw new FileSystemException(
                          "getLastModified not supported for this file type");
      }
      secureTouchFile(item_ob, last_modified);
      this.last_modified = last_modified;
    }

    @Override
    public void setMimeType(String mime_type) {
      if (item_ob == null) {
        throw new FileSystemException(
                              "getMimeType not supported for this file type");
      }
      secureSetMimeType(item_ob, mime_type);
      this.mime_type = mime_type;
    }

  }

}
