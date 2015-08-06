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

package com.mckoi.odb.util;

import com.mckoi.data.DataFile;
import com.mckoi.data.DataFileUtils;
import com.mckoi.util.StyledPrintWriter;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.mckoi.odb.util.DirectorySynchronizerFeedback.*;

/**
 * A utility that can synchronize a hierarchy of files with an ODB file
 * system. The source location of the files is implementation specific (the
 * source may be a local file system, a ZIP archive, or some other source).
 *
 * @author Tobias Downer
 */

public class DirectorySynchronizer {

  /**
   * The list of directories in the source not to include.
   */
  private Set<String> skip_directories = null;

  /**
   * True if the synchronize method should delete files that don't exist in
   * the originating copy (default is true).
   */
  private boolean delete_files = true;

  /**
   * The directory synchronizer feedback object.
   */
  private final DirectorySynchronizerFeedback feedback;
  
  /**
   * The source repository.
   */
  private final SynchronizerRepository source_rep;

  /**
   * The destination repository.
   */
  private final SynchronizerRepository dest_rep;


  public DirectorySynchronizer(DirectorySynchronizerFeedback feedback,
                               SynchronizerRepository source,
                               SynchronizerRepository dest) {
    this.feedback = (feedback == null) ? new StyledDSFeedback(null) : feedback;
    this.source_rep = source;
    this.dest_rep = dest;
  }

  public DirectorySynchronizer(StyledPrintWriter out,
                               SynchronizerRepository source,
                               SynchronizerRepository dest) {
    this(new StyledDSFeedback(out), source, dest);
  }

  /**
   * Adds a directory to skip in the synchronization. (eg. '/META-INF/')
   * 
   * @param path
   */
  public void addPathToSkip(String path) {
    if (skip_directories == null) {
      skip_directories = new HashSet();
    }
    skip_directories.add(path);
  }

  /**
   * Sets the 'delete_files' flag which causes the 'synchronize' method
   * to delete files and directories. By default, this is set to true so set
   * to false if you don't want anything deleted.
   * 
   * @param status
   */
  public void setDeleteFilesFlag(boolean status) {
    delete_files = status;
  }

  /**
   * Synchronizes the source directory hierarchy with the destination
   * location. The status of the synchronize is output to the print stream.
   * <p>
   * Returns the number of updates performed. If 0 is returned then no
   * changes were made to the destination repository.
   * 
   * @return 
   * @throws java.io.IOException
   */
  public long synchronize() throws IOException {
    return recursiveSynchronizeDir("/");
  }

  /**
   * Synchronizes the source directory hierarchy with the destination
   * location without any feedback generated.
   * 
   * @param source the source repository abstraction.
   * @param dest the destination repository abstraction.
   * @return 
   */
  public static DirectorySynchronizer getSynchronizer(
                          SynchronizerRepository source,
                          SynchronizerRepository dest) {

    return new DirectorySynchronizer((StyledPrintWriter) null, source, dest);

  }

  /**
   * Synchronizes the directory in the local file system with the given
   * Mckoi file system.
   * 
   * @param fb the writer to output the feedback.
   * @param local_path the local path in the local file system.
   * @param mckoi_file_sys the Mckoi file system.
   * @param mckoi_path the path in the mckoi file system.
   * @return 
   */
  public static DirectorySynchronizer getJavaToMckoiSynchronizer(
                       DirectorySynchronizerFeedback fb, File local_path,
                       FileSystem mckoi_file_sys, String mckoi_path) {

    return new DirectorySynchronizer(fb,
                         new JavaRepository(local_path),
                         new MckoiRepository(mckoi_file_sys, mckoi_path));

  }

  /**
   * Synchronizes the directory in the Mckoi file system with the local file
   * system.
   * 
   * @param fb the writer to output the feedback.
   * @param mckoi_file_sys the Mckoi file system.
   * @param mckoi_path the path in the mckoi file system.
   * @param local_path the local path in the local file system.
   * @return 
   */
  public static DirectorySynchronizer getMckoiToJavaSynchronizer(
                       DirectorySynchronizerFeedback fb,
                       FileSystem mckoi_file_sys, String mckoi_path,
                       File local_path) {

    return new DirectorySynchronizer(fb,
                         new MckoiRepository(mckoi_file_sys, mckoi_path),
                         new JavaRepository(local_path) );

  }

  /**
   * Synchronizes the Mckoi file system with the given Mckoi file system.
   * 
   * @param fb the writer to output the feedback.
   * @param mckoi_src_filesys the source Mckoi file system.
   * @param mckoi_src_path the source Mckoi path.
   * @param mckoi_dst_filesys the destination Mckoi file system.
   * @param mckoi_dst_path the destination Mckoi path
   * @return 
   */
  public static DirectorySynchronizer getMckoiToMckoiSynchronizer(
                       DirectorySynchronizerFeedback fb,
                       FileSystem mckoi_src_filesys, String mckoi_src_path,
                       FileSystem mckoi_dst_filesys, String mckoi_dst_path) {

    return new DirectorySynchronizer(fb,
                       new MckoiRepository(mckoi_src_filesys, mckoi_src_path),
                       new MckoiRepository(mckoi_dst_filesys, mckoi_dst_path));

  }

  /**
   * Synchronizes a Zip file with the given Mckoi file system.
   * <p>
   * Returns the number of updates performed. If 0 is returned then no
   * changes were made to the destination repository.
   * 
   * @param fb the writer to output the feedback.
   * @param zip_file the zip file source.
   * @param mckoi_dst_filesys the destination Mckoi file system.
   * @param mckoi_dst_path the destination Mckoi path
   * @return 
   * @throws java.io.IOException 
   */
  public static DirectorySynchronizer getZipToMckoiSynchronizer(
                       DirectorySynchronizerFeedback fb, File zip_file,
                       FileSystem mckoi_dst_filesys, String mckoi_dst_path)
                                                           throws IOException {

    ZipRepository zip_repository = new ZipRepository(zip_file);
    zip_repository.init();

    return new DirectorySynchronizer(fb,
                       zip_repository,
                       new MckoiRepository(mckoi_dst_filesys, mckoi_dst_path));

  }



  /**
   * Calculates the SHA-256 hash of the data in the given input stream.
   * 
   * @param ins the input stream to hash.
   * @return 
   * @throws java.io.IOException
   */
  public static byte[] calcHash(InputStream ins) throws IOException {
    // Get SHA digest,
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    }
    catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    // Find the hash,
    byte[] buf = new byte[1024];
    while (true) {
      int read = ins.read(buf, 0, buf.length);
      if (read < 0) {
        break;
      }
      digest.update(buf, 0, read);
    }
    // Return it,
    return digest.digest();
  }

  /**
   * Makes a copy of the list into an ArrayList.
   */
  private static <T> List<T> copyOf(List<T> list) {
    ArrayList<T> dest = new ArrayList(list.size());
    dest.addAll(list);
    return dest;
  }

  /**
   * Recursively deletes the given directory in the file system and all
   * sub-directories and files.
   */
  private static void recursiveRemoveDir(FileSystem file_sys, String dir) {
    // For each of the source files,
    List<FileInfo> source_files = copyOf(file_sys.getFileList(dir));
    for (FileInfo source_file : source_files) {
      file_sys.deleteFile(source_file.getAbsoluteName());
    }
    // Recurse on the sub-directories,
    List<FileInfo> source_sub_dirs = copyOf(file_sys.getSubDirectoryList(dir));
    for (FileInfo source_sub_dir : source_sub_dirs) {
      String sub_dir = source_sub_dir.getAbsoluteName();
      recursiveRemoveDir(file_sys, sub_dir);
    }
    // Remove this directory after it has been cleared,
    file_sys.removeDirectory(dir);
  }

  /**
   * Recursively synchronizes the files from the source directory into the
   * destination directory. The destination directory may contain the same
   * files, in which case the file is only updated if it's different.
   * <p>
   * Returns the number of files updated in the new version.
   * <p>
   * Writes what happened to the PrintWriter if 'out' is not null.
   */
  private long recursiveSynchronizeDir(final String sync_dir)
                                                           throws IOException {

    // Any directories we are skipping,
    if (skip_directories != null) {
      if (skip_directories.contains(sync_dir)) {
        return 0;
      }
    }

    long running_update_count = 0;

    // The list of touched file names in this directory,
    List<String> touched_files = new ArrayList();
    List<String> touched_dirs = new ArrayList();

    // Make the destination directory if we need to,
    if (!dest_rep.hasDirectory(sync_dir)) {
      dest_rep.makeDirectory(sync_dir);
      ++running_update_count;
    }

    // Recurse on directories,
    List<String> source_dirs = source_rep.allSubDirectories(sync_dir);
    for (String source_dir : source_dirs) {
      touched_dirs.add(source_dir);
      String new_path = sync_dir + source_dir;
      long update_count = recursiveSynchronizeDir(new_path);
      running_update_count += update_count;
    }

    // Recurse on source files,
    List<SynchronizerFile> source_files = source_rep.allFiles(sync_dir);
    for (SynchronizerFile source_file : source_files) {
      String source_file_name = source_file.getName();
      touched_files.add(source_file_name);
      String absolute_dest_name = sync_dir + source_file_name;

      boolean write_new_file = true;
      boolean write_timestamp = false;
      SynchronizerFile dest_fo =
                            dest_rep.getFileObject(sync_dir, source_file_name);
      // Create it if it doesn't exist
      if (!dest_fo.exists()) {
        dest_fo.create();
      }

      // Is the file different?
      long src_size = source_file.getSize();
      long dst_size = dest_fo.getSize();
      long source_ts = source_file.getTimestamp();
      long dest_ts = dest_fo.getTimestamp();

      // If size is same, compare hashes,
      if (src_size == dst_size) {
        // Calculate hashes of the files,
        byte[] src_hash = source_file.getSHAHash();
        byte[] dst_hash = dest_fo.getSHAHash();
        // If the hash is the same, the files are not different
        if (Arrays.equals(src_hash, dst_hash)) {
          // Size and hash the same, are the timestamps different?
          if (source_ts < dest_ts - 500 || source_ts > dest_ts + 500) {
            // Yes
            write_timestamp = true;
          }
          write_new_file = false;
        }
      }
      // If the file content is different then rewrite the destination file,
      if (write_new_file) {
        feedback.reportFileSynchronized(WRITTEN, absolute_dest_name, src_size);
        dest_rep.writeCopyOf(source_file, sync_dir);
        ++running_update_count;
      }
      // If the timestamp is different then update the timestamp,
      else if (write_timestamp) {
        feedback.reportFileSynchronized(TOUCHED, absolute_dest_name, dst_size);
        // Update the timestamp,
        dest_fo.setTimestamp(source_ts);
        ++running_update_count;
      }
      // Otherwise no change necessary,
      else {
        feedback.reportFileSynchronized(SKIPPED, absolute_dest_name, dst_size);
      }
    }

    // Do we delete files that aren't in the source?
    if (delete_files) {
      // Sort the touched files list,
      Collections.sort(touched_files);
      Collections.sort(touched_dirs);

      // Any files in the destination that aren't touched we delete,
      List<SynchronizerFile> dest_files = dest_rep.allFiles(sync_dir);
      for (SynchronizerFile dest_file : dest_files) {
        String item_name = dest_file.getName();
        int pos = Collections.binarySearch(touched_files, item_name);
        if (pos < 0) {
          String file_abs_name = sync_dir + item_name;
          // Not in the list, so delete this,
          feedback.reportFileSynchronized(DELETED, file_abs_name, 0);
          dest_file.delete();
          ++running_update_count;
        }
      }
      // Any directories in the destination that we didn't touch we delete,
      List<String> dest_dirs = dest_rep.allSubDirectories(sync_dir);
      for (String dir : dest_dirs) {
        int pos = Collections.binarySearch(touched_dirs, dir);
        if (pos < 0) {
          String dir_abs_name = sync_dir + dir;
          // Not in the list, so recursively delete this,
          feedback.reportDirectoryRemoved(dir_abs_name);
          dest_rep.removeDirectory(dir_abs_name);
          ++running_update_count;
        }
      }
    }
    
    return running_update_count;
  }

  // ----- Synchronizer implementations -----

  /**
   * A SynchronizerRepository of a directory hierarchy in the local file
   * system.
   */
  public static class JavaRepository implements SynchronizerRepository {

    private final File local_base_dir_or_file;
    private final boolean is_single;

    public JavaRepository(File local_base_dir_or_file) {
      this.local_base_dir_or_file = local_base_dir_or_file;
      this.is_single = local_base_dir_or_file.isFile();
    }

    private File toFile(String path) {
      if (is_single) {
        throw new RuntimeException("repository is a single object");
      }
      // The path must start with '/', which we remove in this implementation,
      if (!path.startsWith("/")) {
        throw new RuntimeException("path must start with '/'");
      }
      path = path.substring(1);
      File f_path = local_base_dir_or_file;
      if (path.length() > 0) {
        f_path = new File(f_path, path);
      }
      return f_path;
    }
    
    private File[] getFileArray(String path) {
      // All the files from this location,
      File[] file_arr = toFile(path).listFiles();
      return file_arr;
    }
    
    
    @Override
    public List<SynchronizerFile> allFiles(String path) {
      // If it's a single, return the file or empty list,
      if (is_single) {
        if (path.equals("/")) {
          return Collections.singletonList(
                             (SynchronizerFile) new JavaFile(local_base_dir_or_file));
        }
        else {
          return Collections.EMPTY_LIST;
        }
      }
      // Handle the rest of the files,
      List<SynchronizerFile> files_list = new ArrayList();
      File[] files = getFileArray(path);
      for (File f : files) {
        if (f.isFile()) {
          files_list.add(new JavaFile(f));
        }
      }
      return files_list;
    }

    @Override
    public List<String> allSubDirectories(String path) {
      // If it's a single, return empty list,
      if (is_single) {
        return Collections.EMPTY_LIST;
      }
      List<String> dirs_list = new ArrayList();
      File[] files = getFileArray(path);
      for (File f : files) {
        if (f.isDirectory()) {
          dirs_list.add(f.getName() + "/");
        }
      }
      return dirs_list;
    }

    @Override
    public SynchronizerFile getFileObject(String path, String file_name) {
      File f = new File(toFile(path), file_name);
      return new JavaFile(f);
    }

    @Override
    public void writeCopyOf(SynchronizerFile file_ob, String path)
                                                           throws IOException {
      // The destination file,
      SynchronizerFile dest_file_ob = getFileObject(path, file_ob.getName());
      if (!dest_file_ob.exists()) {
        dest_file_ob.create();
      }
      // Bitwise copy,
      InputStream ins = file_ob.getInputStream();
      OutputStream outs = dest_file_ob.getOutputStream();

      byte[] buf = new byte[1024];
      while (true) {
        int read = ins.read(buf, 0, buf.length);
        if (read < 0) {
          break;
        }
        outs.write(buf, 0, read);
      }

      // Touch the copy,
      dest_file_ob.setTimestamp(file_ob.getTimestamp());

      ins.close();
      outs.close();

    }

    @Override
    public boolean hasDirectory(String path) {
      File f = toFile(path);
      if (f.exists() && f.isDirectory()) {
        return true;
      }
      return false;
    }

    @Override
    public void makeDirectory(String path) {
      File f = toFile(path);
      f.mkdirs();
    }

    @Override
    public void removeDirectory(String path) {
      // PENDING, need to recursively remove the path and sub-directories...
    }

  }

  /**
   * An implementation of SynchronizerFile for a java.io.File object.
   */
  public static class JavaFile implements SynchronizerFile {

    private final File file;

    public JavaFile(File file) {
      this.file = file;
    }

    private void checkExists() {
      if (!exists()) {
        throw new RuntimeException("File doesn't exist");
      }
    }
    
    @Override
    public String getName() {
      return file.getName();
    }

    @Override
    public long getSize() {
      checkExists();
      return file.length();
    }

    @Override
    public long getTimestamp() {
      checkExists();
      return file.lastModified();
    }

    @Override
    public void setTimestamp(long timestamp) throws IOException {
      checkExists();
      file.setLastModified(timestamp);
    }

    @Override
    public byte[] getSHAHash() throws IOException {
      checkExists();
      FileInputStream fin = new FileInputStream(file);
      byte[] hash = calcHash(fin);
      fin.close();
      return hash;
    }

    @Override
    public void create() throws IOException {
      if (!exists()) {
        file.createNewFile();
      }
      else {
        throw new RuntimeException("Already exists");
      }
    }
    
    @Override
    public void delete() throws IOException {
      checkExists();
      file.delete();
    }

    @Override
    public boolean exists() {
      return file.exists();
    }

    @Override
    public InputStream getInputStream() throws IOException {
      checkExists();
      return new FileInputStream(file);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      checkExists();
      // Set the size of the file to zero
      RandomAccessFile rac = new RandomAccessFile(file, "rw");
      rac.setLength(0);
      rac.close();
      // Return an output stream to the file,
      return new FileOutputStream(file);
    }

  }

  /**
   * An implementation of SynchronizerRepository for a Mckoi FileSystem
   * implementation.
   */
  public static class MckoiRepository implements SynchronizerRepository {

    private final FileSystem file_sys;
    private final String base_path_or_file;
    private final FileInfo single_file;

    public MckoiRepository(FileSystem file_sys, String base_path_or_file) {
      this.file_sys = file_sys;
      this.base_path_or_file = base_path_or_file;
      FileInfo fi = file_sys.getFileInfo(base_path_or_file);
      single_file = (fi != null && fi.isFile()) ? fi : null;
    }

    private String resolvePath(String path) {
      if (single_file != null) {
        throw new RuntimeException("repository is a single object");
      }
      // The path must start with '/', which we remove in this implementation,
      if (!path.startsWith("/")) {
        throw new RuntimeException("path must start with '/'");
      }
      path = path.substring(1);
      return base_path_or_file + path;
    }

    @Override
    public List<SynchronizerFile> allFiles(String path) {
      // If it's a single, return the file or empty list,
      if (single_file != null) {
        if (path.equals("/")) {
          return Collections.singletonList(
                    (SynchronizerFile) new MckoiFile(
                                  file_sys, base_path_or_file, single_file));
        }
        else {
          return Collections.EMPTY_LIST;
        }
      }
      // Handle the rest of the files,
      List<FileInfo> files = file_sys.getFileList(resolvePath(path));
      List<SynchronizerFile> out_files = new ArrayList(files.size());
      for (FileInfo file : files) {
        out_files.add(new MckoiFile(file_sys, file.getAbsoluteName(), file));
      }
      return out_files;
    }

    @Override
    public List<String> allSubDirectories(String path) {
      // If it's a single, return empty list,
      if (single_file != null) {
        return Collections.EMPTY_LIST;
      }

      List<FileInfo> dirs = file_sys.getSubDirectoryList(resolvePath(path));
      List<String> out_dirs = new ArrayList(dirs.size());
      for (FileInfo dir : dirs) {
        out_dirs.add(dir.getItemName());
      }
      return out_dirs;
    }

    @Override
    public SynchronizerFile getFileObject(String path, String file_name) {
      String absolute_name = resolvePath(path) + file_name;
      FileInfo fi = file_sys.getFileInfo(absolute_name);
      return new MckoiFile(file_sys, absolute_name, fi);
    }

    @Override
    public void writeCopyOf(SynchronizerFile file_ob, String path) throws IOException {
      // The destination file,
      SynchronizerFile dest_file_ob = getFileObject(path, file_ob.getName());
      if (!dest_file_ob.exists()) {
        dest_file_ob.create();
      }
      // Is it a MckoiFile?
      if (file_ob instanceof MckoiFile) {
        // Do a Mckoi copy if source and destination are both Mckoi file
        // systems.
        MckoiFile mckoi_src_file_ob = (MckoiFile) file_ob;
        MckoiFile mckoi_dst_file_ob = (MckoiFile) dest_file_ob;
        DataFile src_df = mckoi_src_file_ob.file_info.getDataFile();
        DataFile dst_df = mckoi_dst_file_ob.file_info.getDataFile();
        // Set destination size to 0
        dst_df.setSize(0);
        dst_df.position(0);
        src_df.position(0);
        // Perform copy
        dst_df.copyFrom(src_df, src_df.size());
      }
      else {
        // Bitwise copy,
        InputStream ins = file_ob.getInputStream();
        OutputStream outs = dest_file_ob.getOutputStream();

        byte[] buf = new byte[1024];
        while (true) {
          int read = ins.read(buf, 0, buf.length);
          if (read < 0) {
            break;
          }
          outs.write(buf, 0, read);
        }
        
        ins.close();
        outs.close();
      }

      // Touch the file,
      dest_file_ob.setTimestamp(file_ob.getTimestamp());

    }

    @Override
    public boolean hasDirectory(String path) {
      FileInfo finfo = file_sys.getFileInfo(resolvePath(path));
      if (finfo != null && finfo.isDirectory()) {
        return true;
      }
      return false;
    }

    @Override
    public void makeDirectory(String path) {
      file_sys.makeDirectory(resolvePath(path));
    }

    @Override
    public void removeDirectory(String path) {
      recursiveRemoveDir(file_sys, resolvePath(path));
    }

  }

  /**
   * An implementation of SynchronizerFile for a Mckoi FileInfo object.
   */
  public static class MckoiFile implements SynchronizerFile {
    
    private final FileSystem file_sys;
    private final String absolute_file_name;
    private FileInfo file_info;

    public MckoiFile(FileSystem file_sys,
                     String absolute_file_name, FileInfo file_info) {
      this.file_sys = file_sys;
      this.absolute_file_name = absolute_file_name;
      this.file_info = file_info;
    }

    @Override
    public String getName() {
      int delim = absolute_file_name.lastIndexOf("/");
      return absolute_file_name.substring(delim + 1);
    }

    @Override
    public long getSize() {
      return file_info.getDataFile().size();
    }

    @Override
    public long getTimestamp() {
      return file_info.getLastModified();
    }

    @Override
    public void setTimestamp(long timestamp) {
      file_info.setLastModified(timestamp);
    }

    @Override
    public byte[] getSHAHash() throws IOException {
      return calcHash(DataFileUtils.asInputStream(file_info.getDataFile()));
    }

    @Override
    public void create() throws IOException {
      if (file_info == null) {
        String mime_type = FileUtilities.findMimeType(absolute_file_name);
        long last_modified = System.currentTimeMillis();
        file_sys.createFile(absolute_file_name, mime_type, last_modified);
        file_info = file_sys.getFileInfo(absolute_file_name);
      }
      else {
        throw new RuntimeException("Already exists");
      }
    }

    @Override
    public void delete() throws IOException {
      file_sys.deleteFile(absolute_file_name);
      file_info = null;
    }

    @Override
    public boolean exists() {
      return (file_info != null);
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return DataFileUtils.asInputStream(file_info.getDataFile());
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      DataFile dfile = file_info.getDataFile();
      dfile.setSize(0);
      dfile.position(0);
      return DataFileUtils.asOutputStream(dfile);
    }

  }

  /**
   * An implementation of SynchronizerRepository for a Zip file.
   */
  public static class ZipRepository implements SynchronizerRepository {

    private final File zip_file_ob;
    private final ZipFile zip_file;
    private List<String> zip_dir_list;
    private List<ZipFileObject> zip_file_list;

    public ZipRepository(File zip_file_ob) throws IOException {
      this.zip_file_ob = zip_file_ob;
      this.zip_file = new ZipFile(zip_file_ob);
    }

    public void init() {
      List<ZipFileObject> file_obs = new ArrayList();
      Set<String> directories = new HashSet();
      directories.add("/");

      // Build the directory table,
      Enumeration<? extends ZipEntry> entries = zip_file.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        String name = entry.getName();
        if (!entry.isDirectory()) {
          // Some zip files don't have directory entries, so we need to make
          // sure all the sub-directories of a file are included,
          String path_str = "/" + name;
          while (true) {
            int delim = path_str.lastIndexOf("/");
            if (delim <= 0) break;
            path_str = path_str.substring(0, delim + 1);
            directories.add(path_str);
            path_str = path_str.substring(0, delim);
          }
          file_obs.add(new ZipFileObject(zip_file, entry));
        }
        else {
          directories.add("/" + name);
        }
      }

      // Add all the directories to a directory list,
      List<String> dir_list = new ArrayList(directories.size());
      dir_list.addAll(directories);
      
      // Sort the file list,
      Collections.sort(file_obs);
      // Sort the directory list,
      Collections.sort(dir_list);

      zip_dir_list = dir_list;
      zip_file_list = file_obs;
    }

    @Override
    public List<SynchronizerFile> allFiles(String path) {
      List<SynchronizerFile> out_list = new ArrayList();
      int p = Collections.binarySearch(zip_file_list, path, ZIP_PATH_COMPARATOR);
      if (p < 0) {
        p = -(p + 1);
      }
      int sz = zip_file_list.size();
      while (p < sz) {
        ZipFileObject f = zip_file_list.get(p);
        String fp = f.getPath();
        if (!fp.startsWith(path)) {
          break;
        }
        if (fp.equals(path)) {
          out_list.add(f);
        }
        ++p;
      }
      return out_list;
    }

    @Override
    public List<String> allSubDirectories(String path) {
//      System.out.println(" subDirs = '" + path + "'");
//      System.out.println(" zip_dir_list = " + zip_dir_list);
      List<String> out_list = new ArrayList();
      int p = Collections.binarySearch(zip_dir_list, path);
      if (p < 0) {
        p = -(p + 1);
      }
      else {
        p = p + 1;
      }
//      System.out.println(" p = " + p);
      int sz = zip_dir_list.size();
      while (p < sz) {
        String d = zip_dir_list.get(p);
        int delim = d.lastIndexOf("/", d.length() - 2);
        String parent_path = d.substring(0, delim + 1);
        if (!parent_path.startsWith(path)) {
          break;
        }
        if (parent_path.equals(path)) {
          out_list.add(d.substring(delim + 1));
        }
        ++p;
      }
//      System.out.println(" out_list = " + out_list);
      return out_list;
    }

    @Override
    public SynchronizerFile getFileObject(String path, String file_name) {
      String absolute_name = path + file_name;
      int p = Collections.binarySearch(zip_file_list,
                                       absolute_name, ZIP_PATH_COMPARATOR);
      if (p >= 0) {
        return zip_file_list.get(p);
      }
      throw new RuntimeException("File not found");
    }

    @Override
    public void writeCopyOf(SynchronizerFile file_ob, String path) throws IOException {
      // Zip files are read-only.
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasDirectory(String path) {
      int p = Collections.binarySearch(zip_dir_list, path);
      return (p >= 0);
    }

    @Override
    public void makeDirectory(String path) {
      // Zip files are read-only.
      throw new UnsupportedOperationException();
    }

    @Override
    public void removeDirectory(String path) {
      // Zip files are read-only.
      throw new UnsupportedOperationException();
    }
    
  }

  public static class ZipFileObject
                       implements SynchronizerFile, Comparable<ZipFileObject> {

    private final ZipFile zip_file;
    private final ZipEntry zip_entry;

    public ZipFileObject(ZipFile zip_file, ZipEntry zip_entry) {
      this.zip_file = zip_file;
      this.zip_entry = zip_entry;
    }

    public String getPath() {
      String name = zip_entry.getName();
      int delim = name.lastIndexOf("/");
      if (delim == -1) {
        return "/";
      }
      else {
        return "/" + name.substring(0, delim + 1);
      }
    }

    @Override
    public String getName() {
      String name = zip_entry.getName();
      int delim = name.lastIndexOf("/");
      if (delim == -1) {
        return name;
      }
      else {
        return name.substring(delim + 1);
      }
    }

    @Override
    public long getSize() {
      return zip_entry.getSize();
    }

    @Override
    public long getTimestamp() {
      return zip_entry.getTime();
    }

    @Override
    public void setTimestamp(long timestamp) {
      // Zip files are read-only.
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getSHAHash() throws IOException {
      return calcHash(zip_file.getInputStream(zip_entry));
    }

    @Override
    public void create() throws IOException {
      // Zip files are read-only.
      throw new UnsupportedOperationException();
    }

    @Override
    public void delete() throws IOException {
      // Zip files are read-only.
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists() {
      return zip_entry != null;
    }

    @Override
    public InputStream getInputStream() throws IOException {
      final InputStream zip_input = zip_file.getInputStream(zip_entry);
      // Delegate InputStream and prevent it from being closed. When a zip
      // file entry is closed it closes all other entries that are open.
      return new InputStream() {
        @Override
        public int read() throws IOException {
          return zip_input.read();
        }
        @Override
        public boolean markSupported() {
          return zip_input.markSupported();
        }
        @Override
        public synchronized void reset() throws IOException {
          zip_input.reset();
        }
        @Override
        public synchronized void mark(int readlimit) {
          zip_input.mark(readlimit);
        }
        @Override
        public void close() throws IOException {
          // We don't want to close the zip when close is called, so this does
          // nothing.
        }
        @Override
        public int available() throws IOException {
          return zip_input.available();
        }
        @Override
        public long skip(long n) throws IOException {
          return zip_input.skip(n);
        }
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          return zip_input.read(b, off, len);
        }
        @Override
        public int read(byte[] b) throws IOException {
          return zip_input.read(b);
        }
      };
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      // Zip files are read-only.
      throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(ZipFileObject o) {
      return zip_entry.getName().compareTo(o.zip_entry.getName());
    }

    @Override
    public String toString() {
      return zip_entry.getName();
    }

  }

  /**
   * Outputs feedback to a styled writer.
   */
  public static class StyledDSFeedback
                                    implements DirectorySynchronizerFeedback {

    private final StyledPrintWriter out;

    public StyledDSFeedback(StyledPrintWriter out) {
      this.out = out;
    }

    @Override
    public void reportFileSynchronized(
                          String sync_type, String file_name, long file_size) {
      if (out != null) {
        switch (sync_type) {
          case SKIPPED:
            out.print("Skipped; ");
            break;
          case TOUCHED:
            out.print("Touched; ");
            break;
          case WRITTEN:
            out.print("Written; ");
            break;
          case DELETED:
            out.print("Deleted; ");
            break;
          default:
            out.print("?: ");
            break;
        }
        out.println(file_name, StyledPrintWriter.INFO);
      }
    }

    @Override
    public void reportDirectoryRemoved(String directory_name) {
      if (out != null) {
        out.print("Deleted Directory; ");
        out.println(directory_name, StyledPrintWriter.INFO);
      }
    }
  
  }

  public final static Comparator ZIP_PATH_COMPARATOR = new Comparator() {
    
    private String getPath(Object o) {
      if (o instanceof String) {
        return (String) o;
      }
      else {
        return "/" + ((ZipFileObject) o).zip_entry.getName();
      }
    }
    
    @Override
    public int compare(Object o1, Object o2) {
      String path1 = getPath(o1);
      String path2 = getPath(o2);
      return path1.compareTo(path2);
    }
  };
  
}
