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

import java.net.FileNameMap;
import java.net.URLConnection;
import java.util.HashMap;

/**
 * Various static utility methods for handling file objects.
 *
 * @author Tobias Downer
 */

public class FileUtilities {

  private static final HashMap<String, String> mime_map = new HashMap();
  static {

    // List created from various open sources on the web

    mime_map.put("jar", "application/java-archive");
    mime_map.put("war", "application/x-zip");
    mime_map.put("zip", "application/x-zip");
    mime_map.put("bz2", "application/x-bzip2");
    mime_map.put("tar", "application/x-tar");
    mime_map.put("gz", "application/x-gzip");
    mime_map.put("tgz", "application/x-gzip");
    mime_map.put("rar", "application/x-rar-compressed");

    mime_map.put("mp3", "audio/mp3");
    mime_map.put("m4a", "audio/mp4");
    mime_map.put("aac", "audio/mp4");
    mime_map.put("aiff", "audio/x-aiff");
    mime_map.put("aif", "audio/x-aiff");
    mime_map.put("aifc", "audio/x-aiff");
    mime_map.put("wav", "audio/x-wav");
    mime_map.put("midi", "audio/x-midi");
    mime_map.put("mid", "audio/x-midi");

    mime_map.put("application/x-iso-image", "iso");

    mime_map.put("jnlp", "x-java-jnlp-file");
    mime_map.put("class", "application/java-vm");
    mime_map.put("exe", "application/octet-stream");

    mime_map.put("gif", "image/gif");
    mime_map.put("jpg", "image/jpeg");
    mime_map.put("jpeg", "image/jpeg");
    mime_map.put("jfif", "image/jpeg");
    mime_map.put("jpe", "image/jpeg");
    mime_map.put("png", "image/png");
    mime_map.put("tif", "image/tiff");
    mime_map.put("tiff", "image/tiff");
    mime_map.put("bmp", "image/x-bmp");
    mime_map.put("psd", "image/x-photoshop");

    mime_map.put("rss", "application/rss+xml");
    mime_map.put("atom", "application/atom+xml");
    mime_map.put("ser", "application/x-java-serialized-object");
    mime_map.put("swf", "application/x-shockwave-flash");
    mime_map.put("svg", "image/svg+xml");
    mime_map.put("xml", "application/xml");
    mime_map.put("dtd", "application/xml-dtd");
    mime_map.put("ps", "application/postscript");
    mime_map.put("torrent", "application/x-bittorrent");
    mime_map.put("pdf", "application/pdf");
    mime_map.put("ps", "application/postscript");
    mime_map.put("eps", "application/postscript");
    mime_map.put("ai", "application/postscript");
    mime_map.put("ra", "audio/x-realaudio");

    mime_map.put("mov", "video/quicktime");
    mime_map.put("qt", "video/quicktime");
    mime_map.put("avi", "video/x-msvideo");
    mime_map.put("mpeg", "video/mpeg");
    mime_map.put("mpg", "video/mpeg");
    mime_map.put("mpe", "video/mpeg");
    mime_map.put("mp4", "video/mp4");
    mime_map.put("m4v", "video/mp4");
    mime_map.put("ogg", "application/x-ogg");
    mime_map.put("ogm", "application/x-ogg");
    mime_map.put("flac", "audio/x-flac");

    mime_map.put("otf", "font/opentype");
    mime_map.put("ttf", "application/x-font-ttf");
    mime_map.put("ttc", "application/x-font-ttf");
    mime_map.put("pfb", "application/x-font-type1");
    mime_map.put("pfa", "application/x-font-type1");

    mime_map.put("doc", "application/msword");
    mime_map.put("xls", "application/vnd.ms-excel");
    mime_map.put("ppt", "application/vnd.ms-powerpoint");

    // Text types,
    mime_map.put("css", "text/css");
    mime_map.put("jad", "text/vnd.sun.j2me.app-descriptor");
    mime_map.put("txt", "text/plain");
    mime_map.put("asc", "text/plain");
    mime_map.put("bas", "text/plain");
    mime_map.put("bat", "text/plain");
    mime_map.put("cmd", "text/plain");
    mime_map.put("csv", "text/comma-separated-values");
    mime_map.put("dss", "text/dss");
    mime_map.put("htm", "text/html");
    mime_map.put("html", "text/html");
    mime_map.put("xhtml", "text/html");
    mime_map.put("xhtm", "text/html");
    mime_map.put("shtml", "text/html");
    mime_map.put("shtm", "text/html");
    mime_map.put("lsp", "text/lsp");
    mime_map.put("rtf", "text/rtf");
    mime_map.put("sgm", "text/sgml");
    mime_map.put("sgml", "text/sgml");
    mime_map.put("sql", "text/plain");
    mime_map.put("text", "text/plain");
    mime_map.put("tsv", "text/tab-separated-values");
    mime_map.put("txt", "text/plain");
    mime_map.put("vb", "text/vbscript");

    mime_map.put("tex", "text/tex");
    mime_map.put("latex", "text/tex");
    mime_map.put("ltx", "text/tex");
    mime_map.put("texi", "text/tex");
    mime_map.put("ctx", "text/tex");

    // Web application source code types,
    mime_map.put("java", "text/x-java-source");
    mime_map.put("js", "application/x-javascript");
    mime_map.put("jsp", "text/plain");
    mime_map.put("scala", "text/plain");
    mime_map.put("groovy", "text/plain");
    mime_map.put("rb", "text/plain");
    mime_map.put("php", "application/x-php");
    mime_map.put("php3", "application/x-php");
    mime_map.put("php4", "application/x-php");
    mime_map.put("phtml", "application/x-php");
    mime_map.put("py", "text/x-python");
    mime_map.put("c", "text/x-c-code");
    mime_map.put("cpp", "text/x-c++-code");
    mime_map.put("cp", "text/x-c++-code");
    mime_map.put("c++", "text/x-c++-code");
    mime_map.put("cc", "text/x-c++-code");
    mime_map.put("h", "text/x-c-header");

  }

  /**
   * Given a file name, guesses an appropriate mime type for the file.
   */
  public static String findMimeType(String file_name) {
    file_name = file_name.trim();

    FileNameMap fileNameMap = URLConnection.getFileNameMap();
    String mime_type = fileNameMap.getContentTypeFor(file_name);

    // If a mime type not found in the internal mime resolution method, we
    // do discovery on our own.
    if (mime_type == null || mime_type.length() == 0) {
      int name_break = file_name.lastIndexOf('/');
      if (name_break < 0) name_break = 0;
      String name = file_name.substring(name_break);
      int ext_delim = name.lastIndexOf('.');
      if (ext_delim >= 0) {
        String ext = name.substring(ext_delim + 1);
        mime_type = mime_map.get(ext);
      }
      else {
        // No extension on this filename, so mime type is default
      }
      // If still no mime-type found, so use default,
      if (mime_type == null) {
        mime_type = "application/octet-stream";
      }
    }

    return mime_type;
  }

}
