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

import java.text.MessageFormat;

/**
 * A runtime exception thrown when a file repository operation fails.
 *
 * @author Tobias Downer
 */

public class FileSystemException extends RuntimeException {

  public FileSystemException(String msg) {
    super(MessageFormat.format(msg, new Object[0]));
  }

  public FileSystemException(Throwable e) {
    super(e);
  }

  public FileSystemException(String msg, Throwable e) {
    super(MessageFormat.format(msg, new Object[0]), e);
  }

  public FileSystemException(String msg, Object params) {
    this(MessageFormat.format(msg, new Object[] { params }));
  }
  public FileSystemException(String msg, Object param1, Object param2) {
    this(MessageFormat.format(msg, new Object[] { param1, param2 }));
  }
  public FileSystemException(String msg, Object param1, Object param2, Object param3) {
    this(MessageFormat.format(msg, new Object[] { param1, param2, param3 }));
  }

  public FileSystemException(String msg, Object params, Throwable e) {
    this(MessageFormat.format(msg, new Object[] { params }), e);
  }
  public FileSystemException(String msg, Object param1, Object param2, Throwable e) {
    this(MessageFormat.format(msg, new Object[] { param1, param2 }), e);
  }
  public FileSystemException(String msg, Object param1, Object param2, Object param3, Throwable e) {
    this(MessageFormat.format(msg, new Object[] { param1, param2, param3 }), e);
  }

  public FileSystemException(String msg, Object[] params) {
    this(MessageFormat.format(msg, params));
  }
  public FileSystemException(String msg, Object[] params, Throwable e) {
    super(MessageFormat.format(msg, params), e);
  }

}
