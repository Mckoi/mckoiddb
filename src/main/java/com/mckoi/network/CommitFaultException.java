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

package com.mckoi.network;

import java.text.MessageFormat;

/**
 * An exception that indicates changes made by concurrent transactions are in
 * conflict and a consistent model of a proposed change can not be published.
 * For example, two transactions deleting the same row in a table would cause
 * a CommitFaultException to be thrown.
 *
 * @author Tobias Downer
 */

public class CommitFaultException extends Exception {

  /**
   * Constructs the commit fault exception with a message. The message should
   * describe the reason for the fault in a way a human could understand.
   */
  public CommitFaultException(String msg) {
    super(msg);
  }

  /**
   * Constructs the commit fault exception with a formatted message and
   * arguments. The message should describe the reason for the fault in a way
   * a human could understand.
   */
  public CommitFaultException(String msg, Object ... args) {
    this(MessageFormat.format(msg, args));
  }

//  private static String format(String msg, Object[] args) {
//
//
//    for (int i = 0; i < args.length; ++i) {
//      msg = msg.replace("%" + i, args[i].toString());
//    }
//    return msg;
//  }

}
