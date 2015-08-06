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

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * An exception that happened in an external service (eg. an error that
 * occurred during the process of a query on a server).
 *
 * @author Tobias Downer
 */

public class ExternalThrowable {

  /**
   * The class of the thrown exception.
   */
  private String class_name;
  
  /**
   * The message string of the exception.
   */
  private String message;
  
  /**
   * The stack trace of the exception as reported by 'printStackTrace'.
   */
  private String stack_trace;
  
  /**
   * Constructs the external throwable.
   */
  ExternalThrowable(Throwable t) {
    this.class_name = t.getClass().getName();
    this.message = t.getMessage();
    StringWriter w = new StringWriter();
    PrintWriter pw = new PrintWriter(w);
    t.printStackTrace(pw);
    pw.flush();
    this.stack_trace = w.toString();
  }

  /**
   * Full argument constructor.
   */
  ExternalThrowable(String class_name, String message, String stack_trace) {
    this.class_name = class_name;
    this.message = message;
    this.stack_trace = stack_trace;
  }
  
  /**
   * Returns the class of the external exception.
   */
  public String getClassName() {
    return class_name;
  }
  
  /**
   * Returns the external exception message.
   */
  public String getMessage() {
    return message;
  }
  
  /**
   * Returns the external exception stack trace.
   */
  public String getStackTrace() {
    return stack_trace;
  }

  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("[External Throwable: ");
    b.append(getClassName());
    b.append(" message: '");
    b.append(getMessage());
    b.append("']");
    return b.toString();
  }

}
