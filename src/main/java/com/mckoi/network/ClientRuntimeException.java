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

/**
 * A client-side runtime exception that may have resulted from an external
 * throwable error (an exception that happened on a remote server).
 *
 * @author Tobias Downer
 */

public class ClientRuntimeException extends RuntimeException {

  /**
   * The external throwable (may be null).
   */
  private ExternalThrowable external_throwable = null;
  
  /**
   * Constructors.
   */
  public ClientRuntimeException(Throwable cause) {
    super(cause);
  }

  public ClientRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public ClientRuntimeException(String message) {
    super(message);
  }

  public ClientRuntimeException() {
  }

  /**
   * Sets the external throwable.
   */
  void setExternalThrowable(ExternalThrowable et) {
    external_throwable = et;
  }

  /**
   * The ExternalThrowable if it has been set.
   */
  public ExternalThrowable getExternalThrowable() {
    return external_throwable;
  }

}
