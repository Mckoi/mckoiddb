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

package com.mckoi.data;

/**
 * An error caused by a critical stop state of the database, such as a
 * faulty disk/full disk or running out of memory in the VM.  Any condition
 * that may detriment the state of any existing persistent state will cause a
 * database implementation to generate this exception on all access until the
 * VM running the system is shut down, the problem fixed, and the system
 * restarted.
 *
 * @author Tobias Downer
 */

public class CriticalStopError extends Error {

  /**
   * The Error constructor.
   */
  public CriticalStopError(String message, Throwable parent) {
    super(message, parent);
  }

}
