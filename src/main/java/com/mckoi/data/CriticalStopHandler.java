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
 * An object that handles critical stop conditions in an application.  For
 * example, if it was necessary to stop all IO operations when an IOException
 * is encountered then it may be handled by this object.
 * 
 * @author Tobias Downer
 * @deprecated this feature is no longer user defined.
 */

public interface CriticalStopHandler {

  /**
   * Throws an Error exception when a critical IO error occurs, and performs
   * any operations necessary to shut down operations on the database.
   */
  Error errorIO(java.io.IOException e) throws Error;

  /**
   * Throws an Error exception when a critical virtual machine error occurs,
   * and performs any operations necessary to shut down operations on the
   * database.  Most typically this is caused by an out of memory error.
   */
  Error errorVirtualMachine(java.lang.VirtualMachineError e) throws Error;
  
}
