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
 * A data access exception thrown when the position of an operation is out
 * of bounds.
 *
 * @author Tobias Downer
 */

public class DataPositionOutOfBoundsException extends DataAccessException {

  public DataPositionOutOfBoundsException() {
  }

  public DataPositionOutOfBoundsException(String message) {
    super(message);
  }

  public DataPositionOutOfBoundsException(String message, Throwable cause) {
    super(message, cause);
  }

  public DataPositionOutOfBoundsException(Throwable cause) {
    super(cause);
  }

}
