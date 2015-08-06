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

package com.mckoi.odb;

import com.mckoi.data.GeneralCacheKey;

/**
 * A GeneralCacheKey implementation based on a Reference.
 *
 * @author Tobias Downer
 */

public class ReferenceCacheKey implements GeneralCacheKey {

  private final int type_code;
  private final Reference reference;

  ReferenceCacheKey(int type_code, Reference reference) {
    if (reference == null) throw new NullPointerException();
    this.type_code = type_code;
    this.reference = reference;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ReferenceCacheKey other = (ReferenceCacheKey) obj;
    return (this.type_code == other.type_code &&
            this.reference.equals(other.reference));
  }

  @Override
  public int hashCode() {
    return reference.hashCode() ^ type_code;
  }

}
