/**
 * com.mckoi.sdb.SDBTrustedObject  Jul 16, 2010
 *
 * Mckoi Database Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2010  Diehl and Associates, Inc.
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License version 3
 * along with this program.  If not, see ( http://www.gnu.org/licenses/ ) or
 * write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * Change Log:
 *
 *
 */

package com.mckoi.sdb;

/**
 * Implemented by objects in the SDB hierarchy for objects that can perform
 * trusted operations by an untrusted client. A trusted object does not
 * expose structures outside of the scope of the object specification. A
 * trusted object will not give out information about the larger context of
 * the object (for example, a trusted List object will not expose information
 * about the session or transaction).
 * <p>
 * Trusted objects allow security policy decisions, such as allowing an
 * untrusted client to access the content of a file or table in a controlled
 * way.
 * <p>
 * Note that this is a public interface. If a security manager should allow
 * trusted operations on objects that implement this, the security manager
 * should check the object was loaded by a trusted class loader also.
 *
 * @author Tobias Downer
 */

public interface SDBTrustedObject {

}
