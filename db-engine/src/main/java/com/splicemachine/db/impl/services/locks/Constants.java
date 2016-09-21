/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.services.locks;

/**
*/

public class Constants {

	/**
		Trace flag to display lock requests, grants and unlocks.
	*/
	public static final String LOCK_TRACE = "LockTrace";

	/**
		Trace flag to display stack trace of lock calls.
	*/
	public static final String LOCK_STACK_TRACE = "LockStackTrace";

	/**
		Trace flag to add thread information to trace info of LockTrace, 
        requires that LockTrace be set to true.
	*/
	public static final String LOCK_TRACE_ADD_THREAD_INFO = "LockTraceAddThreadInfo";


	static final byte WAITING_LOCK_IN_WAIT = 0;
	static final byte WAITING_LOCK_GRANT = 1;
	static final byte WAITING_LOCK_DEADLOCK = 2;
    static final byte WAITING_LOCK_INTERRUPTED = 3;
}