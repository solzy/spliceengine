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
package com.splicemachine.dbTesting.system.nstest.init;

import java.io.PrintWriter;
import java.net.InetAddress;

import com.splicemachine.db.drda.NetworkServerControl;

/**
 * NWServerThread: Start a Network Server in a new Thread, based on the
 * NsTest.START_SERVER_IN_SAME_VM setting
 */
public class NWServerThread extends Thread {

	InetAddress inetaddr = null;

	String address = "localhost";

	int port = 1900;

	public NWServerThread(String address, int port) throws Exception {
		if (!(address == null)) {
			if (!(address.equals(""))) {
				this.address = address;
			}
		}
		if (port > 0) {
			this.port = port;
		}

		try {
			inetaddr = InetAddress.getByName(address);

		} catch (Exception e) {
			System.out
					.println("Invalid host address passed, cannot start server");
			e.printStackTrace();
			throw e;
		}
	}

	/*
	 * Implementation of the run() method to start the server
	 * 
	 */
	public void run() {
		try {
			NetworkServerControl nsw = new NetworkServerControl(inetaddr, port);
			nsw.start(new PrintWriter(System.out));
			System.out.println("===> Derby Network Server on " + address + ":"
					+ port + " <===");
		} catch (Exception e) {
			;
			e.printStackTrace();
		}
	}
}