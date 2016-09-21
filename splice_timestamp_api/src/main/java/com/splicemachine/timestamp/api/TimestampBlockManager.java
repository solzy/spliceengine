/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.timestamp.api;

/**
 * Manages how timestamp blocks are allocated to the TimestampOracle.
 *
 * Created by jleach on 12/9/15.
 */
public interface TimestampBlockManager{
    void reserveNextBlock(long currentMaxReserved) throws TimestampIOException;
    long initialize() throws TimestampIOException;
}