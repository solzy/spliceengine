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

package com.splicemachine.range;

import com.splicemachine.primitives.ByteComparator;

/**
 * @author Scott Fines
 *         Date: 11/21/14
 */
public class ByteInterval {
    private byte[] start;
    private byte[] stop;
    private final ByteComparator byteComparator;

    public ByteInterval(byte[] start, byte[] stop,ByteComparator byteComparator) {
        this.start = start;
        this.stop = stop;
        this.byteComparator = byteComparator;
    }

    public byte[] start(){
        return start;
    }

    public byte[] stop(){
        return stop;
    }

    public static ByteInterval wrap(byte[] start, byte[] stop,ByteComparator byteComparator){
        return new ByteInterval(start,stop,byteComparator);
    }


    public ByteInterval intersect(ByteInterval other){
        //object efficiency--don't create a new object unless we know we aren't contained
        if(contains(other)) return other;
        else if(other.contains(this)) return this;

        if(isEmptyStart() || other.isEmptyStop()){
            return new ByteInterval(other.start,stop,byteComparator);
        }else if(isEmptyStop() || other.isEmptyStart()){
            return new ByteInterval(start,other.stop,byteComparator);
        }else {
            /*
             *we know that both us an other have non-empty start and stop keys, which means
             * that we have the following possibility:
             */
            if(byteComparator.compare(stop,other.start)<=0) return null; //do not intersect
            else if(byteComparator.compare(start,other.stop)>=0) return null; //do not intersect
            else return new ByteInterval(max(start,other.start),min(stop,other.stop),byteComparator);
        }
    }

    public boolean contains(ByteInterval other) {
        if(isEmptyStart()) {
            /*
             * We have an empty start key, so we cover all the range up to stop.
             * This leaves three scenarios:
             *
             * 1. isEmptyStop()         => we contain the entire byte range, so we contain everything
             * 2. other.isEmptyStop()   => our stop ends before other does, so we don't contain other
             * 3. stop < other.stop     => our stop happens before other does, so we don't contain other
             * 4. stop >= other.stop    => out stop happens after, so we contain other
             */
            if (isEmptyStop()) return true;
            else if (other.isEmptyStop() || byteComparator.compare(stop, other.stop) < 0) return false;
            else return true;
        }else if(other.isEmptyStart() || byteComparator.compare(start,other.start)>0)
            return false; //the others start key is before ours
        else {
            /*
             * We don't have an empty start key, BUT we know that start <= stop, so we have the scenarios:
             *
             * 1. isEmptyStop()         =>  we contain everything after start, so we contain everything
             * 2. other.isEmptyStop()   =>  other.stop happens after ours, so we don't contain other
             * 3. stop < other.stop     =>  our stop happens before other does, so we don't contain other
             * 4. stop >= other.stop    =>  our stop happens after, so we contain other
             */
            if(isEmptyStop())return true;
            else if(other.isEmptyStop() || byteComparator.compare(stop,other.stop)<0) return false;
            else return true;
        }
    }

    /*****************************************************************************************************************/
    /*private helper methods*/
    private byte[] max(byte[] array1, byte[] array2) {
        int compare = byteComparator.compare(array1,array2);
        if(compare>=0) return array1;
        else return array2;
    }

    private byte[] min(byte[] array1, byte[] array2) {
        int compare = byteComparator.compare(array1,array2);
        if(compare<=0) return array1;
        else return array2;
    }



    private boolean isEmptyStop() {
        return byteComparator.isEmpty(stop);
    }

    private boolean isEmptyStart() {
        return byteComparator.isEmpty(start);
    }


}