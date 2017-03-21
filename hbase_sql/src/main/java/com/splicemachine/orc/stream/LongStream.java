/*
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
package com.splicemachine.orc.stream;

import com.splicemachine.orc.checkpoint.LongStreamCheckpoint;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import java.io.IOException;

public interface LongStream
        extends ValueStream<LongStreamCheckpoint>
{
    long next()
            throws IOException;

    void nextIntVector(int items, int[] vector)
            throws IOException;

    void nextIntVector(int items, int[] vector, boolean[] isNull)
            throws IOException;

    void nextLongVector(int items, long[] vector)
            throws IOException;

    void nextLongVector(int items, long[] vector, boolean[] isNull)
            throws IOException;

    void nextLongVector(DataType type, int items, ColumnVector columnVector)
            throws IOException;

    void nextLongVector(DataType type, int items, ColumnVector columnVector, boolean[] isNull)
            throws IOException;

    void nextIntVector(DataType type, int items, ColumnVector columnVector)
            throws IOException;

    void nextIntVector(DataType type, int items, ColumnVector columnVector, boolean[] isNull)
            throws IOException;

    long sum(int items)
            throws IOException;
}
