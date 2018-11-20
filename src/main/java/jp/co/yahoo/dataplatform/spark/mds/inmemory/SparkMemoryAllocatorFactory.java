/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.dataplatform.spark.mds.inmemory;

import java.util.Objects;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;


public final class SparkMemoryAllocatorFactory {
  private static final Map<Class, MemoryAllocatorFactory> dispatch = new HashMap<>();
  static {
    dispatch.put(ArrayType.class,   (v, rc) -> new SparkArrayMemoryAllocator(v, rc));
    dispatch.put(StructType.class,  (v, rc) -> new SparkStructMemoryAllocator(v, rc, (StructType)v.dataType()));
    dispatch.put(StringType.class,  (v, rc) -> new SparkBytesMemoryAllocator(v, rc));
    dispatch.put(BinaryType.class,  (v, rc) -> new SparkBytesMemoryAllocator(v, rc));
    dispatch.put(BooleanType.class, (v, rc) -> new SparkBooleanMemoryAllocator(v, rc));
    dispatch.put(ByteType.class,    (v, rc) -> new SparkByteMemoryAllocator(v, rc));
    dispatch.put(ShortType.class,   (v, rc) -> new SparkShortMemoryAllocator(v, rc));
    dispatch.put(IntegerType.class, (v, rc) -> new SparkIntegerMemoryAllocator(v, rc));
    dispatch.put(LongType.class,    (v, rc) -> new SparkLongMemoryAllocator(v, rc));
    dispatch.put(FloatType.class,   (v, rc) -> new SparkFloatMemoryAllocator(v, rc));
    dispatch.put(DoubleType.class,  (v, rc) -> new SparkDoubleMemoryAllocator(v, rc));

    dispatch.put(MapType.class, (vector, rowCount) -> {
      if (!(vector.getChild(0).dataType() instanceof StringType)) {
        throw new UnsupportedOperationException(makeErrorMessage(vector) + ". Map key type is string only.");
      }
      return new SparkMapMemoryAllocator(vector, rowCount);
    });
  }

  private SparkMemoryAllocatorFactory() {}

  public static IMemoryAllocator get(final WritableColumnVector vector, final int rowCount) {
    MemoryAllocatorFactory factory = dispatch.get(vector.dataType().getClass());
    if (Objects.isNull(factory)) throw new UnsupportedOperationException(makeErrorMessage(vector));
    return factory.get(vector, rowCount);
  }

  private static String makeErrorMessage(final WritableColumnVector vector) {
    return "Unsupported map key datatype : " + vector.dataType().toString();
  }

  @FunctionalInterface
  private static interface MemoryAllocatorFactory {
    IMemoryAllocator get(final WritableColumnVector vector, final int rowCount) throws UnsupportedOperationException;
  }
}

