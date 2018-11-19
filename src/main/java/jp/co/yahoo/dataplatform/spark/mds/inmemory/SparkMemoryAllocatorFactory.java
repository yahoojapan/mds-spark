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

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.execution.vectorized.ColumnVector;

import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public final class SparkMemoryAllocatorFactory{

  private SparkMemoryAllocatorFactory(){}

  public static IMemoryAllocator get( final ColumnVector vector , final int rowCount ){
    DataType schema = vector.dataType();
    if( schema instanceof ArrayType ){
      return new SparkArrayMemoryAllocator( vector , rowCount );
    }
    else if( schema instanceof StructType ){
      StructType st = (StructType)schema;
      return new SparkStructMemoryAllocator( vector , rowCount , st );
    }
    else if( schema instanceof MapType ){
      throw new UnsupportedOperationException( "Unsupported datatype : " + schema.toString() );
    }
    else if( schema instanceof StringType ){
      return new SparkBytesMemoryAllocator( vector , rowCount );
    }
    else if( schema instanceof BinaryType ){
      return new SparkBytesMemoryAllocator( vector , rowCount );
    }
    else if( schema instanceof BooleanType ){
      return new SparkBooleanMemoryAllocator( vector , rowCount );
    }
    else if( schema instanceof ByteType ){
      return new SparkByteMemoryAllocator( vector , rowCount );
    }
    else if( schema instanceof ShortType ){
      return new SparkShortMemoryAllocator( vector , rowCount );
    }
    else if( schema instanceof IntegerType ){
      return new SparkIntegerMemoryAllocator( vector , rowCount );
    }
    else if( schema instanceof LongType ){
      return new SparkLongMemoryAllocator( vector , rowCount );
    }
    else if( schema instanceof FloatType ){
      return new SparkFloatMemoryAllocator( vector , rowCount );
    }
    else if( schema instanceof DoubleType ){
      return new SparkDoubleMemoryAllocator( vector , rowCount );
    }
    else{
      throw new UnsupportedOperationException( "Unsupported datatype : " + schema.toString() );
    }
  }

}
