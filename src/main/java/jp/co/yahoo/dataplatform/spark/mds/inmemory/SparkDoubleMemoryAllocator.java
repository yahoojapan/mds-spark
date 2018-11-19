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

import java.io.IOException;

import org.apache.spark.sql.execution.vectorized.ColumnVector;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class SparkDoubleMemoryAllocator implements IMemoryAllocator{

  private final ColumnVector vector;
  private final int vectorSize;

  public SparkDoubleMemoryAllocator( final ColumnVector vector , final int vectorSize ){
    this.vector = vector;
    this.vectorSize = vectorSize;
  }

  @Override
  public void setNull( final int index ){
    vector.putNull( index );
  }

  @Override
  public void setBoolean( final int index , final boolean value ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setBoolean()" );
  }

  @Override
  public void setByte( final int index , final byte value ) throws IOException{
    setPrimitiveObject( index , new ByteObj( value ) );
  }

  @Override
  public void setShort( final int index , final short value ) throws IOException{
    setPrimitiveObject( index , new ShortObj( value ) );
  }

  @Override
  public void setInteger( final int index , final int value ) throws IOException{
    setPrimitiveObject( index , new IntegerObj( value ) );
  }

  @Override
  public void setLong( final int index , final long value ) throws IOException{
    setPrimitiveObject( index , new LongObj( value ) );
  }

  @Override
  public void setFloat( final int index , final float value ) throws IOException{
    setPrimitiveObject( index , new FloatObj( value ) );
  }

  @Override
  public void setDouble( final int index , final double value ) throws IOException{
    vector.putDouble( index , value );
  }

  @Override
  public void setBytes( final int index , final byte[] value ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setBytes()" );
  }

  @Override
  public void setBytes( final int index , final byte[] value , final int start , final int length ) throws IOException{
    //throw new UnsupportedOperationException( "Unsupported method setBytes()" );
    vector.putDoubles( index , length / 8 , value , start );
  }

  @Override
  public void setString( final int index , final String value ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setString()" );
  }

  @Override
  public void setString( final int index , final char[] value ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setString()" );
  }

  @Override
  public void setString( final int index , final char[] value , final int start , final int length ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setString()" );
  }

  @Override
  public void setPrimitiveObject( final int index , final PrimitiveObject value ) throws IOException{
    if( value == null ){
      setNull( index );
    }
    else{
      try{
        setDouble( index , value.getDouble() );
      }catch( Exception e ){
        setNull( index );
      }
    }
  }

  @Override
  public void setArrayIndex( final int index , final int start , final int length ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method setArrayIndex()" );
  }

  @Override
  public void setValueCount( final int count ) throws IOException{
    for( int i = count ; i < vectorSize ; i++ ){
      setNull( i );
    }
  }

  @Override
  public int getValueCount() throws IOException{
    return vectorSize;
  }

  @Override
  public IMemoryAllocator getChild( final String columnName , final ColumnType type ) throws IOException{
    throw new UnsupportedOperationException( "Unsupported method getChild()" );
  }

}
