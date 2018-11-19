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
package jp.co.yahoo.dataplatform.spark.mds.pushdown;

import java.util.Set;
import java.util.HashSet;

import org.apache.spark.sql.sources.*;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.spread.expression.*;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.*;

public final class FilterConnectorFactory{

  public static IExpressionNode get( final Filter filter ){
    if( filter instanceof And ){
      return createAnd( (And)filter );
    }
    else if( filter instanceof Or ){
      return createOr( (Or)filter );
    }
    else if( filter instanceof Not ){
      return createNot( (Not)filter );
    }
    else if( filter instanceof EqualNullSafe ){
      return null;
    }
    else if( filter instanceof EqualTo ){
      return createEqualTo( (EqualTo)filter );
    }
    else if( filter instanceof GreaterThan ){
      return createGreaterThan( (GreaterThan)filter );
    }
    else if( filter instanceof GreaterThanOrEqual ){
      return createGreaterThanOrEqual( (GreaterThanOrEqual)filter );
    }
    else if( filter instanceof LessThan ){
      return createLessThan( (LessThan)filter );
    }
    else if( filter instanceof LessThanOrEqual ){
      return createLessThanOrEqual( (LessThanOrEqual)filter );
    }
    else if( filter instanceof StringStartsWith ){
      return createStringStartsWith( (StringStartsWith)filter );
    }
    else if( filter instanceof StringEndsWith ){
      return createStringEndsWith( (StringEndsWith)filter );
    }
    else if( filter instanceof StringContains ){
      return createStringContains( (StringContains)filter );
    }
    else if( filter instanceof In ){
      return createIn( (In)filter );
    }
    else if( filter instanceof IsNull ){
      return null;
    }
    else if( filter instanceof IsNotNull ){
      return null;
    }
    else{
      return null;
    }
  }

  public static IExpressionNode createAnd( final And filter ){
    IExpressionNode result = new AndExpressionNode();
    result.addChildNode( get( filter.left() ) );
    result.addChildNode( get( filter.right() ) );
    return result;
  }

  public static IExpressionNode createOr( final Or filter ){
    IExpressionNode result = new OrExpressionNode();
    result.addChildNode( get( filter.left() ) );
    result.addChildNode( get( filter.right() ) );
    return result;
  }

  public static IExpressionNode createNot( final Not filter ){
    IExpressionNode result = new NotExpressionNode();
    result.addChildNode( get( filter.child() ) );
    return result;
  }

  public static IExpressionNode createGreaterThan( final GreaterThan filter ){
    Object value = filter.value();
    if( value instanceof String ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new GtStringCompareFilter( value.toString() ) );
    }
    else if( value instanceof Byte ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.GT , new ByteObj( ( (Byte)value ).byteValue() ) ) );
    }
    else if( value instanceof Short ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.GT , new ShortObj( ( (Short)value ).shortValue() ) ) );
    }
    else if( value instanceof Integer ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.GT , new IntegerObj( ( (Integer)value ).intValue() ) ) );
    }
    else if( value instanceof Long ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.GT , new LongObj( ( (Long)value ).longValue() ) ) );
    }
    else if( value instanceof Float ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.GT , new FloatObj( ( (Float)value ).floatValue() ) ) );
    }
    else if( value instanceof Double ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.GT , new DoubleObj( ( (Double)value ).doubleValue() ) ) );
    }
    else{
      return null;
    }
  }

  public static IExpressionNode createGreaterThanOrEqual( final GreaterThanOrEqual filter ){
    Object value = filter.value();
    if( value instanceof String ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new GeStringCompareFilter( value.toString() ) );
    }
    else if( value instanceof Byte ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.GE , new ByteObj( ( (Byte)value ).byteValue() ) ) );
    }
    else if( value instanceof Short ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.GE , new ShortObj( ( (Short)value ).shortValue() ) ) );
    }
    else if( value instanceof Integer ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.GE , new IntegerObj( ( (Integer)value ).intValue() ) ) );
    }
    else if( value instanceof Long ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.GE , new LongObj( ( (Long)value ).longValue() ) ) );
    }
    else if( value instanceof Float ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.GE , new FloatObj( ( (Float)value ).floatValue() ) ) );
    }
    else if( value instanceof Double ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.GE , new DoubleObj( ( (Double)value ).doubleValue() ) ) );
    }
    else{
      return null;
    }
  }

  public static IExpressionNode createLessThan( final LessThan filter ){
    Object value = filter.value();
    if( value instanceof String ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new LtStringCompareFilter( value.toString() ) );
    }
    else if( value instanceof Byte ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.LT , new ByteObj( ( (Byte)value ).byteValue() ) ) );
    }
    else if( value instanceof Short ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.LT , new ShortObj( ( (Short)value ).shortValue() ) ) );
    }
    else if( value instanceof Integer ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.LT , new IntegerObj( ( (Integer)value ).intValue() ) ) );
    }
    else if( value instanceof Long ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.LT , new LongObj( ( (Long)value ).longValue() ) ) );
    }
    else if( value instanceof Float ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.LT , new FloatObj( ( (Float)value ).floatValue() ) ) );
    }
    else if( value instanceof Double ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.LT , new DoubleObj( ( (Double)value ).doubleValue() ) ) );
    }
    else{
      return null;
    }
  }

  public static IExpressionNode createLessThanOrEqual( final LessThanOrEqual filter ){
    Object value = filter.value();
    if( value instanceof String ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new LeStringCompareFilter( value.toString() ) );
    }
    else if( value instanceof Byte ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.LE , new ByteObj( ( (Byte)value ).byteValue() ) ) );
    }
    else if( value instanceof Short ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.LE , new ShortObj( ( (Short)value ).shortValue() ) ) );
    }
    else if( value instanceof Integer ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.LE , new IntegerObj( ( (Integer)value ).intValue() ) ) );
    }
    else if( value instanceof Long ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.LE , new LongObj( ( (Long)value ).longValue() ) ) );
    }
    else if( value instanceof Float ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.LE , new FloatObj( ( (Float)value ).floatValue() ) ) );
    }
    else if( value instanceof Double ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.LE , new DoubleObj( ( (Double)value ).doubleValue() ) ) );
    }
    else{
      return null;
    }
  }

  public static IExpressionNode createEqualTo( final EqualTo filter ){
    Object value = filter.value();
    if( value instanceof String ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new PerfectMatchStringFilter( value.toString() ) );
    }
    else if( value instanceof Byte ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.EQUAL , new ByteObj( ( (Byte)value ).byteValue() ) ) );
    }
    else if( value instanceof Short ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.EQUAL , new ShortObj( ( (Short)value ).shortValue() ) ) );
    }
    else if( value instanceof Integer ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.EQUAL , new IntegerObj( ( (Integer)value ).intValue() ) ) );
    }
    else if( value instanceof Long ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.EQUAL , new LongObj( ( (Long)value ).longValue() ) ) );
    }
    else if( value instanceof Float ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.EQUAL , new FloatObj( ( (Float)value ).floatValue() ) ) );
    }
    else if( value instanceof Double ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new NumberFilter( NumberFilterType.EQUAL , new DoubleObj( ( (Double)value ).doubleValue() ) ) );
    }
    else if( value instanceof Boolean ){
      return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new BooleanFilter( ( (Boolean)value ) .booleanValue() ) );
    }
    else{
      return null;
    }
  }

  public static IExpressionNode createStringStartsWith( final StringStartsWith filter ){
    return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new ForwardMatchStringFilter( filter.value() ) );
  }

  public static IExpressionNode createStringEndsWith( final StringEndsWith filter ){
    return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new BackwardMatchStringFilter( filter.value() ) );
  }

  public static IExpressionNode createStringContains( final StringContains filter ){
    return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new PartialMatchStringFilter( filter.value() ) );
  }

  public static IExpressionNode createIn( final In filter ){
    Object[] values = filter.values();
    Set<String> dic = new HashSet<String>();
    for( Object value : values ){
      if( value instanceof String ){
        dic.add( value.toString() );
      }
      else{
        return null;
      }
    }
    return new ExecuterNode( new StringExtractNode( filter.attribute() ) , new StringDictionaryFilter( dic ) );
  }

}
