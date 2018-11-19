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
package jp.co.yahoo.dataplatform.spark.mds.schema;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.SparkSession;

import jp.co.yahoo.dataplatform.schema.design.IField;
import jp.co.yahoo.dataplatform.schema.design.StructContainerField;
import jp.co.yahoo.dataplatform.schema.design.SparkSchemaFactory;

import jp.co.yahoo.dataplatform.mds.MDSReader;

public final class SchemaFactory{

  private SchemaFactory(){}

  public static StructType create( final SparkSession sparkSession , final jp.co.yahoo.dataplatform.config.Configuration mdsConfig , final FileStatus[] files ) throws IOException{
    Configuration conf = sparkSession.sessionState().newHadoopConf();
    StructType result = SparkSchemaFactory.getSparkSchema( readSchemaFromFile( conf , mdsConfig , files[0] ) );
    return result;
  }

  public static IField readSchemaFromFile( final Configuration config , final jp.co.yahoo.dataplatform.config.Configuration mdsConfig , final FileStatus file ) throws IOException{
    MDSReader reader = new MDSReader();
    IField result = new StructContainerField( "root" );
    try{
      Path filePath = file.getPath();
      FileSystem fs = filePath.getFileSystem( config );
      InputStream in = fs.open( filePath );
      reader.setNewStream( in , file.getLen() , mdsConfig );
      for( int i = 0 ; i < 1 && reader.hasNext() ; i++ ){
        result.merge( reader.next().getSchema() );
      }
    }finally{
      reader.close();
    }
    return result;
  }

}
