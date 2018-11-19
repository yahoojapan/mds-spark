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
package jp.co.yahoo.dataplatform.spark.mds

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory, OutputWriter}
import org.apache.spark.sql.catalyst.InternalRow

import jp.co.yahoo.dataplatform.config.Configuration
import jp.co.yahoo.dataplatform.schema.parser.SparkMessageReader
import jp.co.yahoo.dataplatform.mds.MDSRecordWriter

import jp.co.yahoo.dataplatform.spark.mds.utils.ConfigUtil

class MDSOutputWriterFactory extends OutputWriterFactory{

  override def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    def reader:SparkMessageReader = new SparkMessageReader( dataSchema )
    def fs:FileSystem = FileSystem.get( context.getConfiguration() )
    def writer:MDSRecordWriter = new MDSRecordWriter( fs.create( new Path( path ) ) , ConfigUtil.createConfig )
    new MDSOutputWriter( reader , writer )
  }

  override def getFileExtension(context: TaskAttemptContext): String = {
    ".mds"
  }

}

class MDSOutputWriter( reader:SparkMessageReader , writer:MDSRecordWriter ) extends OutputWriter{

  override def write( row: InternalRow ): Unit = {
    writer.addParserRow( reader.create( row ) );
  }

  override def close(): Unit = {
    writer.close();
  }

}
