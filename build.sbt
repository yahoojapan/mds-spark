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
name := "Spark MDS"
version := "1.0"
scalaVersion := "2.11.8"
fork := true

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.0"
libraryDependencies += "jp.co.yahoo.dataplatform.mds" % "multiple-dimension-spread-common" % "0.8.0_hive-1.2.1000.2.6.2.0-205"
libraryDependencies += "jp.co.yahoo.dataplatform.mds" % "multiple-dimension-spread-arrow" % "0.8.0_hive-1.2.1000.2.6.2.0-205"
libraryDependencies += "jp.co.yahoo.dataplatform.schema" % "schema-spark" % "1.2.0"
libraryDependencies += "jp.co.yahoo.dataplatform.schema" % "schema-jackson" % "1.2.0"
libraryDependencies += "jp.co.yahoo.dataplatform.config" % "dataplatform-common-config" % "1.2.0"
