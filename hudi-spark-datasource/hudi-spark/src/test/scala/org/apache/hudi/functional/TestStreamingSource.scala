/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceReadOptions.STREAMING_READ_START_OFFSET
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.common.model.HoodieTableType.{COPY_ON_WRITE, MERGE_ON_READ}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.HoodieWriteConfig.{DELETE_PARALLELISM_VALUE, INSERT_PARALLELISM_VALUE, TBL_NAME, UPSERT_PARALLELISM_VALUE}
import org.apache.log4j.Level
import org.apache.spark.SparkException
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

class TestStreamingSource extends StreamTest {

  import testImplicits._
  private val commonOptions = Map(
    RECORDKEY_FIELD.key -> "id",
    PRECOMBINE_FIELD.key -> "ts",
    INSERT_PARALLELISM_VALUE.key -> "4",
    UPSERT_PARALLELISM_VALUE.key -> "4",
    DELETE_PARALLELISM_VALUE.key -> "4"
  )

  private val cleanerOptions = Map(
    "hoodie.cleaner.commits.retained" -> "3",
    "hoodie.keep.min.commits" -> "4",
    "hoodie.keep.max.commits" -> "5"
  )
  private val columns = Seq("id", "name", "price", "ts")

  org.apache.log4j.Logger.getRootLogger.setLevel(Level.WARN)

  override protected def sparkConf = {
    super.sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
  }

  test("test cow stream source") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_cow_stream"
      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(COPY_ON_WRITE)
          .setTableName(getTableName(tablePath))
          .setPayloadClassName(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.defaultValue)
        .setPreCombineField("ts")
          .initTable(spark.sessionState.newHadoopConf(), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")), commonOptions)
      val df = spark.readStream
        .format("org.apache.hudi")
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        StartStream(),
        AssertOnQuery {q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("1", "a1", "10", "000")), lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath, Seq(("1", "a1", "12", "000")), commonOptions),
        StartStream(),
        AssertOnQuery {q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("1", "a1", "12", "000")), lastOnly = true, isSorted = false),

        addDataToQuery(tablePath,
          Seq(("2", "a2", "12", "000"),
              ("3", "a3", "12", "000"),
              ("4", "a4", "12", "000")), commonOptions),
        AssertOnQuery {q => q.processAllAvailable(); true },
        CheckAnswerRows(
          Seq(Row("2", "a2", "12", "000"),
             Row("3", "a3", "12", "000"),
             Row("4", "a4", "12", "000")),
          lastOnly = true, isSorted = false),
          StopStream,

        addDataToQuery(tablePath, Seq(("5", "a5", "12", "000")), commonOptions),
        addDataToQuery(tablePath, Seq(("6", "a6", "12", "000")), commonOptions),
        addDataToQuery(tablePath, Seq(("5", "a5", "15", "000")), commonOptions),
        StartStream(),
        AssertOnQuery {q => q.processAllAvailable(); true },
        CheckAnswerRows(
          Seq(Row("6", "a6", "12", "000"),
            Row("5", "a5", "15", "000")),
          lastOnly = true, isSorted = false)
      )
    }
  }

  test("test mor stream source") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_mor_stream"
      HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(MERGE_ON_READ)
        .setTableName(getTableName(tablePath))
        .setPayloadClassName(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.defaultValue)
        .setPreCombineField("ts")
        .initTable(spark.sessionState.newHadoopConf(), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")), commonOptions)
      val df = spark.readStream
        .format("org.apache.hudi")
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery {q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("1", "a1", "10", "000")), lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath,
          Seq(("2", "a2", "12", "000"),
            ("3", "a3", "12", "000"),
            ("2", "a2", "10", "001")), commonOptions),
        StartStream(),
        AssertOnQuery {q => q.processAllAvailable(); true },
        CheckAnswerRows(
          Seq(Row("3", "a3", "12", "000"),
            Row("2", "a2", "10", "001")),
          lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath, Seq(("5", "a5", "12", "000")), commonOptions),
        addDataToQuery(tablePath, Seq(("6", "a6", "12", "000")), commonOptions),
        StartStream(),
        AssertOnQuery {q => q.processAllAvailable(); true },
        CheckAnswerRows(
          Seq(Row("5", "a5", "12", "000"),
            Row("6", "a6", "12", "000")),
          lastOnly = true, isSorted = false)
      )
    }
  }

  test("Test cow from latest offset") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_cow_stream"
      HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(COPY_ON_WRITE)
        .setTableName(getTableName(tablePath))
        .setPayloadClassName(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.defaultValue)
        .setPreCombineField("ts")
        .initTable(spark.sessionState.newHadoopConf(), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")), commonOptions)
      val df = spark.readStream
        .format("org.apache.hudi")
        .option(STREAMING_READ_START_OFFSET.key(), "latest")
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery {q => q.processAllAvailable(); true },
        // Start from the latest, should contains no data
        CheckAnswerRows(Seq(), lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath, Seq(("2", "a1", "12", "000")), commonOptions),
        StartStream(),
        AssertOnQuery {q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("2", "a1", "12", "000")), lastOnly = false, isSorted = false)
      )
    }
  }

  test("Test cow from specified offset") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_cow_stream"
      val metaClient = HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(COPY_ON_WRITE)
        .setTableName(getTableName(tablePath))
        .setPayloadClassName(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.defaultValue)
        .setPreCombineField("ts")
        .initTable(spark.sessionState.newHadoopConf(), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")), commonOptions)
      addData(tablePath, Seq(("2", "a1", "11", "001")), commonOptions)
      addData(tablePath, Seq(("3", "a1", "12", "002")), commonOptions)

      val timestamp = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants()
        .firstInstant().get().getTimestamp
      val df = spark.readStream
        .format("org.apache.hudi")
        .option(STREAMING_READ_START_OFFSET.key(), timestamp)
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery {q => q.processAllAvailable(); true },
        // Start after the first commit
        CheckAnswerRows(Seq(Row("2", "a1", "11", "001"), Row("3", "a1", "12", "002")), lastOnly = true, isSorted = false)
      )
    }
  }

  test("Test throws exception after restart from checkpoint" +
    "if the offset no longer exists in the timeline") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_cow_stream"
      val checkpointDir = s"${inputDir.getCanonicalPath}/checkpoint"
      val metaClient = HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(COPY_ON_WRITE)
        .setTableName(getTableName(tablePath))
        .setPayloadClassName(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.defaultValue)
        .setPreCombineField("ts")
        .initTable(spark.sessionState.newHadoopConf(), tablePath)

      val writeOptions = commonOptions ++ cleanerOptions

      addData(tablePath, Seq(("1", "a1", "10", "000")), writeOptions)

      val df = spark.readStream
        .format("org.apache.hudi")
        .load(tablePath)
        .select("id", "name", "price", "ts")
      // tests with checkpoints
      // https://github.com/apache/spark/blob/6e4c352d5f91f8343cec748fea4723178d5ae9af/sql/core/src/test/scala/org/apache/spark/sql/streaming/StreamingQueryManagerSuite.scala
      // https://github.com/apache/spark/blob/6e4c352d5f91f8343cec748fea4723178d5ae9af/sql/core/src/test/scala/org/apache/spark/sql/streaming/StreamingQuerySuite.scala
      testStream(df)(
        // Consume available instants and stop the query
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("1", "a1", "10", "000")), lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath, Seq(("2", "a2", "12", "000")), writeOptions),
        addDataToQuery(tablePath, Seq(("3", "a3", "12", "000")), writeOptions),
        addDataToQuery(tablePath, Seq(("4", "a4", "12", "000")), writeOptions),
        addDataToQuery(tablePath, Seq(("5", "a5", "12", "000")), writeOptions),
        addDataToQuery(tablePath, Seq(("6", "a6", "12", "000")), writeOptions),
        addDataToQuery(tablePath, Seq(("7", "a7", "12", "000")), writeOptions),
        addDataToQuery(tablePath, Seq(("8", "a8", "12", "000")), writeOptions),
        addDataToQuery(tablePath, Seq(("9", "a9", "12", "000")), writeOptions),

        StartStream(),
        ExpectFailure[SparkException](),
        AssertOnQuery(_.isActive === false),
        AssertOnQuery(q => {
          q.exception.get.message.contains("No instant found for commit time")
        }, "Incorrect exception message on restart when start offset no longer exists in the timeline")
      )
    }
  }

  private def addData(inputPath: String, rows: Seq[(String, String, String, String)], writeOptions: Map[String, String]): Unit = {
    rows.toDF(columns: _*)
      .write
      .format("org.apache.hudi")
      .options(writeOptions)
      .option(TBL_NAME.key, getTableName(inputPath))
      .mode(SaveMode.Append)
      .save(inputPath)
  }

  private def addDataToQuery(inputPath: String,
                             rows: Seq[(String, String, String, String)],
                             writeOptions: Map[String, String]): AssertOnQuery = {
    AssertOnQuery { _=>
      addData(inputPath, rows, writeOptions)
      true
    }
  }

  private def getTableName(inputPath: String): String = {
    val start = inputPath.lastIndexOf('/')
    inputPath.substring(start + 1)
  }
}
