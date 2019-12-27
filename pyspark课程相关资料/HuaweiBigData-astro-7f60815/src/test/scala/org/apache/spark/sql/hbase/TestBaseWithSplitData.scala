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

package org.apache.spark.sql.hbase

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, GenericRow}
import org.apache.spark.sql.hbase.util.{BinaryBytesUtils, DataTypeUtils, HBaseKVHelper}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * HBaseMainTest
 * create HbTestTable and metadata table, and insert some data
 */
class TestBaseWithSplitData extends TestBase {
  val TableName_a: String = "ta"
  val TableName_b: String = "tb"
  val HbaseTableName: String = "ht"
  val Metadata_Table = "metadata"
  var alreadyInserted = false

  override protected def beforeAll() = {
    super.beforeAll()
    setupData(useMultiplePartitions = true, needInsertData = true)
    TestData
  }

  override protected def afterAll() = {
    runSql("DROP TABLE " + TableName_a)
    runSql("DROP TABLE " + TableName_b)
  }

  def createTable(useMultiplePartitions: Boolean) = {
    try {
      // delete the existing hbase table
      if (TestHbase.hbaseAdmin.tableExists(HbaseTableName)) {
        TestHbase.hbaseAdmin.disableTable(HbaseTableName)
        TestHbase.hbaseAdmin.deleteTable(HbaseTableName)
      }

      if (TestHbase.hbaseAdmin.tableExists(Metadata_Table)) {
        TestHbase.hbaseAdmin.disableTable(Metadata_Table)
        TestHbase.hbaseAdmin.deleteTable(Metadata_Table)
      }

      try {
        dropLogicalTable(TableName_a)
      } catch {
        case e: Throwable => logInfo(e.getMessage)
      }

      val splitKeys = "256,p256,128|32,p32,256|-32,n32,128|-256,n256,256|-128,n128,128|" +
        "0,zero,256|128,p128,512"
      val ta_sql =
        s"""CREATE TABLE $TableName_a(
           |  col1 STRING,
           |  col2 TINYINT,
           |  col3 SMALLINT,
           |  col4 INT,
           |  col5 LONG,
           |  col6 FLOAT,
           |  col7 INT
           |)
           |USING org.apache.spark.sql.hbase.HBaseSource
           |OPTIONS(
           |  tableName "$TableName_a",
           |  hbaseTableName "ht",
           |  keyCols "col7,col1,col3",
           |  colsMapping "col2=cf1.cq11,col4=cf1.cq12,col5=cf2.cq21,col6=cf2.cq22",
           |  splitKeys "$splitKeys"
           |  )""".stripMargin
      runSql(ta_sql)

      try {
        dropLogicalTable(TableName_b)
      } catch {
        case e: Throwable => logInfo(e.getMessage)
      }

      val sql =
        s"""CREATE TABLE $TableName_b(
           |  col1 STRING,
           |  col2 TINYINT,
           |  col3 SMALLINT,
           |  col4 INT,
           |  col5 LONG,
           |  col6 FLOAT,
           |  col7 INT
           |)
           |USING org.apache.spark.sql.hbase.HBaseSource
           |OPTIONS(
           |  tableName "$TableName_b",
           |  hbaseTableName "ht",
           |  keyCols "col7,col1,col3",
           |  colsMapping "col2=cf1.cq11,col4=cf1.cq12,col5=cf2.cq21,col6=cf2.cq22"
           |)
         """.stripMargin
      runSql(sql)

      if (!TestHbase.hbaseAdmin.tableExists(HbaseTableName)) {
        throw new IllegalArgumentException("where is our table?")
      }
    }
  }

  def checkHBaseTableExists(hbaseTable: String): Boolean = {
    val tableName = TableName.valueOf(hbaseTable)
    TestHbase.hbaseAdmin.tableExists(tableName)
  }

  def insertTestData() = {
    if (!checkHBaseTableExists(HbaseTableName)) {
      throw new IllegalStateException(s"Unable to find table $HbaseTableName")
    }

    val htable = new HTable(TestHbase.sparkContext.hadoopConfiguration, HbaseTableName)

    def putNewTableIntoHBase(keys: Seq[Any], keysType: Seq[DataType],
                             vals: Seq[Any], valsType: Seq[DataType]): Unit = {
      val row = new GenericInternalRow(keys.toArray)
      val key = HBaseKVHelper.makeRowKey(row, keysType)
      val put = new Put(key)
      Seq((vals.head, valsType.head, "cf1", "cq11"),
        (vals(1), valsType(1), "cf1", "cq12"),
        (vals(2), valsType(2), "cf2", "cq21"),
        (vals(3), valsType(3), "cf2", "cq22")).foreach {
        case (rowValue, rowType, colFamily, colQualifier) =>
          addRowVals(put, rowValue, rowType, colFamily, colQualifier)
      }
      htable.put(put)
    }

    putNewTableIntoHBase(Seq(-257, UTF8String.fromString(" n257 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](1.toByte, -2048, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(-255, UTF8String.fromString(" n255 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](2.toByte, -1024, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(-129, UTF8String.fromString(" n129 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](3.toByte, -512, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(-127, UTF8String.fromString(" n127 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](4.toByte, -256, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(-33, UTF8String.fromString(" n33 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](5.toByte, -128, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(-31, UTF8String.fromString(" n31 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](6.toByte, -64, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(-1, UTF8String.fromString(" n1 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](7.toByte, -1, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(1, UTF8String.fromString(" p1 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](8.toByte, 1, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(31, UTF8String.fromString(" p31 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](9.toByte, 4, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(33, UTF8String.fromString(" p33 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](10.toByte, 64, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(127, UTF8String.fromString(" p127 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](11.toByte, 128, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(129, UTF8String.fromString(" p129 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](12.toByte, 256, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(255, UTF8String.fromString(" p255 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](13.toByte, 512, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    putNewTableIntoHBase(Seq(257, UTF8String.fromString(" p257 "), 128: Short),
      Seq(IntegerType, StringType, ShortType),
      Seq[Any](14.toByte, 1024, 12345678901234L, 1234.5678F),
      Seq(ByteType, IntegerType, LongType, FloatType))

    htable.close()
  }

  def addRowVals(put: Put, rowValue: Any, rowType: DataType,
                 colFamily: String, colQualifier: String) = {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    val bu = BinaryBytesUtils.create(rowType)
    rowType match {
      case StringType => dos.write(bu.toBytes(rowValue.asInstanceOf[String]))
      case IntegerType => dos.write(bu.toBytes(rowValue.asInstanceOf[Int]))
      case BooleanType => dos.write(bu.toBytes(rowValue.asInstanceOf[Boolean]))
      case ByteType => dos.write(bu.toBytes(rowValue.asInstanceOf[Byte]))
      case DoubleType => dos.write(bu.toBytes(rowValue.asInstanceOf[Double]))
      case FloatType => dos.write(bu.toBytes(rowValue.asInstanceOf[Float]))
      case LongType => dos.write(bu.toBytes(rowValue.asInstanceOf[Long]))
      case ShortType => dos.write(bu.toBytes(rowValue.asInstanceOf[Short]))
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
    put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colQualifier), bos.toByteArray)
  }

  def testHBaseScanner() = {
    val scan = new Scan
    val htable = new HTable(TestHbase.sparkContext.hadoopConfiguration, HbaseTableName)
    val scanner = htable.getScanner(scan)
    var res: Result = null
    do {
      res = scanner.next
      if (res != null) logInfo(s"Row ${res.getRow} has map=${res.getNoVersionMap.toString}")
    } while (res != null)
  }

  def setupData(useMultiplePartitions: Boolean, needInsertData: Boolean = false) {
    if (needInsertData && !alreadyInserted) {
      createTable(useMultiplePartitions)
      insertTestData()
      alreadyInserted = true
    }
  }
}
