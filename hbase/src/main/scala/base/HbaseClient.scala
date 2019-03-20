package base

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter, FilterList}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{Cell, CellUtil, HColumnDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, _}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * author: sunj@szkingdom.com
  * description:hbase客户端：1、提供scan的方式获取hbase数据
  * create in: 2017/9/1.
  */
object HbaseClient {
  protected val OPERATOR_SUCCESS: Int = 0
  protected val DEFAULT_ROW_FAMILY: Array[Byte] = Bytes.toBytes("R")
  private val conn = HbaseConnectionUtil.createConn()

  def scan(tableName: String, filter: Filter, startRow: String, stopRow: String): List[Map[String, String]] = {
    val s = buildScan(filter, startRow, stopRow)
    val t = conn.getTable(TableName.valueOf(tableName))
    scan(t, s)
  }

   def readCell(cell: Cell): (String, String) = {
    val quailifier = Bytes.toString(CellUtil.cloneQualifier(cell))
    val value = Bytes.toString(CellUtil.cloneValue(cell))
    (quailifier, value)
  }

   def scan(table: Table, scan: Scan): List[Map[String, String]] = {
    val scanner = table.getScanner(scan)
    val ite = scanner.iterator()
    val result = new ListBuffer[Map[String, String]]
    while (ite.hasNext) {
      val map = new mutable.ListMap[String, String]
      ite.next().listCells().foreach(c => map += readCell(c))
      result += map.toMap
    }
    result.toList
  }

   def buildScan(filter: Filter, startRow: String, stopRow: String): Scan = {
    val scan = new Scan()
    /*scan.setMaxVersions()
    scan.setCaching(2000)
    scan.setCacheBlocks(false)*/
    if (null != filter) scan.setFilter(filter)
    if (null != startRow) scan.setStartRow(Bytes.toBytes(startRow))
    if (null != stopRow) scan.setStopRow(Bytes.toBytes(stopRow))
    scan
  }

/*  def writeToHBWithBulkLoad(tableName: String, rdd: RDD[Row], gen: RowKeyGenerator, stkIdindex: Int, family: String, fieldList: List[FileField], isWriteWal: Boolean): Int = {
//    println(fieldList.size + "-------<filed=========")
    val jobConf = HbaseConnectionUtil.createJobConf(tableName)
//    println(stkIdindex + "++++++++++++++++++++++")
    rdd.map(attr => {
//      println(attr.length + "-------<RDD=========")
      var iIndex = 1
      var put = new Put(Bytes.toBytes(gen.generateKey(Integer.parseInt(attr.get(0).toString))))
      while (iIndex < attr.length) {
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(fieldList.get(iIndex - 1).p_strFieldName), Bytes.toBytes(attr(iIndex).toString))
        iIndex += 1
      }
      put.setWriteToWAL(isWriteWal)
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)
    OPERATOR_SUCCESS
  }*/

  def writeToHBWithBulkLoadNewApi(hadoopConfiguration:Configuration,tableName: String, rdd: RDD[Row], gen:
  RowKeyGenerator, family: String, fieldList: List[String], isWriteWal: Boolean): Int = {
//    println(fieldList.size + "-------<filed=========")
    val job = HbaseConnectionUtil.createJob(hadoopConfiguration,tableName)
    rdd.map(attr => {
//      println(attr.length + "-------<RDD=========")
      var iIndex = 1
      var put = new Put(Bytes.toBytes(gen.generateKey(Integer.parseInt(attr.get(0).toString))))
      put.add(Bytes.toBytes(family), Bytes.toBytes("ID"), Bytes.toBytes
      (attr.getString(0)))
      while (iIndex < attr.length) {
/*        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(fieldList.get(iIndex - 1).p_strFieldName), Bytes.toBytes(attr(iIndex).toString))
      */
        //除去空格与空
        if (!(attr.getString(iIndex).trim.equals("") || attr.getString(iIndex) == null)) {
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(fieldList.get(iIndex - 1)), Bytes.toBytes
          (attr.getString(iIndex)))
        }
        iIndex += 1
      }
      put.setWriteToWAL(isWriteWal)
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)
    OPERATOR_SUCCESS
  }
  def writeToHBWithBulkLoadNewApi1(tableName: String, rdd: RDD[Row], gen: RowKeyGenerator, family: String, fieldList:
  List[String], isWriteWal: Boolean): Int = {
    //    println(fieldList.size + "-------<filed=========")
    val job = HbaseConnectionUtil.createJob(tableName)

    rdd.map(attr => {
      //      println(attr.length + "-------<RDD=========")
      var iIndex = 0
      var put = new Put(Bytes.toBytes(gen.generateKey(Integer.parseInt(attr.get(0).toString))))
      /* put.add(Bytes.toBytes(family), Bytes.toBytes("RECORD_ID"), Bytes.toBytes
       (attr.getString(0)))*/
      while (iIndex < attr.length) {
        /*        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(fieldList.get(iIndex - 1).p_strFieldName), Bytes.toBytes(attr(iIndex).toString))
              */
        //除去空格与空
        if (!(attr.getString(iIndex).trim.equals("") || attr.getString(iIndex) == null)) {
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(fieldList.get(iIndex)), Bytes.toBytes
          (attr.getString(iIndex)))

        }
        iIndex += 1
      }
      put.setDurability(Durability.SKIP_WAL)
      //put.setWriteToWAL(isWriteWal)
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)
    OPERATOR_SUCCESS
  }

  def writeToHBWithBulkLoadNewApi3(tableName: String, rdd: RDD[Row], gen: RowKeyGenerator, family: String, fieldList:
  List[String], isWriteWal: Boolean): Int = {
    //    println(fieldList.size + "-------<filed=========")
    val job = HbaseConnectionUtil.createJob(tableName)

    rdd.map(attr => {
      //      println(attr.length + "-------<RDD=========")
      var iIndex = 0
      var put = new Put(Bytes.toBytes(gen.generateKey(Integer.parseInt(attr.get(0).toString))))
      /* put.add(Bytes.toBytes(family), Bytes.toBytes("RECORD_ID"), Bytes.toBytes
       (attr.getString(0)))*/
      while (iIndex < attr.length) {
        /*        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(fieldList.get(iIndex - 1).p_strFieldName), Bytes.toBytes(attr(iIndex).toString))
              */
        //除去空格与空
        if (!(attr.getString(iIndex).trim.equals("") || attr.getString(iIndex) == null)) {
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(fieldList.get(iIndex)), Bytes.toBytes
          (attr.getString(iIndex)))

        }
        iIndex += 1
      }
      put.setDurability(Durability.SKIP_WAL)
      //put.setWriteToWAL(isWriteWal)
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)
    OPERATOR_SUCCESS
  }
  def writeToHBWithBulkLoadNewApi2(tableName: String, rdd: RDD[Row], gen: RowKeyGenerator, family: List[String], fieldList:
  List[String], isWriteWal: Boolean): Int = {
    //    println(fieldList.size + "-------<filed=========")
    val job = HbaseConnectionUtil.createJob(tableName)

    rdd.map(attr => {
      //      println(attr.length + "-------<RDD=========")
      var iIndex = 0
      var put = new Put(Bytes.toBytes(gen.generateKey(Integer.parseInt(attr.get(0).toString))))
      /* put.add(Bytes.toBytes(family), Bytes.toBytes("RECORD_ID"), Bytes.toBytes
       (attr.getString(0)))*/
      while (iIndex < attr.length) {
        put.addColumn(Bytes.toBytes(family.get(iIndex)), Bytes.toBytes(fieldList.get(iIndex)), Bytes.toBytes
        (attr.getString(iIndex)))
        //除去空格与空
      /*  if (!(attr.getString(iIndex).trim.equals("") || attr.getString(iIndex) == null)) {
          put.addColumn(Bytes.toBytes(family.get(iIndex)), Bytes.toBytes(fieldList.get(iIndex)), Bytes.toBytes
          (attr.getString(iIndex)))

        }*/
        iIndex += 1
      }
      put.setDurability(Durability.SKIP_WAL)
      //put.setWriteToWAL(isWriteWal)
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)
    OPERATOR_SUCCESS
  }
  def writeToHBWithBulkLoadNewApi(hadoopConfiguration:Configuration,tableName: String, rdd: RDD[List[(String,String)]], gen: RowKeyGenerator, family: String, isWriteWal: Boolean): Int = {
    val job = HbaseConnectionUtil.createJob(hadoopConfiguration,tableName)
    rdd.map(attr => {
      var iIndex = 0
      var put = new Put(Bytes.toBytes(gen.generateKey(Integer.parseInt(attr.get(0)._2))))
      while (iIndex < attr.size) {
        //除去空格与空
        if (!attr.get(iIndex)._2.trim.equals("") || attr.get(iIndex)._2 == null) {
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(attr.get(iIndex)._1), Bytes.toBytes
          (attr.get(iIndex)._2))

        }
        iIndex += 1
      }
      put.setWriteToWAL(isWriteWal)
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)
    OPERATOR_SUCCESS
  }




  /**
    * 查询并转换为RDD[(ImmutableBytesWritable,Result)]
    *
    * @param sparkContext
    * @param tableName
    * @param scan
    * @return
    */
  def scanRaw(sparkContext: SparkContext, tableName: String, scan: Scan): RDD[(ImmutableBytesWritable, Result)] =
  {
    val conf = conn.getConfiguration

    scan.setMaxVersions()
    scan.setCaching(10000)
    scan.setCacheBlocks(false)

    import org.apache.hadoop.hbase.mapreduce.TableInputFormat
    //设置表名
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    //读取数据并转化成 RDD[(ImmutableBytesWritable,Result)]
    sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
  }

  /**
    * 查询并转换为RDD[Row]
    *
    * @param sparkContext
    * @param tableName
    * @return
    */
  def scanAsRow(sparkContext: SparkContext, tableName: String): RDD[Row] =
  {
    scanAsRow(sparkContext,tableName,new Scan())
  }
  /**
    * 查询并转换为RDD[Row]
    *
    * @param sparkContext
    * @param tableName
    * @param scan
    * @return
    */
  def scanAsRow(sparkContext: SparkContext, tableName: String, scan: Scan): RDD[Row] =
  {
    scanRaw(sparkContext, tableName, scan).map(r => {
      val value = r._2.listCells().map(cell => {
        Bytes.toString(CellUtil.cloneValue(cell))
      })
      Row.fromSeq(value)
    })
  }

  /**
    * 查询并转换为RDD[Row]
    *
    * @param sparkContext
    * @param tableName
    * @param scan
    * @param filters
    * @param startRow
    * @param stopRow
    * @return
    */
  def scanAsRow(sparkContext: SparkContext, tableName: String, scan: Scan, filters: List[Filter], startRow:
  String, stopRow: String): RDD[Row] =
  {
    if (null != filters && !filters.isEmpty) {
      val filterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters)
      scan.setFilter(filterList)
    }
    if (null != startRow) scan.setStartRow(Bytes.toBytes(startRow))
    if (null != stopRow) scan.setStopRow(Bytes.toBytes(stopRow))

    scanAsRow(sparkContext, tableName, scan)
  }

  /**
    * 根据字段列表查询数据
    * 如不存在，补空串
    * @param sparkContext
    * @param tableName
    * @param scan
    * @param fields  字段列表
    * @param rowFamily 列簇，默认：R
    * @return
    */
  def scanAsRow(sparkContext: SparkContext, tableName: String, scan: Scan, fields: List[String], rowFamily:
  Array[Byte] = DEFAULT_ROW_FAMILY):
  RDD[Row] =
  {
    scanRaw(sparkContext, tableName, scan).map(r => {
      val values = fields.map(field => {
        Option(Bytes.toString(r._2.getValue(rowFamily, Bytes.toBytes(field)))).getOrElse("")
      })
      Row.fromSeq(values)
    })
    // TODO: 异常处理
  }

  /**
    * 批量删除数据
    * 先查找 后依据 rowkey 删除
    * @param sparkContext
    * @param tableName
    * @param scan
    * @return
    */
  def delete(sparkContext: SparkContext, tableName: String, scan: Scan): Boolean =
  {
    try {
      conn.getConfiguration.asInstanceOf[JobConf].set(TableOutputFormat.OUTPUT_TABLE, tableName)//设置tableName
      val batchs = scanRaw(sparkContext, tableName, scan).map(r => {
          new Delete(r._2.getRow)
        }).collect().toList
      conn.getTable(TableName.valueOf(tableName)).batch(batchs)
      true
    }catch {
      case e:Exception => e.printStackTrace()
        false
    }
  }
  /**
    * 读取并转换为DF
    *
    * @param sparkSession
    * @param tableName
    * @param scan
    * @return
    */
  def scanAsDF(sparkSession: SparkSession, tableName: String, scan: Scan, fields: List[String]): DataFrame =
  {
    val schema = StructType(fields.map(columnName =>{
      StructField(columnName, StringType, true)
    }).seq)
    /*    val schema = scanRaw(sparkSession.sparkContext, tableName, scan).map(r => {
          val structField = r._2.listCells().map(cell => {
            val columnName = Bytes.toString(CellUtil.cloneQualifier(cell))
            StructField(columnName, StringType, true)
          })
          StructType(structField.seq)
        }).take(1)(0)*/

    val rowRDD: RDD[Row] = scanAsRow(sparkSession.sparkContext, tableName, scan)
    val df = sparkSession.createDataFrame(rowRDD, schema)
    // TODO: 异常处理
    //df.createOrReplaceTempView("shop")
    df
  }

  /**
    * 判断表是否存在
    * @param tableName
    * @return
    */
  def tableExists(tableName: String): Boolean =
  {
    val admin = conn.getAdmin

    if (admin.tableExists(TableName.valueOf(tableName)))
      true
    else
      false
  }



}
