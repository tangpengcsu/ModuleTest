package base

import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.hadoop.hbase.client.{Result, _}
import org.apache.hadoop.hbase.filter.{CompareFilter, Filter, FilterList, SubstringComparator}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions._

/**
  * DESCRIPTION ${DESCRIPTION}
  * Author tangpeng
  * Date 2017-9-22
  */
object HbaseUtils
{
  private val DEFAULT_ROW_FAMILY: Array[Byte] = Bytes.toBytes("R")
  private val conn = HbaseConnectionUtil.createConn()
  private val CACHING:Int = 10000


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
    scan.setCaching(CACHING)
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
    * 根据字段列表List[String]查询数据
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
/*
  /***
    * 根据字段列表List[FileField]查询数据
    * 如不存在，补空串
    * @param sparkContext
    * @param tableName
    * @param fields
    * @param scan
    * @param rowFamily
    * @tparam T
    * @return
    */
  def scanAsRow[T](sparkContext: SparkContext, tableName: String, fields: List[FileField], scan: Scan, rowFamily:
  Array[Byte] = DEFAULT_ROW_FAMILY):
  RDD[Row] =
  {
   fields.getClass.getGenericInterfaces
    scanRaw(sparkContext, tableName, scan).map(r => {
      val values = fields.map(field => {
        Option(Bytes.toString(r._2.getValue(rowFamily, Bytes.toBytes(field.p_strFieldName)))).getOrElse("")
      })
      Row.fromSeq(values)
    })
    // TODO: 异常处理
  }*/
  //  List[FileField]

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
