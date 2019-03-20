package com.szkingdom.kdbi.schema

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2018-06-04
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-06-04     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */
class SchemaRegister(sparkSession: SparkSession) {

  private val schemaMap: mutable.Map[String, StructType] = mutable.Map()

  private def setSchema(tableName: String): Unit = {

    if (!schemaMap.contains(tableName)) {
      // TODO: 异常处理
      val schema = sparkSession.table(tableName).schema
      schemaMap put(tableName, schema)
    } else {
      // TODO: 日志记录
    }
  }

  /**
    * 获取表的 schema
    * 如果在map中没有该表的schema 则先设置；
    * 如果已有，则直接取出
    *
    * @param tableName
    * @return
    */
  def getSchema(tableName: String): StructType = {
    val schema = schemaMap.get(tableName)
    if (schema.isDefined) {
      schema.get
    } else {
      setSchema(tableName)
      schemaMap.get(tableName).get
    }
  }


}

object Test {
  def main(args: Array[String]): Unit = {
    val s = new SchemaRegister(null)
    //s.setSchema(null)
    val schemaMap: mutable.Map[String, String] = mutable.Map()
    schemaMap += ("4" -> "1")
    schemaMap += ("3" -> "1")
    schemaMap put("5", "2")
    schemaMap put("3", "2")
    println(schemaMap.get("8"))
    println(schemaMap.get("8").isDefined)
    //val map2:mutable.Map[String,String]= scala.collection.mutable.Map()
    //map2+=("1"->"2")
  }
}
