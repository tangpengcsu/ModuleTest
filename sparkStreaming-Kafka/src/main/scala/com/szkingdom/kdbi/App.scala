package com.szkingdom.kdbi

import com.szkingdom.kdbi.Enums.TopicName
import com.szkingdom.kdbi.schema.SchemaRegister
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import com.szkingdom.kdbi.function.Message

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
object App {
  def main1(args: Array[String]): Unit = {

    val context = KafkaStreamingContext.create(args:_*)

    val sparkSession = context._1
    val streamingContext = context._2
    val stream = context._3

    //schema 初始化
    val schema = new SchemaRegister(sparkSession)

    stream.transform(Message.parseMsg(sparkSession,_))

    streamingContext.start() // Start the computation
    streamingContext.awaitTermination()

  }

  def main(args: Array[String]): Unit = {

  }
}
