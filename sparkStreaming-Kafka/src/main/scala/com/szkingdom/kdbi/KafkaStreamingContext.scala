package com.szkingdom.kdbi

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2018-05-21
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-05-21     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */
object KafkaStreamingContext {

  val warehouseLocation = "hdfs://192.168.50.88:9000/user/hive/warehouse"
 val bootstrapServers ="192.168.50.88:9092,192.168.50.85:9092"
  def create(topics:String*):(SparkSession,StreamingContext,InputDStream[ConsumerRecord[String,String]])={


    val sparkSession = SparkSession
      .builder()
      .appName("Spark Streaming with kafka Example")
      .master("local[1]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    // 设置容错检查点
    streamingContext.checkpoint("/sparkstreaming/checkpoint")


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "2",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val stream:InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    (sparkSession,streamingContext,stream)
  }
}
