package com.szkingdom.kdbi.function

import com.szkingdom.kdbi.Enums.TopicName
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

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
object Message {
  def parseMsg(sparkSession: SparkSession, rdd:RDD[ConsumerRecord[String,String]])={
    rdd.map(recoder=>{
      val topic = recoder.topic()
      val value = recoder.value()
      implicit val formats = DefaultFormats
      val json = parse(value)
      // 样式类从JSON对象中提取值
      sparkSession.read.json(value)
      topic match {
        case TopicName.TEST1 =>

        case TopicName.TEST2 =>
      }
    })
  }
}
