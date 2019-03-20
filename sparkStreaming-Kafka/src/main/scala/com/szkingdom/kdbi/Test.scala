package com.szkingdom.kdbi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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
trait LogDefine

case class Log(name: String, add: String) extends LogDefine

case class Log2(name: String, add: String) extends LogDefine
import org.apache.spark.sql.Row
object Test {
  def main(args: Array[String]): Unit = {
    val context = KafkaStreamingContext.create("test1", "test2")
    val sparkSession = context._1
    val streamingContext = context._2
    val stream = context._3

    import sparkSession.implicits._
    import sparkSession.sql
    sql("select * from testkafka").createOrReplaceTempView("orignal")
   // val schemaString = "name add"



    // Generate the schema based on the string of schema

    /*
        val fields = schemaString.split(" ")

          .map(fieldName => StructField(fieldName, StringType, nullable = true))
    */

    //val schema = StructType(fields)
    var schema: StructType = null
    //缓存
   // stream.cache()

    val testKafka1Schema = sparkSession.table("testkafka1").schema
    val testKafka2Schema = sparkSession.table("testkafka2").schema

    stream.transform(rdd => {
      val m = rdd.map(i => {
        val topic = i.topic()
        val value = i.value()
        val log = buildLog(topic, value)
        println("=======topic:" + topic+"===="+value)

        (topic, log)
      })

      buildDF(m)

      val df1 = sql("select 'test1' as topicName, r.name,r.add from testdata1  r inner join orignal k on r.name = k.name")
      df1.show()
      val df2 = sql("select 'test2' as topicName, r.name,r.add from testdata2  r inner join orignal k on r.name = k.name")

      df2.show()

      df1.rdd.++(df2.rdd)
    }).foreachRDD(item => {
      val count = item.count()
      //item.foreach(i => println(s"==========================${count}+=" + i))
      if (count > 0 && !item.isEmpty()) {

        println("count==========================="+count)
        val test1Rdd = item.filter(_.getString(0) == "test1").map(i => {
          val seq = i.toSeq
          val subSeq = seq.slice(1, seq.length)
          Row.fromSeq(subSeq)
        })
        val test2Rdd = item.filter(_.getString(0) == "test2").map(i => {
          val seq = i.toSeq
          val subSeq = seq.slice(1, seq.length)
          Row.fromSeq(subSeq)
        })

        test1Rdd.foreach(i=>println("row================="+i))


        sparkSession.createDataFrame(test1Rdd,testKafka1Schema).write.mode(SaveMode.Append).insertInto("testkafka1")
        sparkSession.createDataFrame(test2Rdd,testKafka2Schema).write.mode(SaveMode.Append).insertInto("testkafka2")
      }


    })


    def buildDF(topics: RDD[(String, LogDefine)]): Unit = {

      topics.filter(_._1 == "test1").map(_._2.asInstanceOf[Log]).toDF().createOrReplaceTempView("testdata1")

      topics.filter(_._1 == "test2").map(_._2.asInstanceOf[Log2]).toDF.createOrReplaceTempView("testdata2")
    }
    def buildLog(topic: String, value: String): LogDefine = {
      topic match {
        case "test1" =>
          //record 切分规则
          val v = value.split(",")
          Log(v(0), v(1))
        case "test2" =>
          val v = value.split(" ")
          Log2(v(0), v(1))
        case _ =>
          null
      }
    }


  /*  val m = stream.map(i => {
      val v = i.value().split(",")
      Log(v(0), v(1))
    })

    m.transform(rdd => {
      rdd.toDF().createOrReplaceTempView("tmp")
      if (schema == null) {
        schema = sparkSession.table("tmp").schema
      }
      val df = sql("select r.name,r.add from tmp  r inner join orignal k on r.name = k.name ")
      df.rdd
    }).foreachRDD(rdd => {

      val count = rdd.count()
      if (count > 0 && !rdd.isEmpty()) {
        val dataDf = sparkSession.createDataFrame(rdd, schema)
        dataDf.show(10)
        dataDf.write.mode(SaveMode.Append).insertInto("testkafka1")
        dataDf.printSchema()
        rdd.foreach(i => println(s"==========================${count}+=" + i))
      }


    })*/


    streamingContext.start() // Start the computation
    streamingContext.awaitTermination()
  }

}
