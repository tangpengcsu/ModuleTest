
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming._

/** 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang
  * 创建日期：2018-03-24
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-03-24     1.0.1.0    tang      创建
  * ----------------------------------------------------------------
  */
case class Recoder(topic: String, offset: Long, key: String, name: String, add: String) {
  override def toString: String = s"$topic:{topic}, offset:${offset}, key:${key}, name:${name}, add:${add}"
}

trait Log

case class Log1(date: Int, bizCode: Int) extends Log{
  override def toString: String = s"date=${date},bizCode=${bizCode}"
}

case class Log2(date: Int, bizCode: Int) extends Log
case class Topic(topicName:String,log:Log)
object Test {
  def main(args: Array[String]): Unit = {
    test2()
  }

  def test2(): Unit = {
    val warehouseLocation = "hdfs://192.168.50.88:9000/user/hive/warehouse"
    val sparkSession = SparkSession
      .builder()
      .appName("Spark Streaming with kafka Example")
      .master("local[1]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._
    import sparkSession.sql


    //val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Milliseconds(50))

    //org.apache.kafka.common.serialization.StringDeserializer
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.50.88:9092,192.168.50.85:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "2",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test1", "test2")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val sc = streamingContext.sparkContext


    /*   stream.transform {
         rdd => {

           println("=============1")
           import sparkSession.implicits._
           rdd.map(_.value().split(",")).map(p => NginxBeans(p(0), p(1))).toDF().createOrReplaceTempView("nginxtable")
           /*
                   rdd.map(_.value()).map(x=>lineToGroup(x)).filter(_!=null).map(p => NginxBeans(p(0),p(1))).toDF()
                   .registerTempTable("nginxtable")
           */

           sparkSession.sql("select * from  nginxtable ").show()
           println("=================================3")
           rdd
         }
       }*/
    sql("select * from testkafka").createOrReplaceTempView("orignal")

    def procTopicTest1(log1: Log1): Unit ={
      log1
    }

 /*  val map= stream.mapPartitions(it=>{
      val result = List[Topic]()

      it.foreach(i => {
        val key = i.key()
        val value = i.value()
        val topic = i.topic()
        //val offset = i.offset()
        val v = value.split(",")
        var log: Log = null
        topic match {
          case "test1" => log = Log1(v(0).toInt, v(1).toInt)
          case "test2" => log = Log2(v(0).toInt, v(1).toInt)
        }

        result :+ Topic(topic,log)
      })
      result.iterator
    })

    map.transform(rdd=>{
      val test1Topic  = rdd.filter(_.topicName=="test1")
      val log = test1Topic.map(_.log.asInstanceOf[Log1])
      log.toDF().createOrReplaceTempView("test1")
      /*select record_sn,cast(sett_date as int),cast(biz_code as int),fee_id from ODS.KGOB_VOUCHER_LOG limit 2;*/
     val df = sql("select t.date,t.bizCode from test1 t left join ODS.KGOB_VOUCHER_LOG l on t.date=cast(sett_date as " +
        "int) and  t.bizCode=cast(biz_code as int) ")
      df.rdd
    })*/
  /*  val trans = stream.transform(rdd => {

      val s = rdd.mapPartitions(it => {
        val result = List[Topic]()

        it.foreach(i => {
          val key = i.key()
          val value = i.value()
          val topic = i.topic()
          //val offset = i.offset()
          val v = value.split(",")
          var log: Log = null
          topic match {
            case "test1" => log = Log1(v(0).toInt, v(1).toInt)
            case "test2" => log = Log2(v(0).toInt, v(1).toInt)
          }

          result :+ Topic(topic,log)
        })
        result.iterator
      })

      s
    })
    trans.foreachRDD(item=>{
      if(item.count()>0){
        println("==========================================")
        item.foreach(t=>{
          println(t.topicName+t.log.asInstanceOf[Log1])
        })
      }
    })
    trans.print()*/

    val s = stream.map(i => {
      val v = i.value.split(",")
      val recoder = Recoder(i.topic(), i.offset(), i.key(), v(0), v(1))

      recoder
    })
    s.foreachRDD(item => {

      if (item.count() > 0) {
        item.toDF().select("name", "add").createOrReplaceTempView("tmp")
        sql("select r.name,r.add from tmp  r inner join testkafka k on r.name = k.name ").write.mode(SaveMode.Append)
          .insertInto("testkafka2")

        sql("select * from tmp").write.mode(SaveMode.Append).insertInto("testkafka1")
        // sql("select * from testkafka1").show()
        println("finish=============================================================")
      } else {
        println("end=============================================================")

      }
    })


    streamingContext.start() // Start the computation
    streamingContext.awaitTermination()

  }

  case class Recive(key: String, value: String) {

  }

  def test(): Unit = {
    /* //kafka topic
     val topics = List(("aaa",1)).toMap
     //zookeeper
     val zk = "10.1.11.71,10.1.11.72,10.1.11.73"
     val conf = new SparkConf() setMaster "yarn-cluster" setAppName "SparkStreamingETL"
     //create streaming context
     val ssc = new StreamingContext(conf , Seconds(1))
     //get every lines from kafka
     val lines = KafkaUtils.createStream(ssc,zk,"sparkStreaming",topics).map(_._2)
     //get spark context
     val sc = ssc.sparkContext
     //get sql context
     val sqlContext = new SQLContext(sc)
     //process every rdd AND save as HTable
     lines.foreachRDD(rdd => {
       //case class implicits
       import sqlContext.implicits._
       //filter empty rdd
       if (!rdd.isEmpty) {
         //register a temp table
         rdd.map(_.split(",")).map(p => Persion(p(0), p(1).trim.toDouble, p(2).trim.toInt, p(3).trim.toDouble)).toDF
         .registerTempTable("oldDriver")
         //use spark SQL
         val rs = sqlContext.sql("select count(1) from oldDriver")
         //create hbase conf
         val hconf = HBaseConfiguration.create()
         hconf.set("hbase.zookeeper.quorum",zk)
         hconf.set("hbase.zookeeper.property.clientPort", "2181")
         hconf.set("hbase.defaults.for.version.skip", "true")
         hconf.set(TableOutputFormat.OUTPUT_TABLE, "obd_pv")
         hconf.setClass("mapreduce.job.outputformat.class", classOf[TableOutputFormat[String]],
         classOf[OutputFormat[String, Mutation]])
         val jobConf = new JobConf(hconf)
         //convert every line to hbase lines
         rs.rdd.map(line => (System.currentTimeMillis(),line(0))).map(line =>{
           //create hbase put
           val put = new Put(Bytes.toBytes(line._1))
           //add column
           put.addColumn(Bytes.toBytes("pv"),Bytes.toBytes("pv"),Bytes.toBytes(line._2.toString))
           //retuen type
           (new ImmutableBytesWritable,put)
         }).saveAsNewAPIHadoopDataset(jobConf)     //save as HTable
       }
     })
     //streaming start
     ssc start()
     ssc awaitTermination()*/
  }
}
