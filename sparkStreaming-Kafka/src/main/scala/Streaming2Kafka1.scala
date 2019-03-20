/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2018-05-17
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-05-17     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */

import java.util.regex.Pattern

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies.{PreferConsistent => _, _}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yuhui on 2017/2/6.
  */

case class NginxBeans(domain: String, ip: String)

object Streaming2Kafka1 {
  def main(args: Array[String]): Unit = {
    // val LOG = LoggerFactory.getLogger(Streaming2Kafka2.getClass)

    val warehouseLocation = "hdfs://192.168.50.88:9000/user/hive/warehouse"
    val sparkSession = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[1]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._


    //val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(2))

    /*   val conf = new SparkConf().setAppName("streaming_test").setMaster("local[4]").set("spark.driver.port",
    "18080").set("spark.streaming.kafka.maxRatePerPartition" ,"6")
       val ssc = new StreamingContext(conf, Duration(5000))*/

    /*   val kafkaParam = Map[String, String](
         "metadata.broker.list" -> "tagtic-master:9092,tagtic-slave01:9092,tagtic-slave02:9092,tagtic-slave03:9092",
         "auto.offset.reset" -> "smallest"
       )*/
    val kafkaParam = Map[String, Object](
      "bootstrap.servers" -> "192.168.50.88:9092,192.168.50.85:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "2",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topic: String = "test1" //消费的 topic 名字
    val topics: Set[String] = Set(topic) //创建 stream 时使用的 topic 名字集合

/*
    val topicDirs = new ZKGroupTopicDirs("donews_website_nginx_log", topic) //创建一个 ZKGroupTopicDirs 对象，对保存

    val zkClient = new ZkClient("tagtic-master:2181") //zookeeper 的host 和 ip，创建一个 client
    val children = zkClient
      .countChildren(s"${topicDirs.consumerOffsetDir}") //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
*/

    var kafkaStream: InputDStream[(String, String)] = null
    var fromOffsets: Map[TopicAndPartition, Long] = Map() //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd
      .message()) //这个会将 kafka 的消息进行 transform，最终 kafka 的数据都会变成 (topic_name, message) 这样的 tuple
    /*    if (children > 0) {
          //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
          for (i <- 0 until children) {
            val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/$i")
            val tp = TopicAndPartition(topic, i)
            fromOffsets += (tp -> partitionOffset.toLong) //将不同 partition 对应的 offset 增加到 fromOffsets 中
            //        LOG.info("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "]
            @@@@@@")
          }

          kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)]
          (ssc, kafkaParam, fromOffsets, messageHandler)
        }
        else {
          kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam,
          topics) //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
        }*/
    val kafkaStreamo = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParam)
    )
/*    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParam)
    )*/
    kafkaStreamo.transform(i=>{
      val s =i.map(_.value().split(",")).map(p=>{Src(p(0).toInt,p(1))})

      s
    }).foreachRDD(src => {
      //src.toDF().createOrReplaceTempView("test")
      src.toDF().createOrReplaceTempView("test_info")

      val rightDF = sparkSession.sql("select s.key,s.value from test_info  s left join src_info s1 on s" +
        ".key=s1.key")

      rightDF.show

      //println(rightDF.count())

      println("=============================")

    })
/*    kafkaStreamo.transform {
      rdd => {

        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._
        rdd.map(_.value().split(",")).map(p => NginxBeans(p(0), p(1))).toDF().createOrReplaceTempView("nginxtable")
        /*
                rdd.map(_.value()).map(x=>lineToGroup(x)).filter(_!=null).map(p => NginxBeans(p(0),p(1))).toDF()
                .registerTempTable("nginxtable")
        */

        sqlContext.sql("select * from  nginxtable ").show()
        rdd
      }
    }*/
/*    val s = kafkaStreamo.map(i => (i.key(), i.value()))
    s
    var offsetRanges = Array[OffsetRange]()

    kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.map(msg => msg._2).foreachRDD { rdd =>
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      rdd.map(x => lineToGroup(x)).filter(_ != null)
        .map(p => NginxBeans(p(0), p(1), p(4), p(5), p(9), p(10), p(10) + p(1)))
        .toDF().createOrReplaceTempView("nginxtable")

      sqlContext.sql("select * from  nginxtable ").show()

      /* LOG.info("======本次消费为==="+rdd.count()+"=========条记录=================")

       for (o <- offsetRanges) {
         val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
         ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString) //将该 partition 的 offset 保存到 zookeeper
         //        LOG.info(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}
         untiloffset ${o.untilOffset} #######")
       }*/
    }*/

    ssc.start() // 真正启动程序
    ssc.awaitTermination() //阻塞等待
  }

  val regex = "^([\\S]+)\\s([\\S]+)\\s(\\W+)\\s([\\S]+)\\s(\\[.+\\])\\s(\".+\")\\s(\".+\")\\s([\\S]+)\\s([\\S]+)\\s" +
    "(\".+\")\\s(\".+\")\\s(\".+\")\\s(\".+\")\\s(\".+\")\\s(\".+\")$"

  //通过正则分组，获取字段。如果字段匹配成功且大于11的为有效数据
  def lineToGroup(line: String): ArrayBuffer[String] = {
    val groups = ArrayBuffer[String]()
    val p = Pattern.compile(regex)
    val m = p.matcher(line)
    while (m.find()) {
      for (i <- Range(1, m.groupCount() + 1, 1)) {
        groups.append(m.group(i))
      }
    }
    if (groups.length >= 11) {
      return groups
    }
    null
  }
}

case class Src(key:Int,value:String)