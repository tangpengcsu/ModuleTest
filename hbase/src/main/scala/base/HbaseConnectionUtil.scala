package base

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job

/**
  * Created by Administrator on 2017/8/29.
  */
object HbaseConnectionUtil {
  val zookeeperConn = "192.168.50.88,192.168.50.85,192.168.50.86"
  val zNode = "/hbase"
 // val zookeeperConn = "10.80.1.241,10.80.1.243,10.80.1.240"
 // val zNode = "/hbase-unsecure"
  def createConn(): Connection = {
    //创建一个配置，采用的是工厂方法
    val jobConf = HBaseConfiguration.create()
    jobConf.set("hbase.zookeeper.quorum",zookeeperConn)
   jobConf.set("zookeeper.znode.parent",zNode)
    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val connection = ConnectionFactory.createConnection(jobConf)
    connection
  }
  def createJobConf(tableName:String):JobConf={
    val conf = HBaseConfiguration.create()
    var jobConf = new JobConf(conf)
    jobConf.set("hbase.zookeeper.quorum", zookeeperConn)
    jobConf.set("zookeeper.znode.parent", zNode)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf.setOutputFormat(classOf[org.apache.hadoop.hbase.mapred.TableOutputFormat])
    jobConf
  }
  def createJob(tableName:String):Job={

    val jobConf=createJobConf(tableName)
    val job = new Job(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job
  }

  def createJob(hadoopConfiguration:Configuration,tableName:String):Job={
    hadoopConfiguration.set("hbase.zookeeper.quorum",zookeeperConn)
    hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    hadoopConfiguration.set("zookeeper.znode.parent",zNode)

    hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val jobConf=createJobConf(tableName)
    val job = new Job(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job
  }

  def createJobConfForHTable():JobConf={
    val conf = HBaseConfiguration.create()
    var jobConf = new JobConf(conf)
    jobConf.set("hbase.zookeeper.quorum",zookeeperConn)
    jobConf.set("hbase.zookeeper.property.clientPort","2181")
    jobConf.set("zookeeper.znode.parent",zNode)
    jobConf.setOutputFormat(classOf[org.apache.hadoop.hbase.mapred.TableOutputFormat])
    jobConf
  }

}
