package phoenix

import java.sql.{DriverManager, SQLException}

import base.DASContext
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2018-04-26
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-04-26     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */
import org.apache.phoenix.spark._
object PhoenixWithSpark {
  def main(args: Array[String]): Unit = {
    val sparkSession  = DASContext.createSparkSession()
    // readFromPhoenix( sparkSession)
    save( sparkSession)
    //DASContext.stop()
  }
  def readFromPhoenix (sparkSession: SparkSession ): Unit ={
    val dfs = sparkSession.read.format("org.apache.phoenix.spark").options(Map("table" -> "PERSON", "zkUrl" ->
      "192.168.50.88:2181")).load()


    dfs.show()
  }

  def save1(sparkSession: SparkSession):Unit = {
    import sparkSession.implicits._
    import org.apache.phoenix.spark._
   val ss =  sparkSession.sparkContext.parallelize(buildData())
    ss.saveToPhoenix("PERSON",Seq("IDCARDNUM","NAME","AGE"),    zkUrl = Some("192.168.50.88:2181"))
  }

  /**
    * df 数据保存到hbase 只支持overwrite 方式
    * @param sparkSession
    */
  def save(sparkSession: SparkSession ):Unit={

   import sparkSession.sql

    import sparkSession.implicits._
    //val s = sparkSession.sparkContext.parallelize(buildData()).toDF()
    sql("select custid as IDCARDNUM,cast(dcdate as string) as NAME from dm_care.st_dd_profit")
    .write.format("org.apache.phoenix.spark").options(Map("table" -> "person", "zkUrl" ->
      "192.168.50.88:2181")).mode(SaveMode.Overwrite).save

  }

  def buildData():ListBuffer[Person]={
    val persons = new ListBuffer[Person]
    2000 to 3000 foreach(p=>{
     val person =   new Person(p,"654f"+p,p%100)
      persons.append(person)
    })
    persons

  }
}
case class Person(IDCardNum:Int,name:String,age:Int)
