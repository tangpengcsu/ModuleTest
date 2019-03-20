package jdbc

import examples.HiveContextInstance
import org.apache.spark.sql.SparkSession

/** 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang
  * 创建日期：2018-03-22
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-03-22     1.0.1.0    tang      创建
  * ----------------------------------------------------------------
  */
object OracleTest
{
  def main(args: Array[String]): Unit =
  {
    val sparkSession = HiveContextInstance.create

    query(sparkSession)
    HiveContextInstance.stop

  }
  def query(sparkSession: SparkSession):Unit={

/*     sparkSession.sqlContext.read.format("jdbc").options(
       Map("url" -> "jdbc:oracle:thin:fspt_kf/fspt_kf@//10.80.1.250:1521/orcl",
    "dbtable" -> "sys_dd", "driver" -> "oracle.jdbc.driver.OracleDriver",
    "numPartitions" ->"5","partitionColumn"->"dd_id","lowerBound"->"0","upperBound"->"80000000")).load()*/

    val url = "jdbc:oracle:thin:@10.80.1.250:1521/orcl"
    val username = "fspt_kf"
    val password = "fspt_kf"
    val driver = "oracle.jdbc.driver.OracleDriver"
    val dbtable = "(select  dd_id,dd_name,dd_type from sys_dd ) sys_dd"
    val jdbcDF = sparkSession.sqlContext.read.format("jdbc").options(Map("url" -> url, "dbtable" -> dbtable,
      "user" -> username, "password" -> password, "driver" -> driver)).load()
    import sparkSession.implicits._
    val s = jdbcDF.as[SysDd]

    s.show()

/*    val sysDD1 = jdbcDF.foreach(item=>{
      import sparkSession.implicits._
      val sysdd1 = item.asInstanceOf[SysDD1]
      println(item)
    })*/
    //sysDD1.show()
  }

}

case class SysDd(dd_id:String,dd_name :String,dd_type :String)

class SysDD1 extends Serializable{
val dd_id:String = ""
  val dd_name:String = ""
  val dd_type :String =""
}
