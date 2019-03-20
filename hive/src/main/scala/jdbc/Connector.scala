package jdbc

import examples.HiveContextInstance
import org.apache.spark.sql.SparkSession

/** 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang
  * 创建日期：2018-03-21
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-03-21     1.0.1.0    tang      创建
  * ----------------------------------------------------------------
  */
object Connector
{
  def main(args: Array[String]): Unit =
  {
    val sparkSession = HiveContextInstance.create()

    oracle(sparkSession)

    HiveContextInstance.stop()
  }

  def oracle(sparkSession: SparkSession): Unit =
  {
    val table = "all_tab_columns"
    val url = "jdbc:oracle:thin:@192.168.50.85:1521:orcl"
    val username = "fspt"
    val password = "fspt"
    val driver = "oracle.jdbc.driver.OracleDriver"

    val data = sparkSession.sqlContext.read.format("jdbc").options(Map("url" -> url, "dbtable" -> table,
      "user" -> username, "password" -> password, "driver" -> driver)).load().createOrReplaceTempView(table)

    import sparkSession.sql
    sql(s"select * from $table ").show()
    sql(s"select TABLE_NAME,collect_set(concat(COLUMN_NAME ,'|',DATA_TYPE)) FROM $table WHERE TABLE_NAME = " +
      s"'SYS_DD'  group by TABLE_NAME ")
       .foreach(item=>println(item))

  }
}
