package examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2018-07-13
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-07-13     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */
object TestContext {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test")
    //conf.setAll(sparkParam.config)
    conf.set("spark.sql.crossJoin.enabled", "true")
val    sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
   // sparkSession.sparkContext.setLogLevel(sparkParam.logLevel)
 val   sqlContext = sparkSession.sqlContext

  }
}
