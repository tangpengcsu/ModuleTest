package examples

import org.apache.spark.sql.SparkSession

/** 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tangpeng
  * 创建日期：2018-03-16
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-03-16     1.0.1.0    tangpeng      创建
  * ----------------------------------------------------------------
  */
object HiveContextInstance
{
  var sparkSession: SparkSession = _
  //val hdfsPath :String = "hdfs://192.168.50.88:9000/"
  val hdfsPath :String = "hive/src/main/resources/"
  def create(): SparkSession =
  {
    val warehouseLocation = "hdfs://192.168.50.88:9000/user/hive/warehouse"
    sparkSession = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[1]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("mapreduce.output.fileoutputformat.outputdir", "/tmp")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()

   // sparkSession.conf.set("spark.sql.defualt.parallelism","45")
    sparkSession
  }

  def stop() = sparkSession.stop()
}
