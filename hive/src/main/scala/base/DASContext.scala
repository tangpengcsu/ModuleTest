package base

import org.apache.spark.sql.SparkSession

/** 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang
  * 创建日期：2018-03-29
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-03-29     1.0.1.0    tang      创建
  * ----------------------------------------------------------------
  */
object DASContext
{
  var sparkSession: SparkSession = _
  //val hdfsPath :String = "hdfs://192.168.50.88:9000/task/"
  val hdfsPath: String = "hive/src/main/resources/"
  def createSparkSession(): SparkSession =
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
/*      .config("hive.exec.compress.intermediate", "true")
      .config("mapreduce.map.output.compress", "true")
      .config("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec")*/


      .enableHiveSupport()
      .getOrCreate()
//mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec
    // sparkSession.conf.set("spark.sql.defualt.parallelism","45")
    sparkSession
  }
  def getSparkSession():SparkSession={
    if(null == sparkSession){
      createSparkSession()
    }else{
      sparkSession
    }
  }

  def stop() = sparkSession.stop()

}
