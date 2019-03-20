package udf

import base.DASContext
import org.apache.spark.sql.api.java.{UDF1,UDF2}
import org.apache.spark.sql.types.DataTypes

import scala.util.Random

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2018-05-04
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-05-04     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */
class KdbiRand extends UDF2[String, Int, String] {
  override def call(value: String, n: Int): String = {
    println(s"==========================================${value}-${n}")
    (new Random()).nextInt(n).toString + "_" + value
  }
}

class KdbiSplitRand extends UDF1[String, String] {
  override def call(value: String): String = {
    val index = value.indexOf("_")
    if (index != -1) {
      value.substring(index + 1, value.length - 1)
    } else {
      value
    }
  }
}

object KdbiRand {
  def main(args: Array[String]): Unit = {
    val sparkSession = DASContext.createSparkSession()
    // sparkSession.udf.register("kdbiRand",(input:String) => input.length)
    sparkSession.udf.register("kdbiRand", new KdbiRand, DataTypes.StringType)
    sparkSession.udf.register("KdbiSplitRand", new KdbiSplitRand, DataTypes.StringType)


    import sparkSession.sql
    sql("select kdbiRand(cast(key as string),10) as keys from src").createOrReplaceTempView("tmp_src")
    sql("select * from tmp_src").show()
  sql("select kdbiSplitRand(keys) as blkey from tmp_src").show()


    DASContext.stop()
  }
}
