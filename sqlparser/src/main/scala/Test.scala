import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2018-08-22
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-08-22     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */
object Test {
  def main(args: Array[String]): Unit = {
    val sql = "SELECT * FROM SRC_INFO"
    val p  = new  SparkSqlParser(SQLConf)
  val pl = p.parsePlan(sql)
  val pe =   p.parseExpression(sql)
    pe
  }
}
