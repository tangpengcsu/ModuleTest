package examples

import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2018-06-29
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-06-29     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */
case class Person1(name: String, addrname: String)
object TestJDBC {

  def main(args: Array[String]): Unit = {
    test2()
  }
  def test(): Unit = {
    val spark = HiveContextInstance.create()
    import spark.sql
    val tableName = ""
    val oracleDriverUrl = "jdbc:oracle:thin:@192.168.50.85:1521:ORCL"

    val jdbcMap = Map("url" -> oracleDriverUrl,
      "user" -> "kdbase",
      "password" -> "kdbase",
      "dbtable" -> tableName,
      "driver" -> "oracle.jdbc.driver.OracleDriver")
    val OracleDialect = new JdbcDialect {

      override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle") || url.contains("oracle")
      //getJDBCType is used when writing to a JDBC table
      override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
        case StringType => Some(JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR))
        case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.NUMERIC))
        case IntegerType => Some(JdbcType("NUMBER(16)", java.sql.Types.NUMERIC))
        case LongType => Some(JdbcType("NUMBER(16)", java.sql.Types.NUMERIC))
        case DoubleType => Some(JdbcType("NUMBER(16,4)", java.sql.Types.NUMERIC))
        case FloatType => Some(JdbcType("NUMBER(16,4)", java.sql.Types.NUMERIC))
        case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.NUMERIC))
        case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.NUMERIC))
        case BinaryType => Some(JdbcType("BLOB", java.sql.Types.BLOB))
        case TimestampType => Some(JdbcType("DATE", java.sql.Types.DATE))
        case DateType => Some(JdbcType("DATE", java.sql.Types.DATE))
        //        case DecimalType.Fixed(precision, scale) => Some(JdbcType("NUMBER(" + precision + "," + scale + ")", java.sql.Types.NUMERIC))
        //case DecimalType.Unlimited => Some(JdbcType("NUMBER(38,4)", java.sql.Types.NUMERIC))
        case _ => None
      }
    }

    //Registering the OracleDialect
    //JdbcDialects.registerDialect(OracleDialect)

    val connectProperties = new Properties()
    connectProperties.put("user", "GSPWJC")
    connectProperties.put("password", "GSPWJC")
    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
    val  aDataFrame = sql("")
    //write back Oracle
    //Note: When writing the results back orale, be sure that the target table existing
    JdbcUtils.saveTable(aDataFrame, oracleDriverUrl, "tableName", connectProperties)
    aDataFrame.write
    HiveContextInstance.stop()
  }

  def test2():Unit={
    val spark = HiveContextInstance.create()
    import spark.sql
    val tableName = "persontest"
    val oracleDriverUrl = "jdbc:oracle:thin:@192.168.50.85:1521:ORCL"
    val sqlSserverUrl = "jdbc:sqlserver://192.168.50.98:1433;DatabaseName=bi_config"

    val OracleDialect = new JdbcDialect {

      override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle") || url.contains("oracle")
      //getJDBCType is used when writing to a JDBC table
      override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
        case StringType => Some(JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR))
        case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.NUMERIC))
        case IntegerType => Some(JdbcType("NUMBER(16)", java.sql.Types.NUMERIC))
        case LongType => Some(JdbcType("NUMBER(16)", java.sql.Types.NUMERIC))
        case DoubleType => Some(JdbcType("NUMBER(16,4)", java.sql.Types.NUMERIC))
        case FloatType => Some(JdbcType("NUMBER(16,4)", java.sql.Types.NUMERIC))
        case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.NUMERIC))
        case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.NUMERIC))
        case BinaryType => Some(JdbcType("BLOB", java.sql.Types.BLOB))
        case TimestampType => Some(JdbcType("DATE", java.sql.Types.DATE))
        case DateType => Some(JdbcType("DATE", java.sql.Types.DATE))
        //        case DecimalType.Fixed(precision, scale) => Some(JdbcType("NUMBER(" + precision + "," + scale + ")", java.sql.Types.NUMERIC))
        //case DecimalType.Unlimited => Some(JdbcType("NUMBER(38,4)", java.sql.Types.NUMERIC))
        case _ => None
      }
    }

    //Registering the OracleDialect
  // JdbcDialects.registerDialect(OracleDialect)
    val connectProperties = new Properties()
/*    connectProperties.put("user", "kdbase")
    connectProperties.put("password", "kdbase")
    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()*/
    connectProperties.put("user", "sa")
    connectProperties.put("password", "12345678")
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val persons = new scala.collection.mutable.ListBuffer[Person1]

    val names = Array("Runoob", "Baidu", "Google")
    names.foreach(name => {
      persons.append(Person1(name, name + "addr"))
    })
    import spark.implicits._

    val  aDataFrame = persons.toDF()
    //aDataFrame.show()
    //write back Oracle
    //Note: When writing the results back orale, be sure that the target table existing

    JdbcUtils.saveTable(aDataFrame, sqlSserverUrl, tableName, connectProperties)

    HiveContextInstance.stop()
  }
}
