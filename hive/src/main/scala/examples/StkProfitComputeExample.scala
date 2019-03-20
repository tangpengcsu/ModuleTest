package examples

import org.apache.spark.sql.{SaveMode, SparkSession}

/** 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang
  * 创建日期：2018-03-20
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-03-20     1.0.1.0    tang      创建
  * ----------------------------------------------------------------
  */
object StkProfitComputeExample
{
  def main(args: Array[String]): Unit =
  {
    val sparkSession = HiveContextInstance.create

    args(0).toUpperCase match
    {
      case "LOADDATA" => loadData(sparkSession)
      case "SQLPRO" => sqlPro(sparkSession)
      case "SQLPRO1" => sqlPro1(sparkSession)
      case _ =>
    }


    HiveContextInstance.stop
  }

  def loadData(sparkSession: SparkSession): Unit =
  {
    import sparkSession.sql
    sql(
      """
        |CREATE TABLE IF NOT EXISTS ST_DD_STOCK_PROFIT (
        |  CUSTID STRING,
        |  MONEYTYPE STRING,
        |  STKTYPE STRING,
        |  MARKET STRING,
        |  STKCODE STRING,
        |  MKTVAL DOUBLE,
        |  PRE_MKTVAL DOUBLE,
        |  PROFITCOST DOUBLE,
        |  PRE_PROFITCOST DOUBLE,
        |  ADDCOST DOUBLE,
        |  PROFIT_DAY DOUBLE,
        |  DCDATE INT
        |)
      """.stripMargin)



    sql("LOAD DATA LOCAL INPATH 'hive/src/main/resources/ST_DD_STOCK_PROFIT.txt' OVERWRITE  INTO TABLE " +
      "ST_DD_STOCK_PROFIT")

    sql("select * from ST_DD_STOCK_PROFIT").show()
    /*
    * |  custid|moneytype|stktype|market|stkcode|  mktval|pre_mktval|profitcost|pre_profitcost|addcost|profit_day|  dcdate|
      +--------+---------+-------+------+-------+--------+----------+----------+--------------+-------+----------+--------+
      |19089888|        0|      1|     0|  50070| 13.7566|   54.8865|      35.0|          62.0|   89.0|      65.0|20180806|
      |19089888|        0|      1|     0|  50070|     1.0|       1.0|       1.0|           1.0|    1.0|       1.0|20180806|
      |19089889|        0|      1|     0|  50071|113.7566|  154.8865|     135.0|         162.0|  189.0|     165.0|20180806|
      |19089889|        0|      1|     0|  50071|     2.0|       2.0|       2.0|           2.0|    2.0|       2.0|20180806|
      +--------+---------+-------+------+-------+--------+----------+----------+--------------+-------+----------+--------+
    * */

    //创建目标表
    sql(
      """
        |CREATE TABLE IF NOT EXISTS ST_DD_CUST_STKPROFIT(
        |   CUSTID STRING,
        |   MONEYTYPE STRING,
        |   STKTYPE STRING,
        |   MARKET STRING,
        |   STKCODE STRING,
        |   MKTVAL DOUBLE,
        |   PRE_MKTVAL DOUBLE,
        |   PROFITCOST DOUBLE ,
        |   ADDCOST DOUBLE,
        |   PROFIT DOUBLE ,
        |   STK_PROFITRATE DOUBLE
        |)
      """.stripMargin)
  }

  def sqlPro1(sparkSession: SparkSession): Unit =
  {
    val date: Int = 20180806
    import sparkSession.sql
    val data = sql(
      s"""
        |SELECT S.CUSTID,
        |       S.MONEYTYPE,
        |       S.STKTYPE,
        |       S.MARKET,
        |       S.STKCODE,
        |       S.MKTVAL,
        |       S.PRE_MKTVAL,
        |       S.PROFITCOST,
        |       S.PRE_PROFITCOST,
        |       S.ADDCOST,
        |       S.PROFIT,
        |       CASE
        |         WHEN (S.PRE_MKTVAL + S.ADDCOST) = 0 THEN
        |          0
        |         ELSE
        |          CAST(1.0 * S.PROFIT / (S.PRE_MKTVAL + ADDCOST) * 100 AS DECIMAL(20, 4))
        |       END AS STK_PROFITRATE
        |  FROM (SELECT CUSTID,
        |               MONEYTYPE,
        |               STKTYPE,
        |               MARKET,
        |               STKCODE,
        |               SUM(MKTVAL) AS MKTVAL,
        |               SUM(PRE_MKTVAL) AS PRE_MKTVAL,
        |               SUM(PROFITCOST) AS PROFITCOST,
        |               SUM(PRE_PROFITCOST) AS PRE_PROFITCOST,
        |               SUM(ADDCOST) AS ADDCOST,
        |               SUM(PROFIT_DAY) AS PROFIT
        |          FROM ST_DD_STOCK_PROFIT
        |         WHERE DCDATE = $date
        |         GROUP BY CUSTID, MONEYTYPE, STKTYPE, MARKET, STKCODE) s
      """.stripMargin
    )
    //data.show()

    data.coalesce(2).write.mode(SaveMode.Overwrite).saveAsTable("ST_DD_CUST_STKPROFIT")
    /*
* +--------+---------+-------+------+-------+--------+----------+----------+--------------+-------+------+--------------+
  |  CUSTID|MONEYTYPE|STKTYPE|MARKET|STKCODE|  MKTVAL|PRE_MKTVAL|PROFITCOST|PRE_PROFITCOST|ADDCOST|PROFIT|STK_PROFITRATE|
  +--------+---------+-------+------+-------+--------+----------+----------+--------------+-------+------+--------------+
  |19089888|        0|      1|     0|  50070| 14.7566|   55.8865|      36.0|          63.0|   90.0|  66.0|       45.2406|
  |19089889|        0|      1|     0|  50071|115.7566|  156.8865|     137.0|         164.0|  191.0| 167.0|       48.0042|
  +--------+---------+-------+------+-------+--------+----------+----------+--------------+-------+------+--------------+
* */
  }

  def sqlPro(sparkSession: SparkSession): Unit =
  {
    val date: Int = 20180806
    val stkProfit = s"SELECT CUSTID,MONEYTYPE,STKTYPE,MARKET,STKCODE," +
      s" SUM(MKTVAL) AS MKTVAL, " +
      s" SUM(PRE_MKTVAL) AS PRE_MKTVAL," +
      s" SUM(PROFITCOST) AS PROFITCOST," +
      s" SUM(PRE_PROFITCOST) AS PRE_PROFITCOST," +
      s" SUM(ADDCOST) AS ADDCOST," +
      s" SUM(PROFIT_DAY) AS PROFIT" +
      s" FROM ST_DD_STOCK_PROFIT" +
      s" WHERE DCDATE = " + date +
      s" GROUP BY CUSTID,MONEYTYPE,STKTYPE,MARKET,STKCODE"

    import sparkSession.sql
    // 原数据
    val stkProfitDs = sql(stkProfit)


    //保存数据到目标表
    stkProfitDs.selectExpr("CUSTID", "MONEYTYPE", "STKTYPE", "MARKET", "STKCODE",
      "CAST(MKTVAL AS DECIMAL(20,4))", "CAST(PRE_MKTVAL AS DECIMAL(20,4))", "CAST(PROFITCOST AS DECIMAL(20,4))",
      "CAST(ADDCOST AS DECIMAL(20,4))", "CAST(PROFIT AS DECIMAL(20,4))",
      "CASE WHEN (PRE_MKTVAL+ADDCOST) = 0 THEN 0 ELSE CAST(1.0*PROFIT/(PRE_MKTVAL+ADDCOST)*100 AS DECIMAL(20,4)) END " +
        "STK_PROFITRATE ").coalesce(2).write.mode(SaveMode.Append).saveAsTable("ST_DD_CUST_STKPROFIT")

    sql("select * from ST_DD_CUST_STKPROFIT").show()
    /*
    * +--------+---------+-------+------+-------+--------+----------+----------+--------+--------+--------------+
      |  CUSTID|MONEYTYPE|STKTYPE|MARKET|STKCODE|  MKTVAL|PRE_MKTVAL|PROFITCOST| ADDCOST|  PROFIT|STK_PROFITRATE|
      +--------+---------+-------+------+-------+--------+----------+----------+--------+--------+--------------+
      |19089888|        0|      1|     0|  50070| 14.7566|   55.8865|   36.0000| 90.0000| 66.0000|       45.2406|
      |19089889|        0|      1|     0|  50071|115.7566|  156.8865|  137.0000|191.0000|167.0000|       48.0042|
      +--------+---------+-------+------+-------+--------+----------+----------+--------+--------+--------------+
    * */

    //注册临时表
    /*   val tmpTable = stkProfitDs.createOrReplaceTempView("tmp")
       sql("select CUSTID,MONEYTYPE,STKTYPE,MARKET,STKCODE, CAST(MKTVAL AS DECIMAL(20,4)),CAST(PRE_MKTVAL AS DECIMAL(20," +
         "4)),CAST(PROFITCOST AS DECIMAL(20,4)),CAST(ADDCOST AS DECIMAL(20,4)),CAST(PROFIT AS DECIMAL(20,4))," +
         "CASE WHEN (PRE_MKTVAL+ADDCOST) = 0 THEN 0 ELSE CAST(1.0*PROFIT/(PRE_MKTVAL+ADDCOST)*100 AS DECIMAL(20,4)) END  " +
         "STK_PROFITRATE FROM tmp").show()*/
  }
}
