package examples

import org.apache.spark.sql.{SaveMode, SparkSession}
//import com.szkingdom.parse.Parse
/** 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tangpeng
  * 创建日期：2018-03-06
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-03-06     1.0.1.0    tangpeng      创建
  * ----------------------------------------------------------------
  */
object StkProfitCompute
{
  def main(args: Array[String]): Unit =
  {
    val warehouseLocation = "hdfs://192.168.50.88:9000/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[1]")
      .config("spark.sql.warehouse.dir", warehouseLocation)

      .enableHiveSupport()
      .getOrCreate()
/*
    spark.sql("SET -v").show()
    spark.sql("SET -v").toDF().foreach(item=>{

      println(item.getAs[String]("key")+"="+item.getAs[String]("value"))
    })
   //spark.sql("SET -v").toDF().filter($"age" > 15)
    spark.sql("SET spark.sql.sources.bucketing.enabled = false")
    spark.sql("SET -v").toDF().foreach(item=>{

      println(item.getAs[String]("key")+"="+item.getAs[String]("value"))
    })
*/


    loadData(spark)
    //custStkProfitExample(spark)
   //dynamicExample(spark)

    spark.stop()
   // spark.sparkContext.parallelize(1 to 2,2)
    //spark.sparkContext.setCheckpointDir("")
  }

  /**
    * 装载数据
    *
    * @param spark
    */
  private def loadData(spark: SparkSession): Unit =
  {
    import spark.sql
    //sql("DROP TABLE ST_DD_STOCK_PROFIT")
    sql(s"CREATE TABLE IF NOT EXISTS ST_DD_STOCK_PROFIT (CUSTID STRING,MONEYTYPE STRING,STKTYPE STRING,MARKET STRING," +
      s"STKCODE STRING,MKTVAL DOUBLE,PRE_MKTVAL DOUBLE,PROFITCOST DOUBLE,PRE_PROFITCOST DOUBLE,ADDCOST DOUBLE," +
      s"PROFIT_DAY DOUBLE,DCDATE INT)")
    /*    sql(s"CREATE TABLE IF NOT EXISTS ST_DD_STOCK_PROFIT (CUSTID STRING,MONEYTYPE STRING,STKTYPE STRING,MARKET STRING," +
          s"STKCODE STRING) ")*/

    sql("LOAD DATA LOCAL INPATH 'hive/src/main/resources/ST_DD_STOCK_PROFIT.txt' INTO TABLE ST_DD_STOCK_PROFIT")

    sql("select * from ST_DD_STOCK_PROFIT").show()
  }
  case class Person(name: String, name2: String,name3:String)

  def dynamicExample(spark: SparkSession):Unit ={
    val date: Int = 20180806
    import spark.implicits._
    import spark.sql
    sql("create table if not exists ST_DD_STOCK_PROFIT_TMP(id String, profit_rate double)")
    val df = sql("select * from ST_DD_STOCK_PROFIT ")
    println("test========================")

 /*   df.where($"DCDATE" === date).groupBy(df.col("CUSTID"), df.col("MONEYTYPE"), df.col("STKTYPE"), df.col("MARKET"), df
      .col("STKCODE")).agg(Map("MKTVAL" -> "sum", "PRE_MKTVAL" -> "sum")).show()*/


    import org.apache.spark.sql.functions._
    val aggexp = df.where($"DCDATE" === date)
      .groupBy(df.col("CUSTID"), df.col("MONEYTYPE"), df.col("STKTYPE"), df.col("MARKET"), df.col("STKCODE")).
      agg(sum("MKTVAL") as "MKTVAL", sum("PRE_MKTVAL") as "PRE_MKTVAL", sum("PROFITCOST") as "PROFITCOST",
        sum("PRE_PROFITCOST") as "PRE_PROFITCOST", sum("ADDCOST") as "ADDCOST", sum("PROFIT_DAY") as "PROFIT")
    val ss= aggexp.map(item=>{
      var stkProfitRate:Double = 0

      val custId=item.getAs[String]("CUSTID")
      val preMktVal=item.getAs[Double]("PRE_MKTVAL")
      val addCost=item.getAs[Double]("ADDCOST")
      val profit=item.getAs[Double]("PROFIT")
      if (preMktVal.+(addCost).==(0))
      {
        stkProfitRate = 0
      } else
      {
        stkProfitRate = 1.0.*(profit)./(preMktVal.+(addCost)).*(100)
      }
      //item.toSeq.:+(stkProfitRate.formatted("%18.4f"))
     (custId,stkProfitRate.formatted("%18.4f"))
    }).toDF().write.mode(SaveMode.Append).insertInto("ST_DD_STOCK_PROFIT_TMP")
     //.toDF("id","profit_rate").createOrReplaceTempView("tmp")

    //sql("insert into ST_DD_STOCK_PROFIT_TMP select id,profit_rate from tmp")
    sql("select * from ST_DD_STOCK_PROFIT_TMP").show()

  }


  def custStkProfitExample(spark: SparkSession): Unit =
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

    val custProfitTableDefine = s"CREATE TABLE IF NOT EXISTS ST_DD_CUST_STKPROFIT(" +
      s"CUSTID STRING,MONEYTYPE STRING,STKTYPE STRING,MARKET STRING,STKCODE STRING," +
      s"MKTVAL DOUBLE, PRE_MKTVAL DOUBLE, PROFITCOST DOUBLE ,ADDCOST DOUBLE," +
      s"PROFIT DOUBLE ,STK_PROFITRATE DOUBLE" +
      s")"
    import spark.sql

    //创建目标表
    sql(custProfitTableDefine)
    // 原数据
    val stkProfitDs = sql(stkProfit)

   // stkProfitDs.coalesce(1);
    //stkProfitDs.map(t=>t.getAs[String]("name")).show()
    /*val s = stkProfitDs.as[Person].map(teenager =>
    {
      teenager.name
      teenager
    })
    s.toDF().write.mode(SaveMode.Append).saveAsTable("xxxx")*/
    //保存数据到目标表
    stkProfitDs.selectExpr("CUSTID","MONEYTYPE","STKTYPE","MARKET","STKCODE",
       "CAST(MKTVAL AS DECIMAL(20,4))","CAST(PRE_MKTVAL AS DECIMAL(20,4))","CAST(PROFITCOST AS DECIMAL(20,4))",
       "CAST(ADDCOST AS DECIMAL(20,4))","CAST(PROFIT AS DECIMAL(20,4))",
       "CASE WHEN (PRE_MKTVAL+ADDCOST) = 0 THEN 0 ELSE CAST(1.0*PROFIT/(PRE_MKTVAL+ADDCOST)*100 AS DECIMAL(20,4)) END " +
         "STK_PROFITRATE ").coalesce(2).write.mode(SaveMode.Append).saveAsTable("ST_DD_CUST_STKPROFIT")

    sql("select * from ST_DD_CUST_STKPROFIT").show()

    //注册临时表
 /*   val tmpTable = stkProfitDs.createOrReplaceTempView("tmp")
    sql("select CUSTID,MONEYTYPE,STKTYPE,MARKET,STKCODE, CAST(MKTVAL AS DECIMAL(20,4)),CAST(PRE_MKTVAL AS DECIMAL(20," +
      "4)),CAST(PROFITCOST AS DECIMAL(20,4)),CAST(ADDCOST AS DECIMAL(20,4)),CAST(PROFIT AS DECIMAL(20,4))," +
      "CASE WHEN (PRE_MKTVAL+ADDCOST) = 0 THEN 0 ELSE CAST(1.0*PROFIT/(PRE_MKTVAL+ADDCOST)*100 AS DECIMAL(20,4)) END  " +
      "STK_PROFITRATE FROM tmp").show()*/
  }


  def complexCompute (spark: SparkSession):Unit = {
    import spark.sql
    val yesterday:Int = 20180805
    val toDay:Int  = 20180806
    //创建临时表；取今天与昨天的资产与净资产 temp_asset
    val assetQry = s"select t1.custid," +
      s"sum(tt.asset) asset, " +
      s"sum(tt.debt) debt, " +
      s"sum(tt.pledge) pledge, " +
      s"sum(tt.net_asset) net_asset, " +
      s"sum(tt.prenet_asset) preasset " +
      s"from st_dd_custinfo t1, " +
      s"(select t.custid, " +
      s"sum(t.asset) asset, " +
      s"sum(t.debt) debt, " +
      s"sum(t.pledge_value) pledge, " +
      s"sum(t.net_asset) net_asset, " +
      s"0 as prenet_asset " +
      s"from st_dd_asset t " +
      s"where t.dcdate =  " +toDay +
      s"group by t.custid " +
      s"union all " +
      s"select a.custid, 0, 0, 0, 0, sum(a.net_asset) as preasset"+
        s"from st_dd_asset a " +
      s"where a.dcdate = " +yesterday +
      s"group by a.custid " +
      s"union all " +
      s"select b.old_custid, 0, 0, 0, 0, 0 " +
      s"from cfg_custchg b " +
      s"where b.chg_date =  "+yesterday + ") tt " +
      s"where t1.custid = tt.custid " +
      s"and t1.dcdate =  " + toDay +
      s"and t1.status = 0  " +
      s"group by t1.custid distributed by(custid)"
    //创建资金转入转出临时表 temp_fundchg
    val fundQry = s" select custid, " +
      s"sum(fundeffect_rmb) fundeffect, " +
      s"sum(case when zrzctype = 1 then fundeffect_rmb else 0 end) infundeffect, " +
      s"sum(case when zrzctype = 2 then fundeffect_rmb else 0 end) outfundeffect " +
      s"from st_dd_fundchg " +
      s"where dcdate =  " + toDay +
      s"group by custid " +
      s"distributed by (custid) "
    //资金转入例外处理历史表   temp_fundchg_adj
    val fundAdjQry = s"select custid,filterflag, " +
      s"sum(fundeffect_rmb) fundeffect, " +
      s"sum(case when fundeffect_rmb > 0 then fundeffect_rmb else 0 end) infundeffect, " +
      s"sum(case when fundeffect_rmb < 0    then fundeffect_rmb else 0 end) outfundeffect " +
      s"from st_dd_fundchg_adj " +
      s"where dcdate =  " + toDay +
      s"group by custid, filterflag " +
      s"distributed by(custid) "
    //创建股份转入转出临时表 temp_stkchg
    val  stkchgQry = ""
    //--创建股份转入例外处理历史表 temp_stkchg_adj
    val stkchgAdjQry =""
    //限售股汇总到资金帐号 temp_untrdstkasset_details
    val untrdstkassetDetailsQry =""
    //取今天与昨天的资产与净资产 temp_asset
    val asset = sql(assetQry).createOrReplaceTempView("temp_asset")
    //创建资金转入转出临时表 temp_fundchg
    val fund = sql(fundQry).createOrReplaceTempView("temp_fundchg")
    //资金转入例外处理历史表   temp_fundchg_adj
    val fundAdj = sql(fundAdjQry).createOrReplaceTempView("temp_fundchg_adj")
    //创建股份转入转出临时表 temp_stkchg
    val stkchg = sql(stkchgQry).createOrReplaceTempView("temp_stkchg")
    //--创建股份转入例外处理历史表 temp_stkchg_adj
    val stkchgAdj = sql(stkchgAdjQry).createOrReplaceTempView("temp_stkchg_adj")
    //限售股汇总到资金帐号
    val untrdstkassetDetails = sql(untrdstkassetDetailsQry).createOrReplaceTempView("temp_untrdstkasset_details")

    //------------计算
    //累计盈亏与转入转出
    val computeAsset = ""

/*    select t.custid,
    t.asset,t.debt,t.pledge,
    t.net_asset,                                                                     --净资产
      t.preasset,                                                                      --昨净资产
      coalesce(a.infundeffect,0)+coalesce(b.instkeffect,0)+coalesce(a1.infundeffect,0) + coalesce(b1.instkeffect,0) as ineffect,                                      --转入资金
      coalesce(a.outfundeffect,0)+coalesce(b.outstkeffect,0)+coalesce(a1.outfundeffect,0) + coalesce(b1.outstkeffect,0) as outeffect,
    t.net_asset - (t.preasset + coalesce(a.fundeffect,0) + coalesce(b.stkeffect,0)+coalesce(a1.fundeffect,0)+coalesce(b1.stkeffect,0)) profit,  --盈亏,
    t.preasset + coalesce(a.infundeffect,0) + coalesce(b.instkeffect,0) + coalesce(a1.infundeffect,0)+coalesce(b1.instkeffect,0)  begin_asset --昨资产+转入资产
    from temp_asset t
    left join temp_fundchg a        -- 资金转入转出
      on t.custid = a.custid
    left join temp_stkchg b         -- 股份转入转出
      on t.custid = b.custid
    left join temp_fundchg_adj a1   -- 资金转入转出调整表
      on t.custid = a1.custid
    and a1.filterflag = 0
    left join temp_stkchg_adj b1    -- 股份转入转出调整表
      on t.custid = b1.custid
    and b1.filterflag = 0
    distributed by (custid)*/
    val result = sql(computeAsset)


  }
}
