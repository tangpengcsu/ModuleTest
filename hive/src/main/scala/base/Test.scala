package base

import examples.HiveContextInstance
import org.apache.spark.sql.SaveMode

import scala.collection.mutable.ListBuffer

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
object Test
{
  def main(args: Array[String]): Unit =
  {
    println("======入参:文件名："+args(0))
    val sparkSession = DASContext.createSparkSession()
  /*  val s = sparkSession.sql("select * from src")
    println("-------------------------"+sparkSession.table("src").columns.mkString(","))*/
    test()

    /*val ss = s.head(1)
    if(ss == null ){
      println("===null")
    }else if(ss.length<1){
      println(s"=size==${ss.length}")
    }

    else {
      println("not null")
    }*/
    //readJson
   // ScheduleTask.doTask(args(0))
    //testSaveAsCVS
  // testCache
  //  testSnappy
    DASContext.stop()
  }
  def test():Unit={
    val sparkSession = DASContext.getSparkSession()
    import sparkSession.sql
    val ddTmpHql =
      """
        |SELECT T.SSYS_DICT_CD   ,
        |    T.SSYS_DICTEN_CD ,--源系统字典项代码
        |       T.SYS_SRC        ,--系统来源
        |       T.MDL_DICT_CD    ,--模型字典代码
        |       T.MDL_DICTEN_CD   --模型字典项代码
        |  FROM DW.CODE_DICT_CODE T
      """.stripMargin
    sql(ddTmpHql).createOrReplaceTempView("TEMP_DICT_CODE")

    val incTmpHql =
      """
        |SELECT
        |  T1.BANK_ACCT       AS BNK_ACC_ID     ,
        |  T1.INT_ORG         AS BNK_ID         ,
        |  T3.MDL_DICTEN_CD   AS CRRC_CD        ,
        |  '04'               AS BNK_ACC_CLAS_CD,
        |  T2.USER_NAME       AS ACC_NAME       ,
        |  T1.CUST_CODE       AS CUST_ID        ,
        |  T4.MDL_DICTEN_CD   AS CERT_TYPE_CD   ,
        |  T2.ID_CODE         AS CERT_NUM       ,
        |  T1.CUACCT_CODE     AS CPTL_ACC_ID    ,
        |  ''                 AS BNK_ACC_USE_CD ,
        |  ''                 AS PAY_MODE_CD    ,
        |  ''                 AS PAY_ORG_ID     ,
        |  ''                 AS SIGN_TYPE_CD   ,
        |  T5.MDL_DICTEN_CD   AS SIGN_STAT_CD   ,
        |  T1.ACCT_DRAW_UPLMT AS OUT_TOPL       ,
        |  0                  AS OUT_LOWL       ,
        |  0                  AS TAC_LMT        ,
        |  T1.ACCT_DRAWN_AMT  AS USD_AC_LMT
        |FROM
        |  ODS.KUAS_CUBSB_CONTRACT T1
        |  LEFT JOIN ODS.KUAS_USERS T2 ON T1.CUST_CODE=T2.USER_CODE
        |  LEFT JOIN TEMP_DICT_CODE T3 ON T1.CURRENCY = T3.SSYS_DICTEN_CD AND T3.SYS_SRC ='KUAS' AND T3.MDL_DICT_CD ='DIMLS009'
        |  LEFT JOIN TEMP_DICT_CODE T4 ON T2.ID_TYPE = T4.SSYS_DICTEN_CD AND T4.SYS_SRC ='KUAS' AND T4.MDL_DICT_CD ='DIMLS259'
        |  LEFT JOIN TEMP_DICT_CODE T5 ON T1.CONTRACT_STATUS = T5.SSYS_DICTEN_CD AND T5.SYS_SRC ='KUAS' AND T5.MDL_DICT_CD ='DIMLS163'
        |UNION ALL
        |SELECT
        |  T1.BANK_ACCT       AS BNK_ACC_ID     ,
        |  T1.BANK_CODE       AS BNK_ID         ,
        |  '0'                AS CRRC_CD        ,
        |  T3.MDL_DICTEN_CD   AS BNK_ACC_CLAS_CD,
        |  T2.USER_NAME       AS ACC_NAME       ,
        |  T1.CUST_CODE       AS CUST_ID        ,
        |  T4.MDL_DICTEN_CD   AS CERT_TYPE_CD   ,
        |  T2.ID_CODE         AS CERT_NUM       ,
        |  T1.CUACCT_CODE     AS CPTL_ACC_ID    ,
        |  ''                 AS BNK_ACC_USE_CD ,
        |  ''                 AS PAY_MODE_CD    ,
        |  T1.PAY_ORG         AS PAY_ORG_ID     ,
        |  ''                 AS SIGN_TYPE_CD   ,
        |  ''                 AS SIGN_STAT_CD   ,
        |  0                  AS OUT_TOPL       ,
        |  0                  AS OUT_LOWL       ,
        |  0                  AS TAC_LMT        ,
        |  0                  AS USD_AC_LMT
        |FROM
        |  ODS.KUAS_OTC_CUST_PAYER T1
        |  LEFT JOIN ODS.KUAS_USERS T2 ON T1.CUST_CODE=T2.USER_CODE
        |  LEFT JOIN TEMP_DICT_CODE T3 ON T1.BANK_ACCT_TYPE = T3.SSYS_DICTEN_CD AND T3.SYS_SRC ='KUAS' AND T3.MDL_DICT_CD ='DIMLS229'
        |  LEFT JOIN TEMP_DICT_CODE T4 ON T2.ID_TYPE = T4.SSYS_DICTEN_CD AND T4.SYS_SRC ='KUAS' AND T4.MDL_DICT_CD ='DIMLS259'
      """.stripMargin

    sql(incTmpHql).createOrReplaceTempView("ACC_BNK_ACC_t_date")
    sparkSession.sqlContext.cacheTable("ACC_BNK_ACC_t_date")
   ///sql(incTmpHql).write.mode(SaveMode.Overwrite).saveAsTable("ACC_BNK_ACC_t_date_Test")

    val zipperHql =
      """
        |select d.BNK_ACC_ID,
        |       d.BNK_ID,
        |       d.CRRC_CD,
        |       d.BNK_ACC_CLAS_CD,
        |       d.ACC_NAME,
        |       d.CUST_ID,
        |       d.CERT_TYPE_CD,
        |       d.CERT_NUM,
        |       d.CPTL_ACC_ID,
        |       d.BNK_ACC_USE_CD,
        |       d.PAY_MODE_CD,
        |       d.PAY_ORG_ID,
        |       d.SIGN_TYPE_CD,
        |       d.SIGN_STAT_CD,
        |       d.OUT_TOPL,
        |       d.OUT_LOWL,
        |       d.TAC_LMT,
        |       d.USD_AC_LMT
        |  from (select md5(concat(case when BNK_ACC_ID is null then '^' else BNK_ACC_ID end, ',' ,case when BNK_ID is null then '^' else BNK_ID end, ',' ,case when CRRC_CD is null then '^' else CRRC_CD end, ',' ,case when BNK_ACC_CLAS_CD is null then '^' else BNK_ACC_CLAS_CD end, ',' ,case when ACC_NAME is null then '^' else ACC_NAME end, ',' ,case when CUST_ID is null then '^' else CUST_ID end, ',' ,case when CERT_TYPE_CD is null then '^' else CERT_TYPE_CD end, ',' ,case when CERT_NUM is null then '^' else CERT_NUM end, ',' ,case when CPTL_ACC_ID is null then '^' else CPTL_ACC_ID end, ',' ,case when BNK_ACC_USE_CD is null then '^' else BNK_ACC_USE_CD end, ',' ,case when PAY_MODE_CD is null then '^' else PAY_MODE_CD end, ',' ,case when PAY_ORG_ID is null then '^' else PAY_ORG_ID end, ',' ,case when SIGN_TYPE_CD is null then '^' else SIGN_TYPE_CD end, ',' ,case when SIGN_STAT_CD is null then '^' else SIGN_STAT_CD end, ',' ,case when OUT_TOPL is null then '^' else OUT_TOPL end, ',' ,case when OUT_LOWL is null then '^' else OUT_LOWL end, ',' ,case when TAC_LMT is null then '^' else TAC_LMT end, ',' ,case when USD_AC_LMT is null then '^' else USD_AC_LMT end))
        |      as md_key ,
        |      BNK_ACC_ID,
        |       BNK_ID,
        |       CRRC_CD,
        |       BNK_ACC_CLAS_CD,
        |       ACC_NAME,
        |       CUST_ID,
        |       CERT_TYPE_CD,
        |       CERT_NUM,
        |       CPTL_ACC_ID,
        |       BNK_ACC_USE_CD,
        |       PAY_MODE_CD,
        |       PAY_ORG_ID,
        |       SIGN_TYPE_CD,
        |       SIGN_STAT_CD,
        |       OUT_TOPL,
        |       OUT_LOWL,
        |       TAC_LMT,
        |       USD_AC_LMT
        |
        |  from ACC_BNK_ACC_t_date) d
        |left outer join
        |     (select md5(concat(BNK_ACC_ID,BNK_ID,CRRC_CD,BNK_ACC_CLAS_CD,ACC_NAME,CUST_ID,CERT_TYPE_CD,CERT_NUM,CPTL_ACC_ID,BNK_ACC_USE_CD,PAY_MODE_CD,PAY_ORG_ID,SIGN_TYPE_CD,SIGN_STAT_CD,OUT_TOPL,OUT_LOWL,TAC_LMT,USD_AC_LMT))
        |                     as md_key, BNK_ACC_ID from DW.ACC_BNK_ACC where  end_date = '2999-12-31')  h
        |on d.md_key = h.md_key
      """.stripMargin
    sql(zipperHql).write.mode(SaveMode.Overwrite).insertInto("DW.ACC_BNK_ACC")
  }

  def testSnappy(): Unit ={
    val sparkSession = DASContext.getSparkSession()
    import sparkSession.sql
    //sql("insert overwrite table test14 partition(key = 16) select value from src")
    sql("select value,key from src").write.mode(SaveMode.Overwrite).insertInto("test14")
  }

  def testCache()={
    val sparkSession = DASContext.getSparkSession()
    import sparkSession.sql
    sql("select * from src").createOrReplaceTempView("cache_src")
    sparkSession.sqlContext.table("cache_src").schema.fieldNames.foreach(println(_))

    /*sparkSession.sqlContext.cacheTable("cache_src")
    sql("select * from cache_src").createOrReplaceTempView("tmp_src")
    sparkSession.sqlContext.uncacheTable("cache_src")
    //sql("select * from tmp_src").show(10000)
    sparkSession.sqlContext.table("tmp_src").schema.fieldNames.foreach(println(_))*/

  }
  def testSaveAsCVS():Unit={
    val sparkSession = DASContext.getSparkSession()
    import org.apache.spark.sql.SaveMode
    sparkSession
      .sql("select * from src")
      .write
      .mode(SaveMode
        .Append)
      .format("com.databricks.spark.csv")
      .option("header", false)
/*      .option("codec", "org.apache.hadoop.io.compress.SnappyCodec")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")*/
      .save("hdfs://192.168.50.88:9000/tmp/src5")
  }
  def loadData(): Unit ={
    val sparkSession = DASContext.getSparkSession()
    import sparkSession.implicits._

    sparkSession.sparkContext.parallelize(1 to 20).toDF()
  }

  def doMutiTask(fileName:String): Unit =
  {

    val path = s"${DASContext.hdfsPath}${fileName}"
    val sparkSession = DASContext.getSparkSession()
    import sparkSession.implicits._
    val taskInfoDs = sparkSession.read.json(path).as[TaskInfo]

    taskInfoDs.collect().foreach(taskInfo =>
    {
      println(s"====sql:${taskInfo.sql} || ${taskInfo.taskType} || ${taskInfo.tableInfo.tableName} || ${
        taskInfo          .tableInfo.partitionNum
      } || ${taskInfo.tableInfo.saveMode}")
      DASBase.apply(taskInfo)
    })

  }
  def readJson():Unit = {
/*    val s =
      """
        |{
        |    "taskInfo": [{
        |            "id": "1",
        |            "sql": "select custid,$moneytype,stktype,market,stkcode,cast(sum(mktval) as   decimal(20,4)) mktval,cast(sum(pre_mktval) as decimal(20,4)) pre_mktval,cast(sum(profitcost) as decimal(20,4)) profitcost,  cast(sum(pre_profitcost) as decimal(20,4)) pre_profitcost, cast(sum(addcost) as decimal(20,4)) addcost, cast(sum(profit_day) as decimal(20,4)) profit  from st_dd_stock_profit  where dcdate = $dcdate  group by custid,$moneytype,stktype,market,stkcode",
        |            "taskType": "1",
        |            "targetTable": "TMP_RESULT",
        |            "partitionNum": "-1",
        |            "saveMode": "1",
        |            "inputParam": [{
        |                "name": "dcdate",
        |                "value": "20180808"
        |            }, {
        |                "name": "Baidu",
        |                "value": "http://www.baidu.com"
        |            }, {
        |                "name": "moneytype",
        |                "value": "moneytype"
        |            }]
        |        },
        |        {
        |                "id": "2",
        |                "sql": "2  $Google",
        |                "taskType": "1",
        |                "targetTable": "TMP_RESULT",
        |                "partitionNum": "-1",
        |                "saveMode": "1",
        |                "inputParam": [{
        |                    "name": "Google",
        |                    "value": "http://www.google.com"
        |                }, {
        |                    "name": "Baidu",
        |                    "value": "http://www.baidu.com"
        |                }, {
        |                    "name": "SoSo",
        |                    "value": "http://www.SoSo.com"
        |                }]
        |            }
        |    ]
        |}
      """.stripMargin*/


    val s =
      """
        |{
        |    "taskInfo": [{
        |    "taskSn": 111,
        |    "id": 2,
        |    "sql": "select custid,moneytype,stktype,market,stkcode,  mktval,pre_mktval,  profitcost ,pre_profitcost,  addcost,  profit,  case when (pre_mktval+addcost) = 0 then 0 else cast(1.0*(profit/(pre_mktval+addcost))*100 as decimal(20,4)) end stk_profitrate  from tmp_result",
        |    "taskType": "0",
        |    "tableInfo": {
        |        "tableName": "ST_DD_CUST_STKPROFIT",
        |        "partitionNum": "2",
        |        "saveMode": "1",
        |        "hivePartitionInfo": [{
        |            "key": "-1",
        |            "value": "-1"
        |        }]
        |    },
        |    "inputParams": [{
        |        "name": "",
        |        "value": ""
        |    }]
        |},{
        |    "taskSn": 111,
        |    "id": 1,
        |    "sql": "select custid,moneytype,stktype,market,stkcode,cast(sum(mktval) as   decimal(20,4)) mktval,    cast(sum(pre_mktval) as decimal(20,4)) pre_mktval,cast(sum(profitcost) as decimal(20,4)) profitcost,  cast(sum(pre_profitcost) as decimal(20,4)) pre_profitcost, cast(sum(addcost) as decimal(20,4)) addcost, cast(sum(profit_day) as decimal(20,4)) profit  from st_dd_stock_profit  where dcdate = $dcdate  group by custid,moneytype,stktype,market,stkcode",
        |    "taskType": "1",
        |    "tableInfo": {
        |        "tableName": "TMP_RESULT",
        |        "partitionNum": "",
        |        "saveMode": "1",
        |        "hivePartitionInfo": [{
        |            "key": "1",
        |            "value": "-1"
        |        }]
        |    },
        |    "inputParams": [{
        |        "name": "dcdate",
        |        "value": "20180806"
        |    }, {
        |        "name": "Baidu",
        |        "value": "http://www.baidu.com"
        |    }, {
        |        "name": "moneytype",
        |        "value": "moneytype"
        |    }]
        |}
        |    ]
        |}
      """.stripMargin
    val tasks = new JsonParseUtils().json2Object(s)
    tasks.foreach(taskInfo=>{
      println(s"====sql:${taskInfo}")
      //DASBase.apply(taskInfo)
    })
  }
}
