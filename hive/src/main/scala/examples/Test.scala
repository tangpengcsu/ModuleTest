package examples

import java.util.regex.Pattern

import examples.SparkSQLExample.Person
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StructType, _}

import scala.util.Random

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
object Test
{
  private val SPACE: Pattern = Pattern.compile(" ")

  def main(args: Array[String]): Unit =
  {
    System.setProperty("user.name", "hadoop")
    val spark = HiveContextInstance.create()
    //spark.sparkContext.par
    /*val lines = spark.sparkContext.textFile("/README.md")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)) .reduceByKey(_ + _).collect()*/

    import spark.sql

/*    val ss = sql("select * from src").queryExecution

    /*    println( ss.queryExecution.optimizedPlan.schemaString)
        println(ss.queryExecution.executedPlan.schemaString)
        println(ss.queryExecution.sparkPlan.schemaString)*/
    ss.printSchema()

 println(ss.toJSON.toString())*/
  //wordCount(spark)
    //spark.table("afafkds")
    spark.sql("select macro from  ods_tmp.dev_ods_kspb_digest").show()
    //test(spark)
    HiveContextInstance.stop()
  }
  def test1(sparkSession: SparkSession) :Unit ={
    val json =
      """
        |{"table":"OGG.ORDERS","op_type":"I","op_ts":"2018-06-01 08:30:03.626564","current_ts":"2018-06-01T16:30:06.076000","pos":"00000000000000006714","after":{"ORDER_DATE":"2017-12-06:17:27:04.493411000","TRD_DATE":20181207,"CUST_CODE":1116614305,"SECU_ACC_NAME":"1116614305","USER_CHAR":"1","CUST_CLS":"0","SPECIAL_CUST":"0","ACCOUNT":"1117714305","CURRENCY":"0","ACC_CLS":"1","BRANCH":280,"EXT_INST":0,"INITIATOR":"I","SECU_ACC":"0006614305","DCL_SECU_ACC":"0006614305","EXT_CLS":"0","TRD_ID":"WB","MARKET":"0","BOARD":"0","ORDER_ID":"BT100005","BIZ_NO":"BT100005","SECU_INTL":1,"SECU_CODE":"000001","SECU_NAME":"平安银行","SECU_CLS":"0","ORDER_PRICE":"12.1","ORDER_QTY":"1000","DCL_QTY":"1000","ORDER_AMT":12100.00,"ORDER_FRZ_AMT":12130.25,"AVAILABLE":89975739.50,"SHARE_AVL":"0","IS_WITHDRAW":"0","IS_WITHDRAWN":"0","CAN_WITHDRAW":"0","SEAT":"666660","DCL_TIMESLICE":"245     ","DCL_TIME":"2017-12-06:17:27:04.508735000","DCL_FLAG":"1","VALID_FLAG":"0","RET_MSG":"2067-Open queue manager kgdbkcxp error.(reason=2067)","INTERFACE_REC":0,"MATCHED_QTY":"0","WITHDRAWN_QTY":"1000","MATCHED_AMT":0,"RLT_FRZ_AMT":0,"RLT_FRZ_QTY":"0","RLT_SETT_AMT":12130.25,"RLT_SETT_QTY":"0","OP_USER":30280,"OP_NAME":"业务测试员","OP_ROLE":"2","OP_BRH":280,"OP_SITE":"0024e89a8053","CHANNEL":"2","REMARK":",,","EXT_REC_NUM":0,"EXT_ORDER_ID":null,"EXT_BIZ_NO":null,"EXT_FRZ_AMT":0,"EXT_SETT_AMT":0,"EXT_ACC":null,"EXT_SUB_ACC":"11.0000","EXT_APP_SN":0}}
        |{"table":"OGG.ORDERS","op_type":"U","op_ts":"2018-06-01 09:24:16.613461","current_ts":"2018-06-01T17:24:20.005000","pos":"00000000000000010735","before":{"ORDER_DATE":"2017-12-06:17:27:20.419558000","TRD_DATE":20171207,"CUST_CODE":1116614353,"SECU_ACC_NAME":"1116614353","USER_CHAR":"2","CUST_CLS":"0","SPECIAL_CUST":"0","ACCOUNT":"1117714353","CURRENCY":"0","ACC_CLS":"1","BRANCH":280,"EXT_INST":0,"INITIATOR":"I","SECU_ACC":"0006614353","DCL_SECU_ACC":"0006614353","EXT_CLS":"0","TRD_ID":"HB","MARKET":"0","BOARD":"0","ORDER_ID":"BT100051","BIZ_NO":"BT100051","SECU_INTL":118005,"SECU_CODE":"118005","SECU_NAME":"12浔旅债","SECU_CLS":"S","ORDER_PRICE":"107.583","ORDER_QTY":"500000","DCL_QTY":"500000","ORDER_AMT":53791500.00,"ORDER_FRZ_AMT":53925978.75,"AVAILABLE":82148042.50,"SHARE_AVL":"0","IS_WITHDRAW":"0","IS_WITHDRAWN":"0","CAN_WITHDRAW":"0","SEAT":"666660","DCL_TIMESLICE":"24567   ","DCL_TIME":"2017-12-06:17:27:20.442927000","DCL_FLAG":"1","VALID_FLAG":"0","RET_MSG":"2067-Open queue manager kgdbkcxp error.(reason=2067)","INTERFACE_REC":0,"MATCHED_QTY":"0","WITHDRAWN_QTY":"500000","MATCHED_AMT":0,"RLT_FRZ_AMT":0,"RLT_FRZ_QTY":"0","RLT_SETT_AMT":53925978.75,"RLT_SETT_QTY":"0","OP_USER":30280,"OP_NAME":"业务测试员","OP_ROLE":"2","OP_BRH":280,"OP_SITE":"0024e89a8053","CHANNEL":"2","REMARK":",,","EXT_REC_NUM":0,"EXT_ORDER_ID":null,"EXT_BIZ_NO":null,"EXT_FRZ_AMT":0,"EXT_SETT_AMT":0,"EXT_ACC":"XXL","EXT_SUB_ACC":"1","EXT_APP_SN":0},"after":{"TRD_DATE":20171207,"USER_CHAR":"3","MARKET":"0","BOARD":"0","ORDER_ID":"BT100051","IS_WITHDRAW":"0"}}
      """.stripMargin
    import org.apache.spark.sql.functions._
    sparkSession.read.json(json).show()
  }
  def testast(sparkSession: SparkSession):Unit={
    import sparkSession.sql
   val tree =  sql("select 'test1' as topicName, r.name,r.add from testkafka1  r inner join testkafka k on r.name = k.name")
     .queryExecution.logical
    //tree.treeString
    println("fsaffff===="+tree.treeString)
  }

  def test(sparkSession: SparkSession):Unit = {
    val dd =
      """
        |SELECT T.SSYS_DICT_CD   ,
        |       T.SSYS_DICTEN_CD ,--源系统字典项代码
        |       T.SYS_SRC        ,--系统来源
        |       T.MDL_DICT_CD    ,--模型字典代码
        |       T.MDL_DICTEN_CD   --模型字典项代码
        |  FROM DW.PUB_CODE_DICT T
      """.stripMargin
    import sparkSession.sql
    sql(dd).createOrReplaceTempView("TEMP_DICT_CODE ")
    //sql("select * from src").show()
    //sparkSession.sqlContext.cacheTable("TEMP_DICT_CODE")

    val orginalTdate =
      """
        SELECT CAST(T1.YMT_CODE AS STRING)   AS UNIF_ID    ,
        |                   T3.MDL_DICTEN_CD              AS MKT_CD     ,
        |                   CAST(T1.CUST_CODE AS STRING)  AS CUST_ID    ,
        |                   CAST(T1.INT_ORG AS STRING)    AS DEPT_ID    ,
        |                   CAST(T1.YMT_STATUS AS STRING) AS ACC_STAT_CD
        |          FROM ODS_KUAS.STK_YMT T1
        | LEFT JOIN ODS_KUAS.STK_TRDACCT T2 ON T1.YMT_CODE = T2.YMT_CODE AND T1.PTN_DATE = T2.PTN_DATE
        | LEFT JOIN TEMP_DICT_CODE T3 ON T2.STKBD = T3.SSYS_DICTEN_CD AND T3.SYS_SRC ='KUAS' AND T3.MDL_DICT_CD
        | ='DIMLS111'
        |         WHERE T1.PTN_DATE = 20180606 AND T1.YMT_CODE IS NOT NULL
        |
      """.stripMargin

    sql(orginalTdate).createOrReplaceTempView("ACC_CPTL_CRRC_ACC_t_date")

   // sql("select * from ACC_CPTL_ACC_t_date").show()
    // t日数据
    val tDate =
      """
        |select md5(concat(case when CPTL_ACC_ID is null then '^' else CPTL_ACC_ID end, ',' ,case when CPTL_ACC_CLAS_CD is null then '^' else CPTL_ACC_CLAS_CD end, ',' ,case when CPTL_ACC_USE_CD is null then '^' else CPTL_ACC_USE_CD end, ',' ,case when CRRC_CD is null then '^' else CRRC_CD end, ',' ,case when CUST_ID is null then '^' else CUST_ID end, ',' ,case when INTR_TYPE_CD is null then '^' else INTR_TYPE_CD end, ',' ,case when INTEREST is null then '^' else INTEREST end, ',' ,case when INTR_ARRG is null then '^' else INTR_ARRG end, ',' ,case when PNLT is null then '^' else PNLT end, ',' ,case when PNLT_ARRG is null then '^' else PNLT_ARRG end, ',' ,case when INTR_TAX is null then '^' else INTR_TAX end, ',' ,case when LAST_INTL_DATE is null then '^' else LAST_INTL_DATE end, ',' ,case when LAST_INTA_DATE is null then '^' else LAST_INTA_DATE end)) as md_key ,CPTL_ACC_ID,CPTL_ACC_CLAS_CD,CPTL_ACC_USE_CD,CRRC_CD,CUST_ID,INTR_TYPE_CD,INTEREST,INTR_ARRG,PNLT,PNLT_ARRG,INTR_TAX,LAST_INTL_DATE,LAST_INTA_DATE from ACC_CPTL_CRRC_ACC_t_date
        |
      """.stripMargin
    //sql(tDate).createOrReplaceTempView("ACC_BNK_ACC_t_date_tmp_table")
     //sql(tDate).show

    //sql(tDate).write.mode(SaveMode.Overwrite).saveAsTable("ACC_CPTL_CRRC_ACC_t_date_tmp_table")
    //sql("select * from ACC_BNK_ACC_t_date_tmp_table where   bnk_acc_id = '111111111111111111'").show(100000)
      //增量数据

    val zl =
      """
        |select d.CRRC_CD,d.CUST_ID,d.INTR_TYPE_CD,d.INTEREST,d.INTR_ARRG,d.PNLT,d.PNLT_ARRG,d.INTR_TAX,d.LAST_INTL_DATE,d.LAST_INTA_DATE from
        |    ACC_CPTL_CRRC_ACC_t_date_tmp_table d
        |left outer join
        |     (select md5(concat(case when CRRC_CD is null then '^' else CRRC_CD end, ',' ,case when CUST_ID is null then '^' else CUST_ID end, ',' ,case when INTR_TYPE_CD is null then '^' else INTR_TYPE_CD end, ',' ,case when INTEREST is null then '^' else INTEREST end, ',' ,case when INTR_ARRG is null then '^' else INTR_ARRG end, ',' ,case when PNLT is null then '^' else PNLT end, ',' ,case when PNLT_ARRG is null then '^' else PNLT_ARRG end, ',' ,case when INTR_TAX is null then '^' else INTR_TAX end, ',' ,case when LAST_INTL_DATE is null then '^' else LAST_INTL_DATE end, ',' ,case when LAST_INTA_DATE is null then '^' else LAST_INTA_DATE end)) as md_key, CRRC_CD from DW.ACC_CPTL_CRRC_ACC where  end_date =
        |     '2999-12-31')  h
        |on d.md_key = h.md_key where h.CRRC_CD is null
        |""".stripMargin

  //sql(zl).createOrReplaceTempView("ACC_BNK_ACC_t_date_inc_view_1")
 //sql(zl).show()
    //sql(zl).write.mode(SaveMode.Overwrite).saveAsTable   ("ACC_CPTL_CRRC_ACC_t_date_inc_view")
  // sql("select * from ACC_BNK_ACC_t_date_inc_view_1 where   bnk_acc_id = '111111111111111111'").show(1000000);
    //最终数据
    val ll =
      """
        |select
        |	CRRC_CD,CUST_ID,INTR_TYPE_CD,INTEREST,INTR_ARRG,PNLT,PNLT_ARRG,INTR_TAX,LAST_INTL_DATE,LAST_INTA_DATE,
        |	cast('2018-06-06' as date) as start_date,
        |	cast('2999-12-31' as date) as end_date
        |from ACC_CPTL_CRRC_ACC_t_date_inc_view
      """.stripMargin
    /*
    select h.CRRC_CD as CRRC_CD,h.CUST_ID as CUST_ID,h.INTR_TYPE_CD as INTR_TYPE_CD,h.INTEREST as INTEREST,h.INTR_ARRG as INTR_ARRG,h.PNLT as PNLT,h.PNLT_ARRG as PNLT_ARRG,h.INTR_TAX as INTR_TAX,h.LAST_INTL_DATE as LAST_INTL_DATE,h.LAST_INTA_DATE as LAST_INTA_DATE,
		h.start_date as start_date,
		case when
          u.CRRC_CD is null
			 then cast('2018-06-06' as date)
			 else h.end_date end as end_date
		from (select md5(concat(case when CRRC_CD is null then '^' else CRRC_CD end, ',' ,case when CUST_ID is null then '^' else CUST_ID end, ',' ,case when INTR_TYPE_CD is null then '^' else INTR_TYPE_CD end, ',' ,case when INTEREST is null then '^' else INTEREST end, ',' ,case when INTR_ARRG is null then '^' else INTR_ARRG end, ',' ,case when PNLT is null then '^' else PNLT end, ',' ,case when PNLT_ARRG is null then '^' else PNLT_ARRG end, ',' ,case when INTR_TAX is null then '^' else INTR_TAX end, ',' ,case when LAST_INTL_DATE is null then '^' else LAST_INTL_DATE end, ',' ,case when LAST_INTA_DATE is null then '^' else LAST_INTA_DATE end)) as md_key,CRRC_CD,CUST_ID,INTR_TYPE_CD,INTEREST,INTR_ARRG,PNLT,PNLT_ARRG,INTR_TAX,LAST_INTL_DATE,LAST_INTA_DATE, start_date, end_date from  DW.ACC_CPTL_CRRC_ACC where  end_date =  '2999-12-31') as h
       left join ACC_CPTL_CRRC_ACC_t_date_tmp_table u on h.md_key = u.md_key
    union
select
	CRRC_CD,CUST_ID,INTR_TYPE_CD,INTEREST,INTR_ARRG,PNLT,PNLT_ARRG,INTR_TAX,LAST_INTL_DATE,LAST_INTA_DATE,
	cast('2018-06-06' as date) as start_date,
	cast('2999-12-31' as date) as end_date
from ACC_CPTL_CRRC_ACC_t_date_inc_view
*/
    //sql(ll).show()
 //sql(ll)    .write.mode(SaveMode.Overwrite).saveAsTable("ACC_BNK_ACC_TMP2")
  //  println("ACC_BNK_ACC_TMP1====")
  //  sparkSession.table("ACC_BNK_ACC_TMP1").printSchema()
    //println("ACC_BNK_ACC_TMP2====")
    //sparkSession.table("ACC_BNK_ACC_TMP2").printSchema()
  }
  def wordCount(sparkSession: SparkSession): Unit =
  {
    val lines = sparkSession.sparkContext.textFile("hive/src/main/resources/wordcount.txt")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_,1)).reduceByKey(_+_).collect()
    wordCounts.foreach(println(_))
  }
  case class Word(num:Long,word:String)


  def  insertToHive(sparkSession: SparkSession):Unit={

    import sparkSession.sql
    sql("create table if not exists wordcount(num long,word string)")

    import sparkSession.implicits._
/*    val lines = sparkSession.sparkContext.textFile("hive/src/main/resources/wordcount.txt").flatMap(_.split(" "))
    val words =  lines.map((1,_))
    val wordcount = words.map(item=>Word(item._1,item._2.trim)).toDF()

    wordcount.write.mode(SaveMode.Overwrite).saveAsTable("wordcount")*/
  //  val randWord = sql("select * from wordcount").as[Word].map(item=>(Random.nextInt(10)+"_"+item.word,item.num))

    //在word加前缀+随即数--并局部聚合操作
val  localSql ="select word,count(*) as num from (select concat(cast(rand() * 10 as int), '_', word) as word,num " +
    "from  wordcount where word != '') s  group by s.word "
/*    sql(aggSql).map(item=>(item.getString(0).split("_")(1),item.getInt(1))).groupBy("word").sum("count")
      .select("word","count").show()*/

    sql(localSql).createOrReplaceTempView("local_wordcount")
    sql("select word,sum(num) from (select split(word,'_')[1] as word,num from  local_wordcount ) group by word").show()
/*

    sql(localSql).map(item=>Word(item.getAs[Long]("num"),item.getAs[String]("word").split("_")(1))).groupBy("word")
      .sum("num").show()
*/


    //println(sql("select * from wordcount").as[Word].count())
  }
  /**
    * 雇员--10 create table employee(employeeid number ,name string,addr string,custid id)
    * 客户---1w  create table customer(custid number,name string ,addr string)
    */
  def shuffTest(spark: SparkSession):Unit={
    val employeeSchemaString = "employeeid name addr custid"
    //雇员
    val employeeFields = employeeSchemaString.split(" ").map(fieldName=>{
      if(fieldName=="employeeid" || fieldName == "custid")
        StructField(fieldName, IntegerType, nullable = true)
      else
        StructField(fieldName, StringType, nullable = true)
    })
    val employeeSchema = StructType(employeeFields)
    //客户
    val customerSchemaString = "custid name addr"
    val customerFields = customerSchemaString.split(" ").map(fieldName=>{
      if( fieldName == "custid")
        StructField(fieldName, IntegerType, nullable = true)
      else
        StructField(fieldName, StringType, nullable = true)
    })
    val customerSchema = StructType(customerFields)
    val s =spark.sparkContext.parallelize(1 to 10 toList).map(item=>Row(item,s"name_i$item",s"address_$item"))
    s.map(item=>item.eq(""))
    val employeeMem = 1 to 10 toList
    import spark.sql


  }
  case class Employee(id:Int,name:String,address:String,custId:Int)
  case class Consumer(id:Int,name:String,address:String)

}
