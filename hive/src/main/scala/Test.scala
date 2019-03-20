import org.apache.calcite.sql.parser.SqlParser.Config
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/** 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tangpeng
  * 创建日期：2018-02-27
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-02-27     1.0.1.0    tangpeng      创建
  * ----------------------------------------------------------------
  */
class Test
{

}

/**
  * spark sql 读取 hive 数据 spark on hive
  */
object ReadFromHive
{
  case class Record(key: Int, value: String)
  def main(args: Array[String]): Unit =
  {
    println("====开始====")
   // val warehouseLocation = "D:\\workspace\\IdeaProjects\\warehouse"
    val warehouseLocation ="hdfs://192.168.50.88:9000/user/hive/warehouse"
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

/*    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
  //  sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM src").show()
    // +--------+
    // |count(1)|
    // +--------+
    // |    500 |
    // +--------+

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    //数据保存到表
    sql("CREATE TABLE IF NOT EXISTS srcbak (key INT, value STRING)")

    sqlDF.write.mode(SaveMode.Overwrite).saveAsTable("srcbak")

    // The items in DaraFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()*/
    // +--------------------+
    // |               value|
    // +--------------------+
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // ...
/*    val s = sql("select * from src").schema.fieldNames
s.foreach(item=>println(item))*/
    spark.sparkContext.parallelize( 1 to 10).saveAsTextFile("hdfs://192.168.50.88:9000/tmp/test.csv")

    // You can also use DataFrames to create temporary views within a SparkSession.
/*val ssss = sql(
  """
    |select t1.entr_id,
    |t1.cust_id,
    |t1.entr_date,
    |t1.jour_id,
    |t1.entr_time,
    |t1.occu_date,
    |t1.occu_time,
    |t1.inr_org_id,
    |t1.cptl_acc_id,
    |T2.MDL_DICTEN_CD as crrc_cd,
    |t1.fnd_acc_id,
    |t1.fnd_trd_acc_id,
    |t1.fnd_cd,
    |t1.entr_qty,
    |t1.entr_amt,
    |t1.huge_pr_deal_mode_cd,
    |t1.expr_sort_cd,
    |t1.term_quot_strt_date,
    |t1.term_quot_end_date,
    |t1.matu_tims,
    |t1.tot_ivsm_amt,
    |t1.sale_cms_disc_rate,
    |t1.ivsm_desc,
    |t1.dect_pd,
    |t1.pd_dect_date,
    |T3.MDL_DICTEN_CD as chag_mode_cd,
    |T4.MDL_DICTEN_CD as chag_type_cd,
    |t1.trd_app_eff_days,
    |T5.MDL_DICTEN_CD as busi_clas_cd,
    |T6.MDL_DICTEN_CD as chn_clas_cd,
    |T7.MDL_DICTEN_CD as entr_stat_cd,
    |t1.term_info  from
    |(select APP_SN          as entr_id,
    |CUST_CODE       as cust_id,
    |TRD_DATE        as entr_date,
    |ORDER_SN        as jour_id,
    |''              as entr_time,
    |OCCUR_DATE      as occu_date,
    |''              as occu_time,
    |BRANCH          as inr_org_id,
    |ACCOUNT         as cptl_acc_id,
    |CURRENCY        as crrc_cd,
    |FUND_ACC        as fnd_acc_id,
    |FUND_TRD_ACC    as fnd_trd_acc_id,
    |FUND_CODE       as fnd_cd,
    |ORDER_VOL       as entr_qty,
    |ORDER_AMT       as entr_amt,
    |''              as huge_pr_deal_mode_cd,
    |''              as expr_sort_cd,
    |''              as term_quot_strt_date,
    |''              as term_quot_end_date,
    |''              as matu_tims,
    |''              as tot_ivsm_amt,
    |DISCOUNT_RATIO  as sale_cms_disc_rate,
    |''              as ivsm_desc,
    |''              as dect_pd,
    |''              as pd_dect_date,
    |FEE_TYPE        as chag_mode_cd,
    |CHARGE_TYPE     as chag_type_cd,
    |''              as trd_app_eff_days,
    |TRD_ID          as busi_clas_cd,
    |CHANNEL         as chn_clas_cd,
    |ORDER_STATUS    as entr_stat_cd,
    |''              as term_info
    | from ods.KGOB_FUND_ORDERS WHERE TRD_ID='28') t1
    | LEFT JOIN DW.CODE_DICT_CODE T2 ON t1.crrc_cd = T2.SSYS_DICTEN_CD AND T2.SYS_SRC ='KGOB' AND T2.MDL_DICT_CD ='DIMLS009'
    | LEFT JOIN DW.CODE_DICT_CODE T3 ON t1.chag_mode_cd = T3.SSYS_DICTEN_CD AND T3.SYS_SRC ='KGOB' AND T3.MDL_DICT_CD ='DIMLS187'
    | LEFT JOIN DW.CODE_DICT_CODE T4 ON t1.chag_type_cd = T4.SSYS_DICTEN_CD AND T4.SYS_SRC ='KGOB' AND T4.MDL_DICT_CD ='DIMLS188'
    | LEFT JOIN DW.CODE_DICT_CODE T5 ON t1.busi_clas_cd = T5.SSYS_DICTEN_CD AND T5.SYS_SRC ='KGOB' AND T5.MDL_DICT_CD ='DIMLS228'
    | LEFT JOIN DW.CODE_DICT_CODE T6 ON t1.chn_clas_cd = T6.SSYS_DICTEN_CD AND T6.SYS_SRC ='KGOB' AND T6.MDL_DICT_CD ='DIMLS167'
    | LEFT JOIN DW.CODE_DICT_CODE T7 ON t1.entr_stat_cd = T7.SSYS_DICTEN_CD AND T7.SYS_SRC ='KGOB' AND T7.MDL_DICT_CD ='DIMLS210'
  """.stripMargin).queryExecution
    println("ssss-----"+ssss)*/
    /*val dfss = sql(
      """
        |select t1.entr_id,
        |t1.cust_id,
        |t1.entr_date,
        |t1.jour_id,
        |t1.entr_time,
        |t1.occu_date,
        |t1.occu_time,
        |t1.inr_org_id,
        |t1.cptl_acc_id,
        |T2.MDL_DICTEN_CD as crrc_cd,
        |t1.fnd_acc_id,
        |t1.fnd_trd_acc_id,
        |t1.fnd_cd,
        |t1.entr_qty,
        |t1.entr_amt,
        |t1.huge_pr_deal_mode_cd,
        |t1.expr_sort_cd,
        |t1.term_quot_strt_date,
        |t1.term_quot_end_date,
        |t1.matu_tims,
        |t1.tot_ivsm_amt,
        |t1.sale_cms_disc_rate,
        |t1.ivsm_desc,
        |t1.dect_pd,
        |t1.pd_dect_date,
        |T3.MDL_DICTEN_CD as chag_mode_cd,
        |T4.MDL_DICTEN_CD as chag_type_cd,
        |t1.trd_app_eff_days,
        |T5.MDL_DICTEN_CD as busi_clas_cd,
        |T6.MDL_DICTEN_CD as chn_clas_cd,
        |T7.MDL_DICTEN_CD as entr_stat_cd,
        |t1.term_info  from
        |(select APP_SN          as entr_id,
        |CUST_CODE       as cust_id,
        |TRD_DATE        as entr_date,
        |ORDER_SN        as jour_id,
        |''              as entr_time,
        |OCCUR_DATE      as occu_date,
        |''              as occu_time,
        |BRANCH          as inr_org_id,
        |ACCOUNT         as cptl_acc_id,
        |CURRENCY        as crrc_cd,
        |FUND_ACC        as fnd_acc_id,
        |FUND_TRD_ACC    as fnd_trd_acc_id,
        |FUND_CODE       as fnd_cd,
        |ORDER_VOL       as entr_qty,
        |ORDER_AMT       as entr_amt,
        |''              as huge_pr_deal_mode_cd,
        |''              as expr_sort_cd,
        |''              as term_quot_strt_date,
        |''              as term_quot_end_date,
        |''              as matu_tims,
        |''              as tot_ivsm_amt,
        |DISCOUNT_RATIO  as sale_cms_disc_rate,
        |''              as ivsm_desc,
        |''              as dect_pd,
        |''              as pd_dect_date,
        |FEE_TYPE        as chag_mode_cd,
        |CHARGE_TYPE     as chag_type_cd,
        |''              as trd_app_eff_days,
        |TRD_ID          as busi_clas_cd,
        |CHANNEL         as chn_clas_cd,
        |ORDER_STATUS    as entr_stat_cd,
        |''              as term_info
        | from ods.KGOB_FUND_ORDERS WHERE TRD_ID='28') t1
        | LEFT JOIN (select SSYS_DICTEN_CD,MDL_DICTEN_CD from DW.CODE_DICT_CODE where  SYS_SRC ='KGOB' AND MDL_DICT_CD ='DIMLS009') T2 ON t1.crrc_cd = T2.SSYS_DICTEN_CD
        | LEFT JOIN (select SSYS_DICTEN_CD,MDL_DICTEN_CD from DW.CODE_DICT_CODE where SYS_SRC ='KGOB' AND MDL_DICT_CD ='DIMLS187') T3 ON t1.chag_mode_cd = T3.SSYS_DICTEN_CD
        | LEFT JOIN (select SSYS_DICTEN_CD,MDL_DICTEN_CD from DW.CODE_DICT_CODE where SYS_SRC ='KGOB' AND MDL_DICT_CD ='DIMLS188' ) T4 ON t1.chag_type_cd = T4.SSYS_DICTEN_CD
        | LEFT JOIN (select SSYS_DICTEN_CD,MDL_DICTEN_CD from DW.CODE_DICT_CODE where SYS_SRC ='KGOB' AND MDL_DICT_CD ='DIMLS228') T5 ON t1.busi_clas_cd = T5.SSYS_DICTEN_CD
        | LEFT JOIN (select SSYS_DICTEN_CD,MDL_DICTEN_CD from DW.CODE_DICT_CODE  where SYS_SRC ='KGOB' AND MDL_DICT_CD ='DIMLS167') T6 ON t1.chn_clas_cd = T6.SSYS_DICTEN_CD
        | LEFT JOIN (select SSYS_DICTEN_CD,MDL_DICTEN_CD from DW.CODE_DICT_CODE where SYS_SRC ='KGOB' AND MDL_DICT_CD ='DIMLS210') T7 ON t1.entr_stat_cd = T7.SSYS_DICTEN_CD
      """.stripMargin)
    println("2222----"+dfss.queryExecution)*/
/*    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")
    // Queries can then join DataFrame data with data stored in Hive.
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").select("s.key","s.value").limit(10).show()*/
    // +---+------+---+------+
    // |key| value|key| value|
    // +---+------+---+------+
    // |  2| val_2|  2| val_2|
    // |  4| val_4|  4| val_4|
    // |  5| val_5|  5| val_5|
    // ...
    // $example off:spark_hive$
    spark.stop()
  }
}

/**
  * spark SQL 读取 json 数据
  */
object SparkSQL
{
  def main(args: Array[String]): Unit =
  {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // $example on:create_df$
    //spark.sql("select * from src").show()
    val df = spark.read.json("src/main/resources/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()


    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:create_df$

    // $example on:untyped_ops$
    // This import is needed to use the $-notation
    import spark.implicits._
    // Print the schema in a tree format
    df.printSchema()
    df.createOrReplaceTempView("people")
    df.sqlContext.sql("select * from people").show()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // Select only the "name" column
    df.select("name").show()
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     null|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+

    // Select people older than 21
    //df.filter($"age" > 21).show()
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+

    // Count people by age
    df.groupBy("age").count().show()
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+
    // $example off:untyped_ops$

    // $example on:run_sql$
    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:run_sql$

    // $example on:global_temp_view$
    // Register the DataFrame as a global temporary view

    //df.createGlobalTempView("people")


    // Global temporary view is tied to a system preserved database `global_temp`
    //spark.sql("SELECT * FROM global_temp.people").show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

    // Global temporary view is cross-session
   // spark.newSession().sql("SELECT * FROM global_temp.people").show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:global_temp_view$
    spark.stop()
  }
}