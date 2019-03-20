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
object WordCountExample
{
  def main(args: Array[String]): Unit =
  {
    val sparkSession = HiveContextInstance.create()
    args(0).toUpperCase() match
    {

      case "LOADDATA" =>
        import sparkSession.sql
        sql("truncate table wordcount")
        if (args(1) != null && args(1).toInt > 1)
        {
          val num = 0 to args(1).toInt
          num.toList.foreach(_ => loadData(sparkSession))
        } else
        {
          loadData(sparkSession)
        }
        sql("select count(*) from wordcount").show()
      case "EXEC" =>
        shuffleProcess(sparkSession)
      case "ALL" => loadData(sparkSession)
        shuffleProcess(sparkSession)
      case _ => println("========参数不合法")
    }


    HiveContextInstance.stop()

  }

  case class Word(num: Long, word: String)

  /**
    * 装载数据
    *
    * @param sparkSession
    */
  def loadData(sparkSession: SparkSession): Unit =
  {
    val tableName = "wordcount"
    import sparkSession.sql
    //建表
    sql("create table if not exists wordcount(num long,word string)")

    import sparkSession.implicits._
    //切分并装载数据
    val words = sparkSession.sparkContext.textFile(s"${HiveContextInstance.hdfsPath}wordcount.txt", 2).flatMap(_.split(" "))
    //val words = sparkSession.sparkContext.textFile("hive/src/main/resources/wordcount.txt", 2).flatMap(_.split(" "))
    words.map(item => Word(1, item.trim)).toDF().filter("word!=''").write.mode(SaveMode.Append)
      .saveAsTable(tableName)

  }

  /**
    * 随机数前缀-局部聚合-全局聚合
    *
    * @param sparkSession
    */
  def shuffleProcess(sparkSession: SparkSession): Unit =
  {
    //word+随机数前缀--并局部聚合操作
    val localSql =
    """
      | select word, count(*) as num from
      |          (select concat(cast(rand() * 10 as int), '_', word) as word,num  from  wordcount where word != '') s
      |           group by s.word
    """.stripMargin
    /*
        val localSql = "select word,count(*) as num from " +
                " (select concat(cast(rand() * 10 as int), '_', word) as word,num  from  wordcount where word != '') s  " +
                " group by s.word "
    */

    import sparkSession.sql
    sql(localSql).createOrReplaceTempView("local_wordcount")
    println("=========局部聚合")
    sql("select * from local_wordcount").show()
    /*
    * +----------+------+
      |      word|   num|
      +----------+------+
      |5_zhangsan|     1|
      |   3_world|120579|
      |   2_world|120895|
      |   7_hello|     1|
      |      2_hi|     1|
      |   4_world|120426|
      |      4_hi|     2|
      |   3_hello|     1|
      |   8_world|120525|
      |   5_world|120500|
      |   1_world|120578|
      |0_zhangsan|     1|
      |      1_hi|     2|
      |   0_world|120465|
      |   7_world|120138|
      |   9_world|120172|
      |   6_world|120357|
      |      7_hi|     1|
      +----------+------+
    * */
    //去掉随机数前缀并全局聚合
    println("=========全局聚合")

    sql("create table if not exists tmpwordcount(num long,word string)")

    sql("select sum(num) as num,word from (select split(word,'_')[1] as word,num from  local_wordcount ) group by " +
      "word").coalesce(4).write.mode(SaveMode.Overwrite).saveAsTable("tmpwordcount")
    /*
    +--------+--------+
    |    word|sum(num)|
    +--------+--------+
    |   hello|       2|
    |zhangsan|       2|
    |   world| 1204635|
    |      hi|       6|
    +--------+--------+
    * */


  }


}
