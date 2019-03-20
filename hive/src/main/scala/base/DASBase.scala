package base

import java.util.Properties

import log.DASLog
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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

class DASBase(tableInfo: TableInfo, completeHql: String)
{
  val sparkSession: SparkSession = DASContext.getSparkSession()

  /**
    * 添加额外配置
    */
  private def setConf():Unit ={

    val props = new Properties()
    props.setProperty("","")
    sparkSession.sqlContext.setConf(props)
  }

  private def query(): DataFrame =
  {
    try
    {
      DASLog.debug(s"执行HQL语句-${completeHql}")
      sparkSession.sql(completeHql)
    } catch
    {
      case e: Exception =>
        DASLog.error(s"执行HQL报错：-${completeHql}")
        e.printStackTrace()
        throw e
    }
  }

  /**
    * 注册临时表
    */
  private def createOrReplaceTempView(): Unit =
  {
    try
    {
      DASLog.debug(s"注册临时表：表名-${tableInfo.tableName} -|- ${tableInfo.partitionNum}-|- HQL语句-${completeHql}")
      val orginalDF = sparkSession.sql(completeHql)
      if (isResizePartition)
      {
        orginalDF.coalesce(tableInfo.partitionNum.toInt).createOrReplaceTempView(tableInfo.tableName)
      } else
      {
        orginalDF.createOrReplaceTempView(tableInfo.tableName)
      }
    } catch
    {
      case e: Exception =>
        DASLog.error(s"注册临时表失败：表名-${tableInfo.tableName} -|- HQL语句-${completeHql}")
        e.printStackTrace()
        throw e
    }
  }

  /**
    * 保存到 hive 表
    */
  private def saveAsTable(): Unit =
  {
    try
    {
      val saveMode = Utils.transSaveMode(tableInfo.saveMode)

      val hivePartitionInfo = getHivePartitionInfo
      val partitionInsertFlag = hivePartitionInfo.nonEmpty //有无分区插入信息
      val resizePartitionFlag = isResizePartition // 有无调整分区信息

      if (partitionInsertFlag)
      {
        //Hive分区插入信息
        DASLog.debug(s"保存数据到Hive表并指定分区：表名-${tableInfo.tableName} -|- 调整RDD分片大小：${resizePartitionFlag},${
          tableInfo.partitionNum}  -|=  表分区信息：${hivePartitionInfo}  -|-  HQL语句-${completeHql}")
        savaAsTableAssignPartition(resizePartitionFlag, hivePartitionInfo)

      } else
      {
        if (resizePartitionFlag)
        {
          DASLog.debug(s"保存数据并调整RDD分片大小：表名-${tableInfo.tableName} -|- RDD分片调整：${tableInfo.partitionNum} -|- " +
            s"HQL语句-${completeHql}")
          sparkSession.sql(completeHql).coalesce(tableInfo.partitionNum.toInt).write.mode(saveMode).saveAsTable(tableInfo
            .tableName)
        } else
        {
          DASLog.debug(s"保存数据到Hive表：表名-${tableInfo.tableName} -|- HQL语句-${completeHql}")
          sparkSession.sql(completeHql).write.mode(saveMode).saveAsTable(tableInfo.tableName)
        }
      }
    } catch
    {
      case e: Exception =>
        DASLog.error(s"保存数据失败：表名-${tableInfo.tableName} -|- HQL语句-${completeHql}")
        e.printStackTrace()
        throw e
    }
  }

  /**
    * 保存到 hive 表指定分区
    *
    * @param resizePartitionFlag 是否调整rdd分片大小
    * @param hivePartitionInfo   瓶装的表分区数据
    */
  private def savaAsTableAssignPartition(resizePartitionFlag: Boolean, hivePartitionInfo: String):
  Unit =
  {
    val tmpTableName = s"${tableInfo.tableName}_tmp_000"
    if (resizePartitionFlag)
    {
      sparkSession.sql(completeHql).coalesce(tableInfo.partitionNum.toInt).createOrReplaceTempView(tmpTableName)
    } else
    {
      sparkSession.sql(completeHql).createOrReplaceTempView(tmpTableName)
    }
    sparkSession.sql(s"insert into ${tableInfo.tableName} partition(${hivePartitionInfo}) select * ${tmpTableName}")
  }

  /**
    * 先注册后缓存表
    */
  private def saveAsCacheTable(): Unit =
  {
    saveAsTable()
    DASLog.debug(s"缓存表：${tableInfo.tableName}")
    sparkSession.sqlContext.cacheTable(tableInfo.tableName)
  }

  /**
    * 仅缓存表
    */
  private def saveCacheTableOnly: Unit =
  {
    DASLog.debug(s"仅缓存表：${tableInfo.tableName}")
    sparkSession.sqlContext.cacheTable(tableInfo.tableName)
  }

  /**
    * 移除缓存表
    */
  private def unCacheTable(): Unit =
  {
    DASLog.debug(s"移除缓存表：${tableInfo.tableName}")
    sparkSession.sql(s"uncache table ${tableInfo.tableName}")
    //sparkSession.sqlContext.uncacheTable(taskInfo.targetTable)
  }

  /**
    * hive小文件合并
    */
  private def coalesceHiveTable(): Unit =
  {

    val tmpTableName = s"${tableInfo.tableName}_ccoalesce_000"
    val hivePartitionInfo = getHivePartitionInfo
    var tmpHql = s"select * from ${tableInfo.tableName}"
    var insertHql = s"insert into ${tableInfo.tableName}"
    if (hivePartitionInfo.nonEmpty)
    {
      tmpHql = s"${tmpHql} where " + hivePartitionInfo
      insertHql = s"${insertHql} partition( ${hivePartitionInfo} )"
    }
    insertHql = s"${insertHql} select * from ${tmpTableName}"


    val coalesceTableDF = sparkSession.sql(tmpHql).coalesce(tableInfo.partitionNum
      .toInt)
    coalesceTableDF.createOrReplaceTempView(tmpTableName)

    sparkSession.sql(insertHql)

  }

  /**
    * 有无调整分区信息
    *
    * @return
    */
  private def isResizePartition: Boolean = !(tableInfo.partitionNum.trim.isEmpty || tableInfo.partitionNum == "-1")

  /**
    * 获取Hive表分区信息
    *
    * @return
    */
  private def getHivePartitionInfo: String = Utils.resolveHivePartitionInfo(tableInfo.hivePartitionInfo)





}

object DASBase
{
  def apply(taskInfo: TaskInfo): DASBase =
  {
    val hql = Utils.resolveSql(taskInfo.sql, taskInfo.inputParams)
    val dasBase = new DASBase(taskInfo.tableInfo, hql)

    taskInfo.taskType match
    {
      case TaskType.SAVEASTABLE => dasBase.saveAsTable
      case TaskType.CREATEORREPLACETEMPVIEW => dasBase.createOrReplaceTempView
      case TaskType.QUERY => dasBase.query
      case TaskType.CACHETABLE => dasBase.saveAsCacheTable
      case _ => println(s"不支持此类型${taskInfo.taskType}")
    }
    dasBase
  }
}
