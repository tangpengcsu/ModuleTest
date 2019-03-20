package base

import log.DASLog
import org.apache.spark.sql.SaveMode

/** 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang
  * 创建日期：2018-04-11
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-04-11     1.0.1.0    tang      创建
  * ----------------------------------------------------------------
  */
object Utils
{

  /**
    * 解析 SQL 入参
    */
  def resolveInputParam(inputParams: Array[InputParam]): String =
  {
    val param = new StringBuilder
    if (null != inputParams && inputParams.nonEmpty)
    {
      val in = inputParams.foreach(item =>
      {
        param.append(s"${item.name}=${item.value}")
        param.append(",")
      })
      param.deleteCharAt(param.length - 1)
    }
    param.toString()
  }


  /**
    * 解析hive表分区信息
    * @param hivePartition
    * @return
    */
  def resolveHivePartitionInfo(hivePartition: Array[HivePartition]): String =
  {
    val sb = new StringBuilder

    if (null != hivePartition && hivePartition.nonEmpty && !(hivePartition.length == 1 &&
      (hivePartition(0).key.trim.isEmpty || hivePartition(0).key == "-1")))
    {
      hivePartition.foreach(item =>
      {
        sb.append(s"${item.key} = '${item.value}' ,")
      })
      sb.deleteCharAt(sb.length - 1)
    }
    sb.toString()
  }

  /**
    * SQL 中占位符替换：$开头的
    *
    * @param hql
    * @param inputParams
    * @return
    */
  def resolveSql(hql: String, inputParams: Array[InputParam]): String =
  {
    var newHql = hql
    DASLog.debug("原HQL语句：" + newHql)

    if (inputParams != null && inputParams.nonEmpty)
    {
      inputParams.foreach(item =>
      {
        DASLog.debug(s"变量替换：${item.name}=${item.value}")
        newHql = newHql.replaceAll("\\$" + item.name, item.value)
      })
    }
    DASLog.debug("参数替换后HQL语句：" + newHql)
    newHql
  }

  /**
    * 转换保存方式
    *
    * @param saveMode
    * @return
    */
  def transSaveMode(saveMode: String): SaveMode =
  {

    saveMode match
    {
      case SaveModes.APPEND => return SaveMode.Append
      case SaveModes.OVERWRITE => return SaveMode.Overwrite
      case SaveModes.ERRORIFEXISTSM => return SaveMode.ErrorIfExists
      case SaveModes.IGNORE => return SaveMode.Ignore
      case _ =>
        DASLog.error(s"不支持该数据保存模式:${saveMode}！")
        throw new IllegalArgumentException
    }
  }
}
