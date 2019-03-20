package base

/** 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang
  * 创建日期：2018-03-30
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-03-30     1.0.1.0    tang      创建
  * ----------------------------------------------------------------
  */
case class Tasks(taskInfo: Array[TaskInfo])

/*case class TaskInfo(taskSn: Long, id: Long, sql: String, taskType: String, targetTable: String, partitionNum:
String, saveMode: String, inputParams: Array[InputParam], hivePartitions:Array[HivePartition])*/
case class TaskInfo(taskSn: Long, id: Long, sql: String, taskType: String, tableInfo: TableInfo,  inputParams:
Array[InputParam])
{

  override def toString: String =  s"HQL:$sql || 任务类型:$taskType || " +
      s"入参:${Utils.resolveInputParam(inputParams)} || 目标表信息：$tableInfo"
}
case class TableInfo(tableName:String,saveMode:String,partitionNum:String,hivePartitionInfo: Array[HivePartition]){
  override def toString: String = s"目标表名：$tableName || 保存方式：$saveMode || 调整分区大小：$partitionNum ||插入分区表信息:${Utils
    .resolveHivePartitionInfo(hivePartitionInfo)}"
}

case class HivePartition(key: String, value: String)
{
  override def toString: String = s"${key} = ${value}"
}

case class InputParam(name: String, value: String)
{
  override def toString: String = s"$name = $value"
}

