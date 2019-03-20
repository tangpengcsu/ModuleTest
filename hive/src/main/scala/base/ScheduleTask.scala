package base

import log.DASLog

/** 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang
  * 创建日期：2018-04-09
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-04-09     1.0.1.0    tang      创建
  * ----------------------------------------------------------------
  */
object ScheduleTask
{
  def doTask(fileName:String): Unit =
  {
    resolveParam(fileName).foreach(taskInfo =>
    {
      DASLog.debug(s"任务信息：${taskInfo}")
      if(isHiveTask().isDefinedAt(taskInfo.taskType)){
        DASBase.apply(taskInfo)
      }else{
        println("编码实现等")
      }
    })

    // DASContext.getSparkSession().sqlContext.clearCache()// Removes all cached tables from the in-memory cache

  }

  /**
    * 入参解析
    * @param fileName
    * @return
    */
  def resolveParam(fileName:String):Array[TaskInfo] = {

    // TODO: 依据实际情况更改
    // TODO: 参数校验
    //    val tasks = new JsonParseUtils().json2Object(fileName)
    val sparkSession = DASContext.getSparkSession()
    val path = s"${DASContext.hdfsPath}${fileName}"

    import sparkSession.implicits._
    // @DESCRIPTION: 按id升序排
    sparkSession.read.json(path).as[TaskInfo].sort($"id".asc).collect

  }

  /**
    * 判断是否是 Hive 任务
    * @return
    */
  def isHiveTask():PartialFunction[String,Unit]={
    case TaskType.SAVEASTABLE => DASLog.info("任务类型：保存数据到Hive表")
    case TaskType.QUERY => DASLog.info("任务类型：执行HQL语句")
    case TaskType.CREATEORREPLACETEMPVIEW => DASLog.info("任务类型：创建临时视图")
    case TaskType.CACHETABLE => DASLog.info("任务类型：创建缓存表")
  }
}
