package log

import org.apache.log4j.Logger

/** 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang
  * 创建日期：2018-04-10
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-04-10     1.0.1.0    tang      创建
  * ----------------------------------------------------------------
  */
object DASLog
{
  private val dasLog : Logger = Logger.getLogger("DASLog")
  def info(msg: String): Unit ={
    dasLog.info(msg)
  }
  def warn(msg: String): Unit =
  {
    dasLog.warn(msg)
  }

  def debug(msg: String): Unit =
  {
    dasLog.debug(msg)
  }

  def fatal(msg: String): Unit =
  {
    dasLog.fatal(msg)
  }

  def error(msg: String): Unit =
  {
    dasLog.error(msg)
  }
}
