package base

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
object Enum
{

}

object SaveModeType extends Enumeration
{
  type SaveModeType = Value

}

object TaskType
{
  val SAVEASTABLE = "0"  //保存到Hive表
  val CREATEORREPLACETEMPVIEW = "1"//注册为临时视图
  val QUERY = "2"//直接执行 HQL 语句
  val CACHETABLE = "3" //创建为缓存表
}

object SaveModes
{
  val APPEND = "0" //在原表数据基础上插入数据
  val OVERWRITE = "1" //清空表数据，再插入数据
  val ERRORIFEXISTSM = "2"//如果数据存在，则报错
  val IGNORE = "3"//忽略错误
}
