package test

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
object UtilTest
{
  def main(args: Array[String]): Unit =
   {

     val numbers = Array(  new Test11("1","11"),   new Test11("2","21"),   new Test11("3","31"),   new Test11("4","41"))
    //numbers.fold("")(_)


    println(pd.isDefinedAt(1))

  }
  def pd :PartialFunction[Int,String] ={
    case 1 => "one"
    case 2 => "two"
    case 3 => "three"
    case _ => "other"
  }

}
case class Test11(key:String,value:String)
