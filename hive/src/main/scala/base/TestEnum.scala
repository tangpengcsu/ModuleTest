package base

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
object TestEnum extends Enumeration
{
  type TestEnum = Value
  val red = Value("red")
  val red1 = Value("red1")
}
object Testtt{
  def main(args: Array[String]): Unit =
  {
    /*"red" match {
      case TestEnum.red =>println("red")
      case TestEnum.red1 => println("red1")
      case _ => println("-----")
    }*/
    val number: Double = 36.0
    number match {
      case Square(n) => println(s"square root of $number is $n")
      case _ => println("nothing matched")
    }
  }
}


object Square {
  def unapply(z: Double): Option[Double] = Some(math.sqrt(z))
}
