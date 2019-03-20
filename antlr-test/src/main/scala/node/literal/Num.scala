package node.literal

import node.DataType
import node.DataType.DataType

/**
  * Created by Administrator on 2017/1/21 0021.
  */
case class Num(num: Double) extends Literal {
  override val getCode: String = num.toString
  override val getType: DataType = DataType.Num
}
