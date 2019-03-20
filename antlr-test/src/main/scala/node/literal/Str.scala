package node.literal

import node.DataType
import node.DataType.DataType

/**
  * Created by Administrator on 2017/1/21 0021.
  */
case class Str(str: String) extends Literal {
  override val getCode: String = str
  override val getType: DataType = DataType.Text
}
