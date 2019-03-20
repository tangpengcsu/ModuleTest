package node.function

import node.DataType
import node.DataType.DataType
import node.TreeNode
/**
  * Created by Administrator on 2017/1/21 0021.
  */
case class Sin(expr: TreeNode) extends Function {
  override val getCode: String = "sin(" + expr.getCode + ")"
  override val getType: DataType = DataType.NumCol

}
