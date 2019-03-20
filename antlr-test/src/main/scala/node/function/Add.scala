package node.function
import node.DataType
import node.DataType.DataType
import node.TreeNode
import node.literal.Num
/**
  * Created by Administrator on 2017/1/21 0021.
  */
case class Add(left: TreeNode, right: TreeNode) extends Function {
  override val getCode: String = {
    if (left.isInstanceOf[Num] && right.isInstanceOf[Num]) {
      (left.getCode.toDouble + right.getCode.toDouble).toString
    }
    else {
      left.getCode + "+" + right.getCode
    }
  }
  override val getType: DataType = {
    if (left.getType == DataType.Num && right.getType == DataType.Num) {
      DataType.Num
    }
    else {
      DataType.NumCol
    }
  }
}
