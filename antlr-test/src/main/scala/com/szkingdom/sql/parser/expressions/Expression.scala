package com.szkingdom.sql.parser.expressions

import com.szkingdom.sql.parser.TreeNode

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2018-08-30
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-08-30     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */
abstract class Expression extends TreeNode[Expression]{
  def nullable: Boolean
}

abstract class BinaryExpression extends Expression {
  def left:Expression
  def right:Expression

  override final def children: Seq[Expression] = Seq(left, right)

  override def nullable: Boolean = left.nullable || right.nullable

}