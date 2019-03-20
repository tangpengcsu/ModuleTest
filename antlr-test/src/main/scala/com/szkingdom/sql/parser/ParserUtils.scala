package com.szkingdom.sql.parser

import org.antlr.v4.runtime.{ParserRuleContext, Token}

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2018-08-25
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-08-25     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */
object ParserUtils {

  /** Get the origin (line and position) of the token. */
  def position(token: Token): Origin = {
    val opt = Option(token)
    Origin(opt.map(_.getLine), opt.map(_.getCharPositionInLine))
  }
  /**
    * Register the origin of the context. Any TreeNode created in the closure will be assigned the
    * registered origin. This method restores the previously set origin after completion of the
    * closure.
    */
  def withOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    val current = CurrentOrigin.get
    CurrentOrigin.set(position(ctx.getStart))
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }
}
