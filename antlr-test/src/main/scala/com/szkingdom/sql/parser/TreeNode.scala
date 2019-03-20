package com.szkingdom.sql.parser

import scala.reflect.ClassTag

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
case class Origin(
                   line: Option[Int] = None,
                   startPosition: Option[Int] = None)
/**
  * Provides a location for TreeNodes to ask about the context of their origin.  For example, which
  * line of code is currently being parsed.
  */
object CurrentOrigin {
  private val value = new ThreadLocal[Origin]() {
    override def initialValue: Origin = Origin()
  }

  def get: Origin = value.get()
  def set(o: Origin): Unit = value.set(o)

  def reset(): Unit = value.set(Origin())

  def setPosition(line: Int, start: Int): Unit = {
    value.set(
      value.get.copy(line = Some(line), startPosition = Some(start)))
  }

  def withOrigin[A](o: Origin)(f: => A): A = {
    set(o)
    val ret = try f finally { reset() }
    ret
  }
}

// scalastyle:off
abstract class TreeNode[BaseType <: TreeNode[BaseType]] extends Product {
  // scalastyle:on
  self: BaseType =>

  val origin: Origin = CurrentOrigin.get

  /**
    * Returns a Seq of the children of this node.
    * Children should not change. Immutability required for containsChild optimization
    */
  def children: Seq[BaseType]

  lazy val containsChild: Set[TreeNode[_]] = children.toSet

  private lazy val _hashCode: Int = scala.util.hashing.MurmurHash3.productHash(this)
  override def hashCode(): Int = _hashCode

  /**
    * Faster version of equality which short-circuits when two treeNodes are the same instance.
    * We don't just override Object.equals, as doing so prevents the scala compiler from
    * generating case class `equals` methods
    */
  def fastEquals(other: TreeNode[_]): Boolean = {
    this.eq(other) || this == other
  }

  /**
    * Find the first [[TreeNode]] that satisfies the condition specified by `f`.
    * The condition is recursively applied to this node and all of its children (pre-order).
    */
  def find(f: BaseType => Boolean): Option[BaseType] = if (f(this)) {
    Some(this)
  } else {
    children.foldLeft(Option.empty[BaseType]) { (l, r) => l.orElse(r.find(f)) }
  }

  /**
    * Runs the given function on this node and then recursively on [[children]].
    * @param f the function to be applied to each node in the tree.
    */
  def foreach(f: BaseType => Unit): Unit = {
    f(this)
    children.foreach(_.foreach(f))
  }

  /**
    * Runs the given function recursively on [[children]] then on this node.
    * @param f the function to be applied to each node in the tree.
    */
  def foreachUp(f: BaseType => Unit): Unit = {
    children.foreach(_.foreachUp(f))
    f(this)
  }

  /**
    * Returns a Seq containing the result of applying the given function to each
    * node in this tree in a preorder traversal.
    * @param f the function to be applied.
    */
  def map[A](f: BaseType => A): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret += f(_))
    ret
  }

  /**
    * Returns a Seq by applying a function to all nodes in this tree and using the elements of the
    * resulting collections.
    */
  def flatMap[A](f: BaseType => TraversableOnce[A]): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret ++= f(_))
    ret
  }

  /**
    * Returns a Seq containing the result of applying a partial function to all elements in this
    * tree on which the function is defined.
    */
  def collect[B](pf: PartialFunction[BaseType, B]): Seq[B] = {
    val ret = new collection.mutable.ArrayBuffer[B]()
    val lifted = pf.lift
    foreach(node => lifted(node).foreach(ret.+=))
    ret
  }

  /**
    * Returns a Seq containing the leaves in this tree.
    */
  def collectLeaves(): Seq[BaseType] = {
    this.collect { case p if p.children.isEmpty => p }
  }

  /**
    * Finds and returns the first [[TreeNode]] of the tree for which the given partial function
    * is defined (pre-order), and applies the partial function to it.
    */
  def collectFirst[B](pf: PartialFunction[BaseType, B]): Option[B] = {
    val lifted = pf.lift
    lifted(this).orElse {
      children.foldLeft(Option.empty[B]) { (l, r) => l.orElse(r.collectFirst(pf)) }
    }
  }

  /**
    * Efficient alternative to `productIterator.map(f).toArray`.
    */
  protected def mapProductIterator[B: ClassTag](f: Any => B): Array[B] = {
    val arr = Array.ofDim[B](productArity)
    var i = 0
    while (i < arr.length) {
      arr(i) = f(productElement(i))
      i += 1
    }
    arr
  }




}

