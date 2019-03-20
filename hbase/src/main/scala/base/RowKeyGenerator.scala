package base

/**
  * author: sunj@szkingdom.com
  * description:rowKey生成器
  * 为避免数据操作的热点问题，充分利用hbase rowkey 的字典序排序的特点，
  * rowkey前缀为客户的证券账户与分区数取模，不足3位则前面补0，
  * 保证rowkey为固定长度。如果数据未包含证券账户信息，
  * 则rowkey前缀为证券代码与分区数取模，不足3位则前面补0.
  * create in: 2017/9/5.
  */
class RowKeyGenerator(partitions:Int, setBatNo:Int, taskCode:String)  extends Serializable {
  //初始化Key的三位索引
  protected val INIT_PRE_COUNT=3
  //记录数的长度限制
  protected val INIT_LAST_COUNT=16
  def generateKey(recordId:Int, extCode: String*): String ={
    val preIndex=recordId.toInt%partitions
    val f1 = s"%0"+INIT_PRE_COUNT+"d"
    val f2 = s"%0"+INIT_LAST_COUNT+"d"
    val f1Str = String.format(f1,new Integer(preIndex))
    val f2Str = String.format(f2,new Integer(recordId))
    var extStr:String=""
    for(str<-extCode){
      extStr+=str
    }
//    f1Str+setBatNo.toString+taskCode+extStr+f2Str
    f1Str+f2Str
  }
}
