import base.{HbaseClient, HbaseUtils, RowKeyGenerator}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Hello world!
  *
  */
object App
{
  def main(args: Array[String]): Unit =
  {
    val sparkSession = getSparkContext("test")
    val sparkContext = sparkSession.sparkContext
    //val tableName = "FSPT:SJSMX"
    val tableName = "TEST2"
    import org.apache.hadoop.hbase.client.Scan
    val scan = new Scan
    val fields = List("MXBFZH","MXCJJG")
    val rowFamily = Bytes.toBytes("R")
    //test(sparkContext,tableName)

    //selectFields(sparkContext,tableName,args(0).toInt)
   //insertWithMutiFamily(sparkSession)
    file(sparkSession,tableName,args(0).toInt,args(1).toInt,args(2).toInt)
   // readFile(sparkContext)
  }
  def readFile(sparkContext:SparkContext):Unit={
    sparkContext.textFile("hdfs://192.168.50.88:9000/FSPT/TEST")
  }
  def read (sparkContext:SparkContext): Unit ={
    HbaseClient.scanRaw(sparkContext,"TEST1",new Scan())
  }
  /**
    * 功能描述: 获得上下文环境变量
    */
  private def getSparkContext(p_strName:String) =
  {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val spark = SparkSession
      .builder
        // .master("local[*]")
      .appName(p_strName)
      .getOrCreate()
    spark
  }

  def test(sparkContext: SparkContext,tableName:String): Unit =
  {

    import org.apache.hadoop.hbase.client.Scan
    val scan = new Scan
    val fields = List("MXBFZH","MXCJJG")
    val rowFamily = Bytes.toBytes("R")
    val result = HbaseUtils.scanAsRow(sparkContext, tableName, scan, fields, rowFamily)
  }


  def insert(sparkSession: SparkSession):Unit = {
    val tableName = "TEST1"
    val fieldNum = 40
    val sum = 100

    val selectFieldList = List("ID" )
    val selectTableName = "SZ_SJSMX1"
    val selectData = HbaseUtils.scanAsRow(sparkSession.sparkContext, selectTableName, new Scan, selectFieldList)



    val settBatNo= 15
    val taskSn = ""
    val gen = new RowKeyGenerator(24, settBatNo,taskSn)

    HbaseClient.writeToHBWithBulkLoadNewApi1(tableName,selectData,gen,
    "R",selectFieldList,
    false)
  }

  /**
    * 批量插入 hbase 表数据 测试
    * 入参 100 2 3
    *      100 行数据 2列
    * 建表：
    * create 'TEST2',{NAME => 'R'}
    * spark submit :$SPARK_HOME/bin/spark-submit --class App --master yarn --deploy-mode client ./hbase-1.0-SNAPSHOT.jar 1000000 20 3


    * @param sparkSession
    * @param sum
    * @param fileLength
    * @param isHbase
    */
  def file(sparkSession: SparkSession,tableName: String,sum :Int,fileLength :Int,isHbase :Int): Unit ={

    var Listb = new ListBuffer[Int]
    var i = 0
    while(i<sum){
      Listb.append(i)
      i += 1
    }
   var valueList = new ListBuffer[String]

    var j = 0
  while(j<fileLength-1){
    valueList.append("Value"+j)
    j+=1
  }
    val rddOriginal: RDD[Int] = sparkSession.sparkContext.parallelize(Listb, 24)

    val selectData = rddOriginal.map(strPosition => {


     Row.fromSeq(strPosition.toString()  +: valueList)
    })
    if(isHbase == 0) {
      selectData.saveAsTextFile("hdfs://192.168.50.88:9000/FSPT/TEST")
      //selectData.filter(r=>r.getString(0).equals("100")).foreach(i=>println(i))
      val fi = selectData.filter(r=>r.getString(0).equals("100"))
    // val result= selectData.union(selectData.filter(r=>r.getString(0).equals("100")))
     val result= selectData.union(fi.map(iii=>Row.fromSeq(Seq(iii.get(0)))))
     result.saveAsTextFile("hdfs://192.168.50" +
        ".88:9000/FSPT/TEST1")
    }else {
      val gen = new RowKeyGenerator(24, 15, "")
      HbaseClient.writeToHBWithBulkLoadNewApi3(tableName, selectData, gen, "R", ("ID" +: valueList).toList, false)
    }
  }
  def insertWithMutiFamily(sparkSession: SparkSession):Unit = {
    val tableName = "TEST1"
    val fieldNum = 40
    val sum = 100

    val selectFieldList = List("ID","MXBFZH","MXCJJG","MXCJRQ","MXCJSL",
      "MXDDBH","MXFSRQ","MXGFXZ","MXGHF","MXHBDH",
      "MXJGGF","MXJSF","MXJSFS","MXJSRQ","MXJSZH",
      "MXJYDY","MXJYFS","MXJYJSF",


      "MXQSBJ","MXQSJG","MXQSRQ",
      "MXQSSL","MXQSYJ","MXQTFY","MXSFJE","MXSJLX",
      "MXSXF","MXTGDY","MXYHS","MXYWLB","MXYYB",
      "MXZJJE","MXZQDM","MXZQLB","MXZQZH"
      )
    /*
       "MXQSBJ","MXQSJG1","MXQSRQ",
      "MXQSSL","MXQSYJ","MXQTFY","MXSFJE","MXSJLX",
      "MXSXF","MXTGDY","MXYHS","MXYWLB","MXYYB",
      "MXZJJE","MXZQDM","MXZQLB","MXZQZH"
   */

    /*

      "MXQSBJ1","MXQSJG1","MXQSRQ1",
      "MXQSSL1","MXQSYJ1","MXQTFY1","MXSFJE1","MXSJLX1",
      "MXSXF1","MXTGDY1","MXYHS1","MXYWLB1","MXYYB1",
      "MXZJJE1","MXZQDM1","MXZQLB1","MXZQZH1"
    * */
    val familyList = List("R","R","R","R","R",
      "R","R","R","R","R",
      "R","R","R","R","R",
      "R","R","R",
/*      "R","R","R","R","R",
      "R","R","R","R","R",
      "R","R","R","R","R",
      "R","R"*/


      "X","X","X","X","X",
      "X","X","X","X","X",
      "X","X","X","X","X",
      "X","X"

      )
    val selectTableName = "SZ_SJSMX1"
    val selectData = HbaseUtils.scanAsRow(sparkSession.sparkContext, selectTableName, new Scan, selectFieldList)
    //FrameWorklog.info("========"+ selectData.count())
    val settBatNo= 15
    val taskSn = ""
    val gen = new RowKeyGenerator(24, settBatNo,taskSn)
    selectData.take(1)

    HbaseClient.writeToHBWithBulkLoadNewApi2(tableName,selectData,gen,
      familyList,selectFieldList,
      false)

  }

  def field ():Unit={
    val sjsmx1 =List("ID","MXBFZH","MXCJJG","MXCJRQ","MXCJSL",
      "MXDDBH","MXFSRQ","MXGFXZ","MXGHF","MXHBDH",
      "MXJGGF","MXJSF","MXJSFS","MXJSRQ","MXJSZH",
      "MXJYDY","MXJYFS","MXJYJSF","MXQSBJ","MXQSJG",
      "MXQSRQ","MXQSSL","MXQSYJ","MXQTFY","MXSFJE",
      "MXSJLX","MXSXF","MXTGDY","MXYHS","MXYWLB",
      "MXYYB","MXZJJE","MXZQDM","MXZQLB","MXZQZH",

      "MXBFZH1","MXCJJG1","MXCJRQ1","MXCJSL1","MXDDBH1",
      "MXFSRQ1","MXGFXZ1","MXGHF1","MXHBDH1","MXJGGF1",
      "MXJSF1","MXJSFS1","MXJSRQ1","MXJSZH1", "MXJYDY1",
      "MXJYFS1","MXJYJSF1","MXQSBJ1","MXQSJG1","MXQSRQ1",
      "MXQSSL1","MXQSYJ1","MXQTFY1","MXSFJE1","MXSJLX1",
      "MXSXF1","MXTGDY1","MXYHS1","MXYWLB1","MXYYB1",
      "MXZJJE1","MXZQDM1","MXZQLB1","MXZQZH1",

      "MXBFZH2","MXCJJG2","MXCJRQ2","MXCJSL2","MXDDBH2",
      "MXFSRQ2","MXGFXZ2","MXGHF2","MXHBDH2","MXJGGF2",
      "MXJSF2","MXJSFS2","MXJSRQ2","MXJSZH2", "MXJYDY2",
      "MXJYFS2","MXJYJSF2","MXQSBJ2","MXQSJG2","MXQSRQ2",
      "MXQSSL2","MXQSYJ2","MXQTFY2","MXSFJE2","MXSJLX2",
      "MXSXF2","MXTGDY2","MXYHS2","MXYWLB2","MXYYB2",
      "MXZJJE2","MXZQDM2","MXZQLB2","MXZQZH2"
    )
    val result = List(


      "m_chDlvyCurrency","m_chFundDlvyPreStatus","m_chFundDlvyStatus","m_chMarket","m_chOpRole","m_chPreProcStatus","m_chShareDlvyPreStatus","m_chShareDlvyStatus",
      "m_clPreProcTime","m_dBondInt","m_dClearingFee","m_dCollateralFee","m_dCommision","m_dCustodyFee","m_dExchRate","m_dExchTransFee","m_dFeeAmt","m_dHandleFee","m_dMatchAmt",
      "m_dMatchPrice","m_dMatchQty","m_dOrderFrzAmt","m_dOrderPrice","m_dOrderQty","m_dOrderUfzAmt","m_dPaidinCom","m_dSecuReguFee","m_dSettAmt","m_dSettQty","m_dSettleFee",
      "m_dStampDuty","m_dTransferFee","m_dTrdRegFee","m_dVentureFee","m_dWithdrawnQty","m_dbFundForceFrz","m_dbFundTrdFrz","m_dbFundTrdUfz","m_dbStkForceFrz","m_dbStkTrdFrz",
      "m_dbStkTrdUfz",    "m_iBkpDate","m_iFundAvlDate","m_iFundDlvyDate","m_iIntOrg","m_iMatchCnt","m_iMatchTime","m_iOpOrg","m_iOrderBsn","m_iOrderSn",
      "m_iPreProcCode","m_iSettDate",    "m_iSettFileRecSn","m_iShareAvlDate","m_iShareDlvyDate","m_iSubsys","m_iTrdDate","m_lOrderRecSn","m_lPerbizOrderRecSn","m_lRecSn",
      "m_strBizCode","m_strBoard","m_strOpName",    "m_strOpUser","m_strOrderId","m_strPreProcMsg","m_strSettBatNo","m_strSettFileNo","m_strSettFileTmplId","m_strStkCls",
      "m_strStkCode","m_strStkName","m_strStkpbu","m_strStktu",      "m_strTrdacct")
    val selectFieldList = List(
      "m_lRecSn",
      "m_chFundDlvyPreStatus","m_chFundDlvyStatus","m_chMarket","m_chOpRole","m_chPreProcStatus","m_chShareDlvyPreStatus","m_chShareDlvyStatus","m_clPreProcTime","m_dBondInt",
      "m_dClearingFee","m_dCollateralFee","m_dCommision","m_dCustodyFee","m_dExchRate","m_dExchTransFee","m_dFeeAmt","m_dHandleFee","m_dMatchAmt","m_dMatchPrice",
      "m_dMatchQty","m_dOrderFrzAmt","m_dOrderPrice","m_dOrderQty","m_dOrderUfzAmt","m_dPaidinCom","m_dSecuReguFee","m_dSettAmt","m_dSettQty","m_dSettleFee",
      "m_dStampDuty","m_dTransferFee","m_dTrdRegFee","m_dVentureFee","m_dWithdrawnQty","m_dbFundForceFrz","m_dbFundTrdFrz","m_dbFundTrdUfz","m_dbStkForceFrz","m_dbStkTrdFrz",
      "m_dbStkTrdUfz",    "m_iBkpDate","m_iFundAvlDate","m_iFundDlvyDate","m_iIntOrg","m_iMatchCnt","m_iMatchTime","m_iOpOrg","m_iOrderBsn","m_iOrderSn",
      "m_iPreProcCode","m_iSettDate",    "m_iSettFileRecSn","m_iShareAvlDate","m_iShareDlvyDate","m_iSubsys","m_iTrdDate","m_lOrderRecSn","m_lPerbizOrderRecSn","m_chDlvyCurrency",
      "m_strBizCode","m_strBoard","m_strOpName",    "m_strOpUser","m_strOrderId","m_strPreProcMsg","m_strSettBatNo","m_strSettFileNo","m_strSettFileTmplId","m_strStkCls",
      "m_strStkCode","m_strStkName","m_strStkpbu","m_strStktu",      "m_strTrdacct")
  }

}
