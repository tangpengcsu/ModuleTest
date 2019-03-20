import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.commons.codec.binary.Base64

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2018-08-10
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-08-10     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */
object App {
  def main(args: Array[String]): Unit = {
    val dir = System.getProperty("user.dir")

    var testFileDir = dir + File.separator + args(0)
    println("文件：" + testFileDir)
    val testSource = scala.io.Source.fromFile(testFileDir)
    val testParam = testSource.mkString
    testSource.close

    val compressedArg = compress(testParam)
    val jobInfoParam = safeUrlBase64Encode(compressedArg)
    println("压缩后的入参：")
    val result = jobInfoParam.replace("\r\n", "")
    val out = new PrintWriter(testFileDir+".result")
    result.foreach(i=>{
      out.print(i)
    })
    out.close()
    println(result)


    println("success!")
  }
  //todo 待删除打印语句
  @throws[IOException]
  def compress(str: String): String = {
    if (str == null || (str.length == 0)) return str
    System.out.println("String length : " + str.length)
    val out: ByteArrayOutputStream = new ByteArrayOutputStream
    val gzip: GZIPOutputStream = new GZIPOutputStream(out)
    gzip.write(str.getBytes)
    gzip.close
    val outStr: String = new String(Base64.encodeBase64(out.toByteArray))
    outStr
  }

  @throws[IOException]
  def unCompress(str: String): String = {
    if (str == null || (str.length == 0)) return str
    val gis: GZIPInputStream = new GZIPInputStream(new ByteArrayInputStream(Base64.decodeBase64(str)))
    val outStr: String = ""
    val out: ByteArrayOutputStream = new ByteArrayOutputStream
    val buffer: Array[Byte] = new Array[Byte](256)
    var n: Int = 0

    while (n >= 0) {
      out.write(buffer, 0, n)
      n = gis.read(buffer)
    }
    new String(out.toByteArray)
  }
  def safeUrlBase64Encode(str: String): String = {
    val encodeBase64: String = Base64.encodeBase64String(str.getBytes)
    var safeBase64Str: String = encodeBase64.replace('+', '-')
    safeBase64Str = safeBase64Str.replace('/', '_')
    safeBase64Str = safeBase64Str.replaceAll("=", "")
    safeBase64Str
  }


}
