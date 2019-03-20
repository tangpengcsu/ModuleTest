package base

import org.json4s.DefaultFormats


/** 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang
  * 创建日期：2018-03-30
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-03-30     1.0.1.0    tang      创建
  * ----------------------------------------------------------------
  */
/*
{\"name\":\"BeJson\",\"url\":\"http://www.bejson.com\",\"page\":88,\"isNonProfit\":true,\"address\":{\"street\":\"科技园路.\",\"city\":\"江苏苏州\",\"country\":\"中国\"},\"links\":[{\"name\":\"Google\",\"url\":\"http://www.google.com\"},{\"name\":\"Baidu\",\"url\":\"http://www.baidu.com\"},{\"name\":\"SoSo\",\"url\":\"http://www.SoSo.com\"}]}"  
* */
object JsonParseUtils
{
  def main(args: Array[String]): Unit =
  {
    val json =
      """
        |{
        |    "taskInfo": [{
        |            "id": "1",
        |            "sql": "select custid,$moneytype,stktype,market,stkcode,cast(sum(mktval) as   decimal(20,4)) mktval,cast(sum(pre_mktval) as decimal(20,4)) pre_mktval,cast(sum(profitcost) as decimal(20,4)) profitcost,  cast(sum(pre_profitcost) as decimal(20,4)) pre_profitcost, cast(sum(addcost) as decimal(20,4)) addcost, cast(sum(profit_day) as decimal(20,4)) profit  from st_dd_stock_profit  where dcdate = $dcdate  group by custid,$moneytype,stktype,market,stkcode",
        |            "taskType": "1",
        |            "targetTable": "TMP_RESULT",
        |            "partitionNum": "-1",
        |            "saveMode": "1",
        |            "inputParam": [{
        |                "name": "dcdate",
        |                "value": "20180808"
        |            }, {
        |                "name": "Baidu",
        |                "value": "http://www.baidu.com"
        |            }, {
        |                "name": "moneytype",
        |                "value": "moneytype"
        |            }]
        |        },
        |        {
        |                "id": "2  $Google",
        |                "sql": "2",
        |                "taskType": "1",
        |                "targetTable": "TMP_RESULT",
        |                "partitionNum": "-1",
        |                "saveMode": "1",
        |                "inputParam": [{
        |                    "name": "Google",
        |                    "value": "http://www.google.com"
        |                }, {
        |                    "name": "Baidu",
        |                    "value": "http://www.baidu.com"
        |                }, {
        |                    "name": "SoSo",
        |                    "value": "http://www.SoSo.com"
        |                }]
        |            }
        |    ]
        |}
      """.stripMargin
    new JsonParseUtils().json2Object(json)
  }
}

class JsonParseUtils
{
  def json2Object(json: String): Array[TaskInfo] =
  {
    implicit val formats = DefaultFormats
    import org.json4s.jackson.JsonMethods._
    //解析结果
    val task: Tasks = parse(json).extract[Tasks]

    task.taskInfo
  }


/*  case class InputParam(name: String, value: String)
  {
    override def toString: String = s"name:$name,value:$value"
  }

  case class TaskInfo(id :String, sql: String, taskType: String, targetTable: String, partitionNum:
  String, saveMode: String, inputParam: Array[InputParam])
  {
    override def toString: String = s"sql:$sql,taskType:$taskType,targetTable:$targetTable,inputParam:inputParam," +
      s"partitionNum:$partitionNum,saveMode:$saveMode"
  }
  case class Tasks(taskInfo: Array[TaskInfo])*/




}

