/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2018-05-21
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-05-21     1.0.1.0    tang.peng        创建
 * ----------------------------------------------------------------
 */


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class ProducerDemo
{
  public static void main(String[] args)
  {


    Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.50.88:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(props);
/*    int i = 0;
    while(true){
      producer.send(new ProducerRecord<String, String>("test1", Integer.toString(i), i+","+(i+1)));
      i++;

    }*/
    String topic = "uc";

    String[] zhangshan = {json1, json2, json3};
    String[] lishi = {json11, json12, json13};
    String[] wanger = {json21, json22, json23};

    for (int i = 0; i < 100000; i++)
    {

      producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), zhangshan[i%3]));
    }


   /* for (int i = 2100109872; i < 2100109872+100; i++)
      producer.send(new ProducerRecord<String, String>("test1", Integer.toString(i), ""+(i+1)));*/

    producer.close();
  }

  public static String json1 = "{\n" +
      "    \"table\": \"UC.LOGIN_INFO\",\n" +
      "    \"detail\": {\n" +
      "        \"LOGIN_TYPE\": \"phone\",\n" +
      "        \"LOGIN_ACCOUNT\": \"zhangshangacc\",\n" +
      "        \"LOGIN_CHANNEL\": \"app\",\n" +
      "        \"USER_NO\": \"1116614349\",\n" +
      "        \"USER_NAME\": \"zhangshang\",\n" +
      "        \"LOGIN_TIME\": \"2018-08-08\"\n" +
      "    }\n" +
      "}";
  public static String json2 = "{\n" +
      "    \"table\": \"UC.LOGIN_INFO\",\n" +
      "    \"detail\": {\n" +
      "        \"LOGIN_TYPE\": \"wx\",\n" +
      "        \"LOGIN_ACCOUNT\": \"zhangshangacc\",\n" +
      "        \"LOGIN_CHANNEL\": \"app\",\n" +
      "        \"USER_NO\": \"1116614349\",\n" +
      "        \"USER_NAME\": \"zhangshang\",\n" +
      "        \"LOGIN_TIME\": \"2018-08-08\"\n" +
      "    }\n" +
      "}";
  public static String json3 = "{\n" +
      "    \"table\": \"UC.LOGIN_INFO\",\n" +
      "    \"detail\": {\n" +
      "        \"LOGIN_TYPE\": \"qq\",\n" +
      "        \"LOGIN_ACCOUNT\": \"zhangshangacc\",\n" +
      "        \"LOGIN_CHANNEL\": \"app\",\n" +
      "        \"USER_NO\": \"1116614349\",\n" +
      "        \"USER_NAME\": \"zhangshang\",\n" +
      "        \"LOGIN_TIME\": \"2018-08-08\"\n" +
      "    }\n" +
      "}";
  public static String json11 = "{\n" +
      "    \"table\": \"UC.LOGIN_INFO\",\n" +
      "    \"detail\": {\n" +
      "        \"LOGIN_TYPE\": \"phone\",\n" +
      "        \"LOGIN_ACCOUNT\": \"lishiacc\",\n" +
      "        \"LOGIN_CHANNEL\": \"app\",\n" +
      "        \"USER_NO\": \"1116614349\",\n" +
      "        \"USER_NAME\": \"lishi\",\n" +
      "        \"LOGIN_TIME\": \"2018-08-08\"\n" +
      "    }\n" +
      "}";
  public static String json12 = "{\n" +
      "    \"table\": \"UC.LOGIN_INFO\",\n" +
      "    \"detail\": {\n" +
      "        \"LOGIN_TYPE\": \"wx\",\n" +
      "        \"LOGIN_ACCOUNT\": \"lishiacc\",\n" +
      "        \"LOGIN_CHANNEL\": \"app\",\n" +
      "        \"USER_NO\": \"1116614349\",\n" +
      "        \"USER_NAME\": \"lishi\",\n" +
      "        \"LOGIN_TIME\": \"2018-08-08\"\n" +
      "    }\n" +
      "}";
  public static String json13 = "{\n" +
      "    \"table\": \"UC.LOGIN_INFO\",\n" +
      "    \"detail\": {\n" +
      "        \"LOGIN_TYPE\": \"qq\",\n" +
      "        \"LOGIN_ACCOUNT\": \"lishiacc\",\n" +
      "        \"LOGIN_CHANNEL\": \"app\",\n" +
      "        \"USER_NO\": \"1116614349\",\n" +
      "        \"USER_NAME\": \"lishi\",\n" +
      "        \"LOGIN_TIME\": \"2018-08-08\"\n" +
      "    }\n" +
      "}";

  public static String json21 ="{\n" +
      "    \"table\": \"UC.LOGIN_INFO1\",\n" +
      "    \"timestamp\": \"1532769621990\",\n" +
      "    \"detail\": {\n" +
      "        \"LOGIN_TYPE\": \"wx\",\n" +
      "        \"LOGIN_ACCOUNT\": \"wangeracc\",\n" +
      "        \"LOGIN_CHANNEL\": \"app\",\n" +
      "        \"USER_NO\": \"1116614310\",\n" +
      "        \"USER_NAME\": \"wanger\",\n" +
      "        \"LOGIN_TIME\": \"2018-08-08\"\n" +
      "    }\n" +
      "}";
  public static String json22 ="{\n" +
      "    \"table\": \"UC.LOGIN_INFO1\",\n" +
      "    \"timestamp\": \"1532769621990\",\n" +
      "    \"detail\": {\n" +
      "        \"LOGIN_TYPE\": \"phone\",\n" +
      "        \"LOGIN_ACCOUNT\": \"wangeracc\",\n" +
      "        \"LOGIN_CHANNEL\": \"app\",\n" +
      "        \"USER_NO\": \"1116614310\",\n" +
      "        \"USER_NAME\": \"wanger\",\n" +
      "        \"LOGIN_TIME\": \"2018-08-08\"\n" +
      "    }\n" +
      "}";
  public static String json23 ="{\n" +
      "    \"table\": \"UC.LOGIN_INFO1\",\n" +
      "    \"timestamp\": \"1532769621990\",\n" +
      "    \"detail\": {\n" +
      "        \"LOGIN_TYPE\": \"qq\",\n" +
      "        \"LOGIN_ACCOUNT\": \"wangeracc\",\n" +
      "        \"LOGIN_CHANNEL\": \"app\",\n" +
      "        \"USER_NO\": \"1116614310\",\n" +
      "        \"USER_NAME\": \"wanger\",\n" +
      "        \"LOGIN_TIME\": \"2018-08-08\"\n" +
      "    }\n" +
      "}";

}