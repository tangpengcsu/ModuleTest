import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2018-06-08
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-06-08     1.0.1.0    tang.peng        创建
 * ----------------------------------------------------------------
 */
public class ConsumerDemo
{
  public static void main(String[] args){
    String topic = "test";
    Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.50.88:9092");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty("group.id", "6");
    props.setProperty("enable.auto.commit", "true");
    props.setProperty("auto.offset.reset", "earliest");
    //max.partition.fetch.bytes
    props.setProperty("max.partition.fetch.bytes", "13107200");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
    consumer.subscribe(Arrays.asList(topic));
    int z = 0;
    while (z<10) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      System.out.println("======count:"+records.count());
      for (ConsumerRecord<String, String> record : records) {
        ///System.out.println("=====value:"+record.value());
        z = z+1;
        System.out.println("=====key:"+record.key()+",offset:"+record.offset()+",partition"+record.partition()+",topic:"+record.topic());
        //System.out.println("=====value:"+record.value());
//              consumer.seekToBeginning(new TopicPartition(record.topic(), record.partition()));
        Map<TopicPartition,OffsetAndMetadata> commitOffset= Collections.singletonMap(new TopicPartition(record.topic(),record.partition()),
            new OffsetAndMetadata(record.offset() + 1));
        consumer.commitSync(commitOffset);
      }
      System.out.println("=====z:" + z);

    }
  }
}
