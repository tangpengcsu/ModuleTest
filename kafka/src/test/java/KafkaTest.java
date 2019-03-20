import kafka.admin.ConsumerGroupCommand;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2018-11-20
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-11-20     1.0.1.0    tang.peng        创建
 * ----------------------------------------------------------------
 */

public class KafkaTest
{

  public static void main(String[] args){
    String topic  = "--topic uc_tag";
    String bootstrap = "--bootstrap-server 192.168.50.88:9092";
    String group = "--group 3";
    String zookeeper = "--zookeeper 192.168.50.88:2181";
    String describe = "--describe";
    String listStrZk = "--zookeeper 192.168.50.88:2181 --list";
    String listStr = "--bootstrap-server 192.168.50.88:9092 --list  --new-consumer";
    String[] listArgs = listStr.split(" ");
    String describeStrzk = "--zookeeper 192.168.50.88:2181 --describe --group 3";
    String describeStr = "--bootstrap-server 192.168.50.88:9092 --describe --group 5 --new-consumer";
    String[] describeArgs = describeStr.split(" ");

    // ConsumerGroupCommand.main(listArgs)
    ConsumerGroupCommand.main(describeArgs);
  }
}
