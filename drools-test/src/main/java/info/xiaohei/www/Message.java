package info.xiaohei.www;

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2018-07-09
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-07-09     1.0.1.0    tang.peng        创建
 * ----------------------------------------------------------------
 */
public class Message
{
  public static final int HELLO = 0;
  public static final int GOODBYE = 1;

  private String message;

  private int status;

  public String getMessage()
  {
    return this.message;
  }

  public void setMessage(String message)
  {
    this.message = message;
  }

  public int getStatus()
  {
    return this.status;
  }

  public void setStatus(int status)
  {
    this.status = status;
  }

  @Override
  public String toString()
  {
    return "Message{" +
        "message='" + message + '\'' +
        ", status=" + status +
        '}';
  }

  public static void main(String[] args)
  {
    //从工厂中获得KieServices实例
    KieServices kieServices = KieServices.Factory.get();

    //从KieServices中获得KieContainer实例，其会加载kmodule.xml文件并load规则文件
    KieContainer kieContainer = kieServices.getKieClasspathContainer();

    //建立KieSession到规则文件的通信管道
    KieSession kSession = kieContainer.newKieSession("ksession-rules");

    Message message = new Message();

    message.setMessage("Hello World");

    message.setStatus(Message.HELLO);

    //将实体类插入执行规则
    FactHandle fh = kSession.insert(message);

    int num =kSession.fireAllRules();

  }
}