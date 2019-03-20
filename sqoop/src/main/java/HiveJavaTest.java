import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

/**
 * 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang
 * 创建日期：2018-03-22
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-03-22     1.0.1.0    tang      创建
 * ----------------------------------------------------------------
 */
public class HiveJavaTest
{
  public static void main(String[] args){
    HiveClient1 client = new HiveClient1();
  Table table = new Table();

   List<String> database =  client.getAllTables("default");
    //database.forEach(item-> System.out.print(item));
    //table.setFieldValue();

/*    try
    {
      client.client.createTable(table);
    } catch (TException e)
    {
      e.printStackTrace();
    }*/
System.out.println("========");
  }
}
