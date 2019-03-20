import java.sql.*;

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
public class HiveClient
{

  private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
  private void getConnect()throws SQLException{
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    Connection con = DriverManager.getConnection("jdbc:hive2://192.168.50.88:10002/default", "hive", "hive");
    Statement stmt = con.createStatement();
    String tableName = "wyphao";
    stmt.execute("drop table if exists " + tableName);
    stmt.execute("create table " + tableName +
        " (key int, value string)");
    System.out.println("Create table success!");
    // show tables
    String sql = "show tables '" + tableName + "'";
    System.out.println("Running: " + sql);
    ResultSet res = stmt.executeQuery(sql);
    if (res.next()) {
      System.out.println(res.getString(1));
    }

    // describe table
    sql = "describe " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1) + "\t" + res.getString(2));
    }


    sql = "select * from " + tableName;
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(String.valueOf(res.getInt(1)) + "\t"
          + res.getString(2));
    }

    sql = "select count(1) from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1));
    }

  }
}
