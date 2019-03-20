import java.sql.*;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2018-09-07
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-09-07     1.0.1.0    tang.peng        创建
 * ----------------------------------------------------------------
 */
public class Test
{  private static String driver = "org.apache.phoenix.jdbc.PhoenixDriver";

  public static void main(String[] args) throws SQLException
  {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    Statement stmt = null;
    ResultSet rs = null;

    Connection con = DriverManager.getConnection("jdbc:phoenix:zoo1,zoo2,zoo3:2181");
    stmt = con.createStatement();
    String sql = "select USER_ID,TAG_VALUE from DM_TAG.CUST_STRA";
    rs = stmt.executeQuery(sql);
    while (rs.next()) {
      System.out.print("USER_ID:"+rs.getString("USER_ID"));
      System.out.println("TAG_VALUE:"+rs.getString("TAG_VALUE"));
    }
    stmt.close();
    con.close();
  }
}
