import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.hive.visitor.HiveSchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2018-06-14
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-06-14     1.0.1.0    tang.peng        创建
 * ----------------------------------------------------------------
 */
public class ParseSQL
{

  public static void main(String[] args ){

    // String sql = "update t set name = 'x' where id < 100 limit 10";
    //String sql = "SELECT ID, NAME, AGE FROM USER WHERE ID = ? limit 2";
    // String sql = "select * from tablename limit 10";
    /*
    SELECT T.SSYS_DICT_CD   ,
    T.SSYS_DICTEN_CD ,--源系统字典项代码
       T.SYS_SRC        ,--系统来源
       T.MDL_DICT_CD    ,--模型字典代码
       T.MDL_DICTEN_CD   --模型字典项代码
  FROM DW.CODE_DICT_CODE T

  SELECT
  T1.BANK_ACCT       AS BNK_ACC_ID     , --银行账户编码
  T1.INT_ORG         AS BNK_ID         , --银行编码
  T3.MDL_DICTEN_CD   AS CRRC_CD        , --币种代码,代码表[DIMLS009]
  '04'               AS BNK_ACC_CLAS_CD, --银行账户类别代码,代码表[DIMLS229]
  T2.USER_NAME       AS ACC_NAME       , --账户名称
  T1.CUST_CODE       AS CUST_ID        , --客户编码
  T4.MDL_DICTEN_CD   AS CERT_TYPE_CD   , --证件类型代码,代码表[DIMLS259]
  T2.ID_CODE         AS CERT_NUM       , --证件号码
  T1.CUACCT_CODE     AS CPTL_ACC_ID    , --资金账户编码
  ''                 AS BNK_ACC_USE_CD , --银行账户用途代码,代码表[DIMLS230]
  ''                 AS PAY_MODE_CD    , --支付方式代码,代码表[DIMLS266]
  ''                 AS PAY_ORG_ID     , --支付机构编码
  ''                 AS SIGN_TYPE_CD   , --签约类型代码,代码表[DIMLS162]
  T5.MDL_DICTEN_CD   AS SIGN_STAT_CD   , --签约状态代码,代码表[DIMLS163]
  T1.ACCT_DRAW_UPLMT AS OUT_TOPL       , --转出上限
  0                  AS OUT_LOWL       , --转出下限
  0                  AS TAC_LMT        , --总出金额度
  T1.ACCT_DRAWN_AMT  AS USD_AC_LMT       --已用出金额度
FROM
  ODS.KUAS_CUBSB_CONTRACT T1 --客户银证业务签约
  LEFT JOIN ODS.KUAS_USERS T2 ON T1.CUST_CODE=T2.USER_CODE --用户
  LEFT JOIN ( SELECT T.SSYS_DICT_CD   ,
    T.SSYS_DICTEN_CD ,--源系统字典项代码
       T.SYS_SRC        ,--系统来源
       T.MDL_DICT_CD    ,--模型字典代码
       T.MDL_DICTEN_CD   --模型字典项代码
  FROM DW.CODE_DICT_CODE T) T3 ON T1.CURRENCY = T3.SSYS_DICTEN_CD AND T3.SYS_SRC ='KUAS' AND T3.MDL_DICT_CD ='DIMLS009'
  LEFT JOIN ( SELECT T.SSYS_DICT_CD   ,
    T.SSYS_DICTEN_CD ,--源系统字典项代码
       T.SYS_SRC        ,--系统来源
       T.MDL_DICT_CD    ,--模型字典代码
       T.MDL_DICTEN_CD   --模型字典项代码
  FROM DW.CODE_DICT_CODE T) T4 ON T2.ID_TYPE = T4.SSYS_DICTEN_CD AND T4.SYS_SRC ='KUAS' AND T4.MDL_DICT_CD ='DIMLS259'
  LEFT JOIN ( SELECT T.SSYS_DICT_CD   ,
    T.SSYS_DICTEN_CD ,--源系统字典项代码
       T.SYS_SRC        ,--系统来源
       T.MDL_DICT_CD    ,--模型字典代码
       T.MDL_DICTEN_CD   --模型字典项代码
  FROM DW.CODE_DICT_CODE T) T5 ON T1.CONTRACT_STATUS = T5.SSYS_DICTEN_CD AND T5.SYS_SRC ='KUAS' AND T5.MDL_DICT_CD ='DIMLS163'
UNION ALL
SELECT
  T1.BANK_ACCT       AS BNK_ACC_ID     , --银行账户编码
  T1.BANK_CODE       AS BNK_ID         , --银行编码
  '0'                AS CRRC_CD        , --币种代码,代码表[DIMLS009]
  T3.MDL_DICTEN_CD   AS BNK_ACC_CLAS_CD, --银行账户类别代码,代码表[DIMLS229]
  T2.USER_NAME       AS ACC_NAME       , --账户名称
  T1.CUST_CODE       AS CUST_ID        , --客户编码
  T4.MDL_DICTEN_CD   AS CERT_TYPE_CD   , --证件类型代码,代码表[DIMLS259]
  T2.ID_CODE         AS CERT_NUM       , --证件号码
  T1.CUACCT_CODE     AS CPTL_ACC_ID    , --资金账户编码
  ''                 AS BNK_ACC_USE_CD , --银行账户用途代码,代码表[DIMLS230]
  ''                 AS PAY_MODE_CD    , --支付方式代码,代码表[DIMLS266]
  T1.PAY_ORG         AS PAY_ORG_ID     , --支付机构编码
  ''                 AS SIGN_TYPE_CD   , --签约类型代码,代码表[DIMLS162]
  ''                 AS SIGN_STAT_CD   , --签约状态代码,代码表[DIMLS163]
  0                  AS OUT_TOPL       , --转出上限
  0                  AS OUT_LOWL       , --转出下限
  0                  AS TAC_LMT        , --总出金额度
  0                  AS USD_AC_LMT       --已用出金额度
FROM
  ODS.KUAS_OTC_CUST_PAYER T1 --第三方支付账户表
  LEFT JOIN ODS.KUAS_USERS T2 ON T1.CUST_CODE=T2.USER_CODE --用户
  LEFT JOIN TEMP_DICT_CODE T3 ON T1.BANK_ACCT_TYPE = T3.SSYS_DICTEN_CD AND T3.SYS_SRC ='KUAS' AND T3.MDL_DICT_CD ='DIMLS229'
  LEFT JOIN TEMP_DICT_CODE T4 ON T2.ID_TYPE = T4.SSYS_DICTEN_CD AND T4.SYS_SRC ='KUAS' AND T4.MDL_DICT_CD ='DIMLS259'
    * */
    String sql = " SELECT\n" +
        "  T1.BANK_ACCT       AS BNK_ACC_ID     , --银行账户编码\n" +
        "  T1.INT_ORG         AS BNK_ID         , --银行编码\n" +
        "  T3.MDL_DICTEN_CD   AS CRRC_CD        , --币种代码,代码表[DIMLS009]\n" +
        "  '04'               AS BNK_ACC_CLAS_CD, --银行账户类别代码,代码表[DIMLS229]\n" +
        "  T2.USER_NAME       AS ACC_NAME       , --账户名称\n" +
        "  T1.CUST_CODE       AS CUST_ID        , --客户编码\n" +
        "  T4.MDL_DICTEN_CD   AS CERT_TYPE_CD   , --证件类型代码,代码表[DIMLS259]\n" +
        "  T2.ID_CODE         AS CERT_NUM       , --证件号码\n" +
        "  T1.CUACCT_CODE     AS CPTL_ACC_ID    , --资金账户编码\n" +
        "  ''                 AS BNK_ACC_USE_CD , --银行账户用途代码,代码表[DIMLS230]\n" +
        "  ''                 AS PAY_MODE_CD    , --支付方式代码,代码表[DIMLS266]\n" +
        "  ''                 AS PAY_ORG_ID     , --支付机构编码\n" +
        "  ''                 AS SIGN_TYPE_CD   , --签约类型代码,代码表[DIMLS162]\n" +
        "  T5.MDL_DICTEN_CD   AS SIGN_STAT_CD   , --签约状态代码,代码表[DIMLS163]\n" +
        "  T1.ACCT_DRAW_UPLMT AS OUT_TOPL       , --转出上限\n" +
        "  0                  AS OUT_LOWL       , --转出下限\n" +
        "  0                  AS TAC_LMT        , --总出金额度\n" +
        "  T1.ACCT_DRAWN_AMT  AS USD_AC_LMT       --已用出金额度\n" +
        "FROM \n" +
        "  ODS.KUAS_CUBSB_CONTRACT T1 --客户银证业务签约\n" +
        "  LEFT JOIN ODS.KUAS_USERS T2 ON T1.CUST_CODE=T2.USER_CODE --用户\n" +
        "  LEFT JOIN ( SELECT T.SSYS_DICT_CD   ,\n" +
        "    T.SSYS_DICTEN_CD ,--源系统字典项代码\n" +
        "       T.SYS_SRC        ,--系统来源\n" +
        "       T.MDL_DICT_CD    ,--模型字典代码\n" +
        "       T.MDL_DICTEN_CD   --模型字典项代码\n" +
        "  FROM DW.CODE_DICT_CODE T) T3 ON T1.CURRENCY = T3.SSYS_DICTEN_CD AND T3.SYS_SRC ='KUAS' AND T3.MDL_DICT_CD ='DIMLS009'\n" +
        "  LEFT JOIN ( SELECT T.SSYS_DICT_CD   ,\n" +
        "    T.SSYS_DICTEN_CD ,--源系统字典项代码\n" +
        "       T.SYS_SRC        ,--系统来源\n" +
        "       T.MDL_DICT_CD    ,--模型字典代码\n" +
        "       T.MDL_DICTEN_CD   --模型字典项代码\n" +
        "  FROM DW.CODE_DICT_CODE T) T4 ON T2.ID_TYPE = T4.SSYS_DICTEN_CD AND T4.SYS_SRC ='KUAS' AND T4.MDL_DICT_CD ='DIMLS259'\n" +
        "  LEFT JOIN ( SELECT T.SSYS_DICT_CD   ,\n" +
        "    T.SSYS_DICTEN_CD ,--源系统字典项代码\n" +
        "       T.SYS_SRC        ,--系统来源\n" +
        "       T.MDL_DICT_CD    ,--模型字典代码\n" +
        "       T.MDL_DICTEN_CD   --模型字典项代码\n" +
        "  FROM DW.CODE_DICT_CODE T) T5 ON T1.CONTRACT_STATUS = T5.SSYS_DICTEN_CD AND T5.SYS_SRC ='KUAS' AND T5.MDL_DICT_CD ='DIMLS163'\n" +
        "UNION \n" +
        "SELECT\n" +
        "  V1.BANK_ACCT       AS BNK_ACC_ID     , --银行账户编码\n" +
        "  V1.BANK_CODE       AS BNK_ID         , --银行编码\n" +
        "  '0'                AS CRRC_CD        , --币种代码,代码表[DIMLS009]\n" +
        "  V3.MDL_DICTEN_CD   AS BNK_ACC_CLAS_CD, --银行账户类别代码,代码表[DIMLS229]\n" +
        "  V2.USER_NAME       AS ACC_NAME       , --账户名称\n" +
        "  cast(V1.CUST_CODE as string)       AS CUST_ID        , --客户编码\n" +
        "  V4.MDL_DICTEN_CD   AS CERT_TYPE_CD   , --证件类型代码,代码表[DIMLS259]\n" +
        "  V2.ID_CODE         AS CERT_NUM       , --证件号码\n" +
        "  V1.CUACCT_CODE     AS CPTL_ACC_ID    , --资金账户编码\n" +
        "  ''                 AS BNK_ACC_USE_CD , --银行账户用途代码,代码表[DIMLS230]\n" +
        "  ''                 AS PAY_MODE_CD    , --支付方式代码,代码表[DIMLS266]\n" +
        "  V1.PAY_ORG         AS PAY_ORG_ID     , --支付机构编码\n" +
        "  ''                 AS SIGN_TYPE_CD   , --签约类型代码,代码表[DIMLS162]\n" +
        "  ''                 AS SIGN_STAT_CD   , --签约状态代码,代码表[DIMLS163]\n" +
        "  0                  AS OUT_TOPL       , --转出上限\n" +
        "  0                  AS OUT_LOWL       , --转出下限\n" +
        "  0                  AS TAC_LMT        , --总出金额度\n" +
        "  0                  AS USD_AC_LMT       --已用出金额度\n" +
        "FROM \n" +
        "  ODS.KUAS_OTC_CUST_PAYER V1 --第三方支付账户表\n" +
        "  LEFT JOIN ODS.KUAS_USERS V2 ON V1.CUST_CODE=V2.USER_CODE --用户\n" +
        "  LEFT JOIN ( SELECT T.SSYS_DICT_CD   ,\n" +
        "    T.SSYS_DICTEN_CD ,--源系统字典项代码\n" +
        "       T.SYS_SRC        ,--系统来源\n" +
        "       T.MDL_DICT_CD    ,--模型字典代码\n" +
        "       T.MDL_DICTEN_CD   --模型字典项代码\n" +
        "  FROM DW.CODE_DICT_CODE T) V3 ON V1.BANK_ACCT_TYPE = V3.SSYS_DICTEN_CD AND V3.SYS_SRC ='KUAS' AND V3.MDL_DICT_CD ='DIMLS229'\n" +
        "  LEFT JOIN ( SELECT T.SSYS_DICT_CD   ,\n" +
        "    T.SSYS_DICTEN_CD ,--源系统字典项代码\n" +
        "       T.SYS_SRC        ,--系统来源\n" +
        "       T.MDL_DICT_CD    ,--模型字典代码\n" +
        "       T.MDL_DICTEN_CD   --模型字典项代码\n" +
        "  FROM DW.CODE_DICT_CODE T) V4 ON V2.ID_TYPE = V4.SSYS_DICTEN_CD AND V4.SYS_SRC ='KUAS' AND V4.MDL_DICT_CD ='DIMLS259' " +
        " ";
  // sql = "select e.user as user1 from emp_table e left join (select name,addr from  ttt_table) t on t.name = e.name";
    String dbType = JdbcConstants.HIVE;

    //格式化输出
    String result = SQLUtils.format(sql, dbType);
    System.out.println(result); // 缺省大写格式
    List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

    //解析出的独立语句的个数
    System.out.println("size is:" + stmtList.size());
    for (int i = 0; i < stmtList.size(); i++) {

      SQLStatement stmt = stmtList.get(i);

      HiveSchemaStatVisitor visitor = new HiveSchemaStatVisitor();

      stmt.accept(visitor);




      //获取表名称
      //System.out.println("Tables : " + visitor.getCurrentTable());
      //获取操作方法名称,依赖于表名称
      System.out.println("Manipulation : " + visitor.getTables());
      //获取字段名称
      System.out.println("fields : " + visitor.getColumns());
      //System.out.println("aliasMap : " + visitor.getAliasMap());
      System.out.println("getRelationships : " + visitor.getRelationships());

    }

  }

}