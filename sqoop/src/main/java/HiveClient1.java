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


import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.slf4j.Logger;

import java.util.List;

public class HiveClient1 {
  protected final Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());
  IMetaStoreClient client;

  public HiveClient1() {
    try {
      HiveConf hiveConf = new HiveConf();
      hiveConf.addResource("hive-site.xml");
      client = RetryingMetaStoreClient.getProxy(hiveConf);
    } catch (MetaException ex) {
      logger.error(ex.getMessage());
    }
  }

  public List<String> getAllDatabases() {
    List<String> databases = null;
    try {
      databases = client.getAllDatabases();
    } catch (TException ex) {
      logger.error(ex.getMessage());
    }
    return databases;
  }

  public Database getDatabase(String db) {
    Database database = null;
    try {
      database = client.getDatabase(db);
    } catch (TException ex) {
      logger.error(ex.getMessage());
    }
    return database;
  }

  public List<FieldSchema> getSchema(String db, String table) {
    List<FieldSchema> schema = null;
    try {
      schema = client.getSchema(db, table);
    } catch (TException ex) {
      logger.error(ex.getMessage());
    }
    return schema;
  }

  public List<String> getAllTables(String db) {
    List<String> tables = null;
    try {
      tables = client.getAllTables(db);
    } catch (TException ex) {
      logger.error(ex.getMessage());
    }
    return tables;
  }

  public String getLocation(String db, String table) {
    String location = null;
    try {
      location = client.getTable(db, table).getSd().getLocation();
    }catch (TException ex) {
      logger.error(ex.getMessage());
    }
    return location;
  }

}
