import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.*;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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


public class MysqlToHdfs
{
  private static String oracleLink = "fromOracleLink";
  private static String hdfsLink = "toHdfsLink";
  public static void main(String[] args)
  {
    //  sqoopTransfer();
    SqoopClient client = createSqoopClient();
  //createHdfsAndOracleConn(client);
    createJob(client);
   //process(client);
  }

  public static void process(SqoopClient client){


    MJob job = client.getJob("OracleToHDFSjob");

    //启动任务
    String jobId = String.valueOf(job.getPersistenceId());
    MSubmission submission = client.startJob("OracleToHDFSjob");
    System.out.println("JOB提交状态为 : " + submission.getStatus());
    while (submission.getStatus()
        .isRunning() && submission.getProgress() != -1)
    {
      System.out.println("进度 : " + String.format("%.2f %%", submission.getProgress() * 100));
      //三秒报告一次进度
      try
      {
        Thread.sleep(3000);
      } catch (InterruptedException e)
      {
        e.printStackTrace();
      }
    }
    System.out.println("JOB执行结束... ...");
    System.out.println("Hadoop任务ID为 :" + submission.getExternalJobId());
    Counters counters = submission.getCounters();
    if (counters != null)
    {
      System.out.println("计数器:");
      for (CounterGroup group : counters)
      {
        System.out.print("\t");
        System.out.println(group.getName());
        for (Counter counter : group)
        {
          System.out.print("\t\t");
          System.out.print(counter.getName());
          System.out.print(": ");
          System.out.println(counter.getValue());
        }
      }
    }
    if (submission.getError() != null)
    {
      System.out.println("JOB执行异常，异常信息为 : " + submission.getError()
          .getErrorSummary() + submission.getError()
          .getErrorDetails());
    }

    System.out.println("MySQL通过sqoop传输数据到HDFS统计执行完毕");

    //Check job statusfor a running job
    submission = client.getJobStatus(jobId);
    if (submission.getStatus()
        .isRunning() && submission.getProgress() != -1)
    {
      System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
    }
    //stop  a running job

    client.stopJob(jobId);
  }

  public static MJob createJob(SqoopClient client){
    //client.deleteJob("OracleToHDFSjob");

    MJob job = client.createJob(oracleLink, hdfsLink);
    job.setName("OracleToHDFSjobTest");

    job.setCreationUser("hadoop");


    //设置源链接任务配置信息
    MFromConfig fromJobConfig = job.getFromJobConfig();
    fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue("FSPT_KF");
    fromJobConfig.getStringInput("fromJobConfig.tableName")
        .setValue("SYS_DD");
    /*DD_ID,DD_NAME ,DD_TYPE , DD_MAINT_FLAGS, SUBSYS*/
   // List columns = Arrays.asList("SUBSYS,SUBSYS_SN,SUBSYS_TYPE,SUBSYS_STATUS,AUTH_PUB_KEY,SUBSYS_SOURCE".split(","));
    List column = new ArrayList();
/*    column.add("dd_id");
    column.add("dd_name");
    column.add("dd_type");*/
    column.add("DUMMY");
   // column.add("dd_maint_flags");
    //fromJobConfig.getListInput("fromJobConfig.columns").setValue(column);
    fromJobConfig.getStringInput("fromJobConfig.partitionColumn").setName("DD_ID");
    MToConfig toJobConfig = job.getToJobConfig();
    toJobConfig.getStringInput("toJobConfig.outputDirectory")
        .setValue("/usr/tmp");


    Status status = client.saveJob(job);  //第一次先
    //Status status = client.getJobStatus("OracleToHDFSjob").getValidationStatus();
    if (status.canProceed())
    {
      System.out.println("JOB创建成功，ID为: " + job.getPersistenceId());
    } else
    {
      System.out.println("JOB创建失败。");
    }
    return job;
  }


  public static SqoopClient createSqoopClient()
  {
    //初始化
    String url = "http://192.168.50.85:12000/sqoop/";
    SqoopClient client = new SqoopClient(url);
    return client;
  }

  public static SqoopClient createHdfsAndOracleConn(SqoopClient client)
  {

  //client.deleteLink(oracleLink);

    //创建一个源链接 JDBC
    //String fromConnectorId = "oracle-jdbc-connector";// show connector
   // MLink fromLink = client.createLink(fromConnectorId);

   /* //String name = "connectionConfig"+random.nextDouble();
    fromLink.setName(oracleLink);//这个值不能重复
    fromLink.setCreationUser("admin");

    MLinkConfig fromLinkConfig = fromLink.getConnectorLinkConfig();
    fromLinkConfig.getStringInput("connectionConfig.connectionString")
        .setValue("jdbc:oracle:thin:@10.80.1.250:1521/orcl");

    fromLinkConfig.getStringInput("connectionConfig.username")
        .setValue("fspt_kf");
    fromLinkConfig.getStringInput("connectionConfig.password")
        .setValue("fspt_kf");
    Status fromStatus = client.saveLink(fromLink);*/
    MLink fromLink = client.createLink("generic-jdbc-connector");
    //String name = "connectionConfig"+random.nextDouble();
    fromLink.setName(oracleLink);//这个值不能重复
    fromLink.setCreationUser("admin");

    MLinkConfig fromLinkConfig = fromLink.getConnectorLinkConfig();
    fromLinkConfig.getStringInput("linkConfig.connectionString")
        .setValue("jdbc:oracle:thin:@10.80.1.250:1521/orcl");
    fromLinkConfig.getStringInput("linkConfig.jdbcDriver")
        .setValue("oracle.jdbc.driver.OracleDriver");
    fromLinkConfig.getStringInput("linkConfig.username")
        .setValue("fspt_kf");
    fromLinkConfig.getStringInput("linkConfig.password")
        .setValue("fspt_kf");
    Status fromStatus = client.saveLink(fromLink);

    if (fromStatus.canProceed())
    {
      System.out.println("创建JDBC Link成功，ID为: " + fromLink.getPersistenceId());
    } else
    {
      System.out.println("创建JDBC Link失败");
    }

    //client.deleteLink(hdfsLink);
    //创建一个目的地链接HDFS
    String toConnectorId = "hdfs-connector";
    MLink toLink = client.createLink(toConnectorId);
    toLink.setName(hdfsLink);
    toLink.setCreationUser("hadoop");
    MLinkConfig toLinkConfig = toLink.getConnectorLinkConfig();
    toLinkConfig.getStringInput("linkConfig.uri")
        .setValue("hdfs://192.168.50.88:9000/");
    Status toStatus = client.saveLink(toLink);
    if (toStatus.canProceed())
    {
      System.out.println("创建HDFS Link成功，ID为: " + toLink.getPersistenceId());
    } else
    {
      System.out.println("创建HDFS Link失败");
    }
    return client;
  }


  public static void sqoopTransfer()
  {
    //初始化
    String url = "http://192.168.50.85:12000/sqoop/";
    SqoopClient client = new SqoopClient(url);

    //创建一个源链接 JDBC
    String fromConnectorId = "oracle-jdbc-connector";// show connector
    MLink fromLink = client.createLink(fromConnectorId);
    Random random = new Random(10000);
    //String name = "connectionConfig"+random.nextDouble();
    fromLink.setName(oracleLink);//这个值不能重复
    fromLink.setCreationUser("admin");

    MLinkConfig fromLinkConfig = fromLink.getConnectorLinkConfig();
    fromLinkConfig.getStringInput("connectionConfig.connectionString")
        .setValue("jdbc:oracle:thin:@10.80.1.250:1521/orcl");

    fromLinkConfig.getStringInput("connectionConfig.username")
        .setValue("fspt_kf");
    fromLinkConfig.getStringInput("connectionConfig.password")
        .setValue("fspt_kf");
    Status fromStatus = client.saveLink(fromLink);
    if (fromStatus.canProceed())
    {
      System.out.println("创建JDBC Link成功，ID为: " + fromLink.getPersistenceId());
    } else
    {
      System.out.println("创建JDBC Link失败");
    }
    //创建一个目的地链接HDFS
    String toConnectorId = "hdfs-connector";
    MLink toLink = client.createLink(toConnectorId);
    toLink.setName(hdfsLink);
    toLink.setCreationUser("hadoop");
    MLinkConfig toLinkConfig = toLink.getConnectorLinkConfig();
    toLinkConfig.getStringInput("linkConfig.uri")
        .setValue("hdfs://192.168.50.88:9000/");
    Status toStatus = client.saveLink(toLink);
    if (toStatus.canProceed())
    {
      System.out.println("创建HDFS Link成功，ID为: " + toLink.getPersistenceId());
    } else
    {
      System.out.println("创建HDFS Link失败");
    }

    //创建一个任务
    String fromLinkId = String.valueOf(fromLink.getPersistenceId());
    String toLinkId = String.valueOf(toLink.getPersistenceId());
    MJob job = client.createJob(fromLinkId, toLinkId);
    job.setName("MySQL to HDFS job");
    job.setCreationUser("admin");
    //设置源链接任务配置信息
    MFromConfig fromJobConfig = job.getFromJobConfig();
    fromJobConfig.getStringInput("fromJobConfig.schemaName")
        .setValue("sqoop");
    fromJobConfig.getStringInput("fromJobConfig.tableName")
        .setValue("sqoop");
    fromJobConfig.getStringInput("fromJobConfig.partitionColumn")
        .setValue("id");
    MToConfig toJobConfig = job.getToJobConfig();
    toJobConfig.getStringInput("toJobConfig.outputDirectory")
        .setValue("/usr/tmp");
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getStringInput("throttlingConfig.numExtractors")
        .setValue("3");

    Status status = client.saveJob(job);
    if (status.canProceed())
    {
      System.out.println("JOB创建成功，ID为: " + job.getPersistenceId());
    } else
    {
      System.out.println("JOB创建失败。");
    }

    //启动任务
    String jobId = String.valueOf(job.getPersistenceId());
    MSubmission submission = client.startJob(jobId);
    System.out.println("JOB提交状态为 : " + submission.getStatus());
    while (submission.getStatus()
        .isRunning() && submission.getProgress() != -1)
    {
      System.out.println("进度 : " + String.format("%.2f %%", submission.getProgress() * 100));
      //三秒报告一次进度
      try
      {
        Thread.sleep(3000);
      } catch (InterruptedException e)
      {
        e.printStackTrace();
      }
    }
    System.out.println("JOB执行结束... ...");
    System.out.println("Hadoop任务ID为 :" + submission.getExternalJobId());
    Counters counters = submission.getCounters();
    if (counters != null)
    {
      System.out.println("计数器:");
      for (CounterGroup group : counters)
      {
        System.out.print("\t");
        System.out.println(group.getName());
        for (Counter counter : group)
        {
          System.out.print("\t\t");
          System.out.print(counter.getName());
          System.out.print(": ");
          System.out.println(counter.getValue());
        }
      }
    }
    if (submission.getError() != null)
    {
      System.out.println("JOB执行异常，异常信息为 : " + submission.getError()
          .getErrorSummary() + submission.getError()
          .getErrorDetails());
    }

    System.out.println("MySQL通过sqoop传输数据到HDFS统计执行完毕");

    //Check job statusfor a running job
    submission = client.getJobStatus(jobId);
    if (submission.getStatus()
        .isRunning() && submission.getProgress() != -1)
    {
      System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
    }
    //stop  a running job

    client.stopJob(jobId);

  }

}
