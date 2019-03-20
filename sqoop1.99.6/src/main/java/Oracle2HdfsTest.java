import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.*;
import org.apache.sqoop.validation.Status;

/**
 * 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang
 * 创建日期：2018-03-28
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-03-28     1.0.1.0    tang      创建
 * ----------------------------------------------------------------
 */
public class Oracle2HdfsTest
{
  //show connector or show connector --all
  private static long jdbcConnectorId = 1; // for jdbc connect
  private static long hdfsConnectorId = 3; // for hdfs connector
  //show link
  private static long jdbcLinkId = 5; // for jdbc link id
  private static long hdfsLinkId = 6; // for hdfs link id
 //show job
  private static long oracle2hdfsJobId = 4;// for job id
  // sqoop url
  private static String url = "http://192.168.50.85:12000/sqoop/";

  private static SqoopClient client = null;

  public static void main(String[] args)
  {
    buildSqoopClient();
    //createOracleLink();
   // createHdfsLink();
    //createJob();
    startJob();
  }

  public static void startJob()
  {
    MSubmission preSubmission = client.getJobStatus(oracle2hdfsJobId);
    if(preSubmission.getStatus().isRunning()){
      System.out.println("job "+oracle2hdfsJobId+"正在运行");
      return;
    }


    MSubmission submission = client.startJob(oracle2hdfsJobId);
    System.out.println("Job Submission Status : " + submission.getStatus());

    System.out.println("Hadoop job id :" + submission.getPersistenceId());
    System.out.println("Job link : " + submission.getExternalLink());

    if (submission.getError().getErrorDetails() != null)
    {
      System.out.println("Exception info : " + submission.getError()
          .getErrorDetails());
    }


//Check job status for a running job
    double progressCount  = 0;

    while (true)
    {
      MSubmission checkSubmission = client.getJobStatus(oracle2hdfsJobId);
      if (checkSubmission.getStatus()
          .isRunning() && checkSubmission.getProgress() != -1)
      {
        double progress = checkSubmission.getProgress() * 100;
        if (progressCount != progress &&(progress % 10 == 0))
        {
          System.out.println();
          System.out.println("Progress : " + String.format("%.2f %%", checkSubmission.getProgress() * 100));
        }else {
          System.out.print(".");
        }
        progressCount = progress;
      } else
      {
        break;
      }
    }

   // checkSubmission.getStatus().isRunning() && checkSubmission.getStatus().isFailure()
    System.out.println("运行结束");
    //client.stopJob(oracle2hdfsJobId);
//Stop a running job


  }

  public static void buildSqoopClient()
  {
    if (null == client)
      client = new SqoopClient(url);
  }

  /**
   *
   * @return job id
   */
  public static long createOracleLink ()
  {

    MLink link = client.createLink(jdbcConnectorId);
    String linkName = "oracleLink";
    link.setName(linkName);
    link.setCreationUser("hadoop");
    MLinkConfig linkConfig = link.getConnectorLinkConfig();
// fill in the link config values
    linkConfig.getStringInput("linkConfig.connectionString")
        .setValue("jdbc:oracle:thin:@10.80.1.250:1521/orcl");
    linkConfig.getStringInput("linkConfig.jdbcDriver")
        .setValue("oracle.jdbc.driver.OracleDriver");
    linkConfig.getStringInput("linkConfig.username")
        .setValue("fspt_kf");
    linkConfig.getStringInput("linkConfig.password")
        .setValue("fspt_kf");
    jdbcLinkId = link.getPersistenceId();
// save the link object that was filled

    Status status = null;
    try
    {
      status = client.saveLink(link);
    } catch (Exception e)
    {
      System.out.println("Link 已存在：Link 名 " + linkName+" 不能重复");
      e.printStackTrace();
    }
    if (status.canProceed())
    {
      System.out.println("Created Link with oracle Link Id : " + link.getPersistenceId());
    } else
    {
      System.out.println("Something went wrong creating the link");
    }
    return jdbcLinkId;

  }

  /**
   *
   * @return job id
   */
  public static long createHdfsLink()
  {

    MLink link = client.createLink(hdfsConnectorId);
    String linkName = "hdfsLink";
    link.setName(linkName);
    link.setCreationUser("hadoop");
    MLinkConfig linkConfig = link.getConnectorLinkConfig();
// fill in the link config values
    linkConfig.getStringInput("linkConfig.uri")
        .setValue("hdfs://192.168.50.88:9000/");
// save the link object that was filled
    hdfsLinkId = link.getPersistenceId();
    Status status = null;
    try
    {
      status = client.saveLink(link);
    } catch (Exception e)
    {
      System.out.println("Link 已存在：Link 名 " + linkName+" 不能重复");
      e.printStackTrace();
    }
    if (status.canProceed())
    {
      System.out.println("Created Link with hdfs Link Id : " + link.getPersistenceId());
    } else
    {
      System.out.println("Something went wrong creating the link");
    }
    return hdfsLinkId;

  }

  /**
   *
   * @return job id
   */
  public static long createJob()
  {

    MJob job = client.createJob(jdbcLinkId, hdfsLinkId);
    job.setName("oracle2hdfs");
    job.setCreationUser("hadoop");
// set the "FROM" link job config values
    MFromConfig fromJobConfig = job.getFromJobConfig();
  /*  fromJobConfig.getStringInput("fromJobConfig.schemaName")
        .setValue("sqoop");
    fromJobConfig.getStringInput("fromJobConfig.tableName")
        .setValue("sqoop");*/
    // ${CONDITIONS} 等价于 1=1 ;配置 fromJobConfig.sql 参数就不能不能配置 schemaName和tablename;
    fromJobConfig.getStringInput("fromJobConfig.sql")
        .setValue("SELECT DD_ID,DD_NAME,DD_TYPE,DD_MAINT_FLAGS,SUBSYS FROM SYS_DD WHERE ${CONDITIONS}");
    //必须要指定partitonColumn
    fromJobConfig.getStringInput("fromJobConfig.partitionColumn")
        .setValue("DD_ID");
// set the "TO" link job config values
    MToConfig toJobConfig = job.getToJobConfig();
    toJobConfig.getStringInput("toJobConfig.outputDirectory")
        .setValue("/usr/tmp");
// set the driver config values
    MDriverConfig driverConfig = job.getDriverConfig();
/*    driverConfig.getStringInput("throttlingConfig.numExtractors")
        .setValue("3");*/

    Status status = client.saveJob(job);
    if (status.canProceed())
    {
      System.out.println("Created oracle to hdfs Job with Job Id: " + job.getPersistenceId());
    } else
    {
      System.out.println("Something went wrong creating the job");
    }
    return job.getPersistenceId();
  }

}
