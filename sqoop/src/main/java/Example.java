import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.*;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

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
public class Example
{
  public static void sqoopTransfer(){
    String url = "http://localhost:12000/sqoop/";
    SqoopClient client = new SqoopClient(url);
//Creating dummy job object
    MJob job = client.createJob("fromLinkName", "toLinkName");
    job.setName("Vampire");
    job.setCreationUser("Buffy");
// set the "FROM" link job config values
    MFromConfig fromJobConfig = job.getFromJobConfig();
    fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue("sqoop");
    fromJobConfig.getStringInput("fromJobConfig.tableName").setValue("sqoop");
    fromJobConfig.getStringInput("fromJobConfig.partitionColumn").setValue("id");
// set the "TO" link job config values
    MToConfig toJobConfig = job.getToJobConfig();
    toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue("/usr/tmp");
// set the driver config values
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getStringInput("throttlingConfig.numExtractors").setValue("3");

    Status status = client.saveJob(job);
    if(status.canProceed()) {
      System.out.println("Created Job with Job Name: "+ job.getName());
    } else {
      System.out.println("Something went wrong creating the job");
    }

//Job start
    MSubmission submission = client.startJob("jobName");
    System.out.println("Job Submission Status : " + submission.getStatus());
    if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
      System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
    }
    System.out.println("Hadoop job id :" + submission.getExternalJobId());
    System.out.println("Job link : " + submission.getExternalLink());
    Counters counters = submission.getCounters();
    if(counters != null) {
      System.out.println("Counters:");
      for(CounterGroup group : counters) {
        System.out.print("\t");
        System.out.println(group.getName());
        for(Counter counter : group) {
          System.out.print("\t\t");
          System.out.print(counter.getName());
          System.out.print(": ");
          System.out.println(counter.getValue());
        }
      }
    }
/*
    if(submission.getExceptionInfo() != null) {
      System.out.println("Exception info : " +submission.getExceptionInfo());
    }
*/


//Check job status for a running job
    submission = client.getJobStatus("jobName");
    if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
      System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
    }

//Stop a running job
    client.stopJob("jobName");




  }
}
