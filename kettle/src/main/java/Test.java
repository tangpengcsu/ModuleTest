import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;
import org.pentaho.di.trans.steps.xbaseinput.XBaseInputMeta;

import java.util.Arrays;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2019/3/16
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2019/3/16     1.0.1.0       tang.peng     创建
 * ----------------------------------------------------------------
 */
public class Test {

    public static void main(String[] args1) throws Exception{
        KettleEnvironment.init();

        /*        TransMeta transMeta1 = new TransMeta("L:\\ETL\\ceshi\\1.ktr");
        Trans trans1 = new Trans(transMeta1);*/
        TransMeta transMeta = new TransMeta( "transFile.ktr", "myTrans" );
        transMeta.setLogLevel(LogLevel.DEBUG);
        //   transMeta.setTransformationType(TransMeta.TransformationType.Normal);

        String name = "uc postgres";
        String type = "POSTGRESQL";
        String access = "Native";
        String host = "10.201.83.54";
        String db = "kucs_admin";
        String port = "5432";
        String user = "postgres";
        String pass = "postgres";

        DatabaseMeta databaseMeta = new DatabaseMeta(name,type,access,host,db,port,user,pass);
        databaseMeta.setUsingConnectionPool(true);
 /*       PostgreSQLDatabaseMeta postgreSQLDatabaseMeta = new PostgreSQLDatabaseMeta();
        postgreSQLDatabaseMeta.setAccessType();*/
        transMeta.setDatabases(Arrays.asList(databaseMeta));


        XBaseInputMeta loadDBF = new XBaseInputMeta();
        loadDBF.setDbfFileName("E:\\Users\\Documents\\大数据分析平台\\kettle\\SJSJG.DBF");
        StepMeta step1 = new StepMeta( "XBase输入", loadDBF );

        SelectValuesMeta selectValuesMeta = new SelectValuesMeta();
        SelectValuesMeta.SelectField f1 = new SelectValuesMeta.SelectField();
        f1.setName("JGJSZH");
        f1.setRename("f1");
        SelectValuesMeta.SelectField f2 = new SelectValuesMeta.SelectField();
        f2.setName("JGBFZH");
        f2.setRename("f2");
        SelectValuesMeta.SelectField[] selectFields = {f1,f2};
        selectValuesMeta.setSelectFields(selectFields);


        StepMeta step2 = new StepMeta( "字段选择", selectValuesMeta );
     /*   ClusterSchema clusterSchema = new ClusterSchema();
        step2.setClusterSchema(clusterSchema);*/
        TransHopMeta hopMeta1 = new TransHopMeta( step1, step2, true );
        TableOutputMeta tableOutputMeta = new TableOutputMeta();
        tableOutputMeta.setTableName("test1");
        tableOutputMeta.setCommitSize(1000);
//        DatabaseMeta databaseMeta = new DatabaseMeta();
        tableOutputMeta.setDatabaseMeta(databaseMeta);
        tableOutputMeta.setTruncateTable(false);

        StepMeta step3 = new StepMeta( "表输出", tableOutputMeta );
        TransHopMeta hopMeta2 = new TransHopMeta( step2, step3, true );
        transMeta.addStep(step1);
        transMeta.addStep(step2);
        transMeta.addStep(step3);
        transMeta.addTransHop(0,hopMeta1);
        transMeta.addTransHop(1,hopMeta2);

        System.out.println(transMeta.getXML());
        Trans trans = new Trans(transMeta);
        String[] args = {""};
        trans.execute(args);

        Thread.sleep(5000000);
    }
}
