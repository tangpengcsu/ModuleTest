package hadoop;
import com.szkingdom.Writer;
import com.szkingdom.text.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2019/3/20
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2019/3/20     1.0.1.0       tang.peng     创建
 * ----------------------------------------------------------------
 */
public class TextTest {
    FileSystem fileSystem;
    // String inputFileNmae = "/tmp/kk/test/obj/order";
    String   inputFileNmae = "/ml/spark-test";
    Path path;
    Configuration conf;

    @Before
    public void init() throws Exception {
        conf = new Configuration();
        // conf.set("fs.defaultFS", "hdfs://10.80.1.242:8020/");
        conf.set("fs.defaultFS", "hdfs://SparkMaster:9000/");
        fileSystem = FileSystem.get(conf);

        path = new Path(inputFileNmae);

    }
    @Test
    public void write() throws Exception{
        String path = "/ml/hadoop/write";

        Path p = new Path(path);
        if(fileSystem.exists(p)){
            fileSystem.delete(p,false);
        }
        List<Map<String,Object>> data = new ArrayList<>();
        Map<String,Object> m  = null;
        for(int i = 0;i<100;i++){
            m = new HashMap();
            for(int j =1;j<10;j++){
                m.put("f_"+j,"value_"+i+"_"+j);
            }
            data.add(m);
        }
        Map<Integer,String> mapper = new HashMap<>();
        for(int v=1;v<10;v++){
            mapper.put(v,"f_"+v);
        }
        HadoopTextFile hadoopTextFile = new HadoopTextFile(fileSystem,path,mapper);
        hadoopTextFile.write(data);

      List readResult =  hadoopTextFile.read();
      readResult.stream().forEach(i->{
          System.out.println(i);
      });
    }

    @Test
    public void other(){
        Character DEFAULT_SEPARATOR  = 9;
        System.out.println("faf"+DEFAULT_SEPARATOR);
    }
    @Test
    public void testMap(){
        Map<Integer,Object> m = new HashMap<>();
        m.put(2,"ueiri");
        m.put(99,"jfjf99");
        m.put(1,"jfjf11");
        m.put(2,"jfjf12");
        m.put(18,"jfjf18");
        m.put(16,"jfjf16");
        m.put(17,"jfjf17");

        Map<Integer, Object> result = new LinkedHashMap<>();
        m.entrySet().stream()
                .sorted(Map.Entry.<Integer, Object>comparingByKey()
                        ).forEachOrdered(e -> result.put(e.getKey(), e.getValue()));

        System.out.println(result);
    }

    @Test
    public void testHbaseTable() throws Exception{
        List<HbaseTableStructInfo> infos = new ArrayList<>();
        HbaseTableStructInfo info = new HbaseTableStructInfo();
        info.setFieldCode("f1");
        info.setFieldSn(1);
        info.setDataType("INTEGER");
        info.setDefaultValue("55");

        HbaseTableStructInfo info1 = new HbaseTableStructInfo();
        info1.setFieldCode("f2");
        info1.setFieldSn(2);
        info1.setDataType("BIGINT");
        info1.setDefaultValue("9888");


        HbaseTableStructInfo info2 = new HbaseTableStructInfo();
        info2.setFieldCode("f3");
        info2.setFieldSn(3);
        info2.setDataType("NUMBER");
        info2.setDecimalDigits(2);
        info2.setDefaultValue("89.09");

        HbaseTableStructInfo info3 = new HbaseTableStructInfo();
        info3.setFieldCode("f4");
        info3.setFieldSn(4);
        info3.setDataType("VARCHAR");
        info3.setDefaultValue("vachahf11343");

        infos.add(info);
        infos.add(info3);
        infos.add(info2);
        infos.add(info1);

        List<Map<String, Object>> data = new ArrayList<>();
        Map<String,Object> map;
        for (int i = 0; i < 10; i++) {
             map = new HashMap<>();

            if (i != 0) {
                map.put("f1", 19 * (i + 1));
            }
            if (i != 5) {
                map.put("f2", 20L * (i + 1));
            }
            if (i != 7) {
                map.put("f3", new BigDecimal(i + 1));
            }
            if (i != 9) {
                map.put("f4", "varchar_" + i);
            }

            data.add(map);

        }
        //System.out.println(data);
        String path = "/ml/hadoop/write";

     /*   Path p = new Path(path);
        if(fileSystem.exists(p)){
            fileSystem.delete(p,false);
        }*/
        HadoopFile hadoopFile = new HadoopFile(fileSystem,path,infos);
       // hadoopFile.write(data);
        System.out.println(hadoopFile.read());
    }


}
