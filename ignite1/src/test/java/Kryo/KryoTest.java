package Kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import kryo.Simple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2019/3/18
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2019/3/18     1.0.1.0       tang.peng     创建
 * ----------------------------------------------------------------
 */
public class KryoTest {
    FileSystem fileSystem;
    String inputFileNmae = "/ml/wc.input6";
    Path path;

    @Before
    public void init() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://SparkMaster:9000/");
        fileSystem = FileSystem.get(configuration);
        path = new Path(inputFileNmae);

    }
    @Test
    public void write()  throws Exception {
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.register(Simple.class);
        Output output = new Output(fileSystem.create(path));
        for (int i = 0; i < 100000; i++) {
            Map<String,Integer> map = new HashMap<String, Integer>(2);
            map.put("zhang0", i);
            map.put("zhang1", i);
            kryo.writeObject(output, new Simple("zhang"+i,(i+1),map));
        }
        output.flush();
        output.close();

        //FSDataOutputStream outStream = fileSystem.create(path);
    }



    @Test
    public void read() throws Exception{
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        Input input;
        try {
            FSDataInputStream fsis = fileSystem.open(path);
            input = new Input(fsis);
            Simple simple =null;
            while((simple=kryo.readObject(input, Simple.class)) != null){
                System.out.println(simple);
                //System.out.println(simple.getAge() + "  " + simple.getName() + "  " + simple.getMap().toString());
            }

            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch(KryoException e){

        }
    }

}
