package kryo;

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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.objenesis.strategy.StdInstantiatorStrategy;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KyroSerializable {

    public static void main(String[] args) throws IOException {
        long start =  System.currentTimeMillis();
        setSerializableObject();
        System.out.println("Kryo 序列化时间:" + (System.currentTimeMillis() - start) + " ms" );
        start =  System.currentTimeMillis();
        getSerializableObject();
        System.out.println("Kryo 反序列化时间:" + (System.currentTimeMillis() - start) + " ms");

    }

    public static void setSerializableObject() throws FileNotFoundException{

        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.register(Simple.class);
        Output output = new Output(new FileOutputStream("D:/file1.bin"));
        for (int i = 0; i < 100000; i++) {
            Map<String,Integer> map = new HashMap<String, Integer>(2);
            map.put("zhang0", i);
            map.put("zhang1", i);
            kryo.writeObject(output, new Simple("zhang"+i,(i+1),map));
        }
        output.flush();
        output.close();
    }


    public static void getSerializableObject(){
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        Input input;
        try {
            input = new Input(new FileInputStream("D:/file1.bin"));
            Simple simple =null;
            while((simple=kryo.readObject(input, Simple.class)) != null){
                //System.out.println(simple.getAge() + "  " + simple.getName() + "  " + simple.getMap().toString());
            }

            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch(KryoException e){

        }
    }

}
