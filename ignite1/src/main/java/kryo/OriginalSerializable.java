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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;


public class OriginalSerializable {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        long start =  System.currentTimeMillis();
        setSerializableObject();
        System.out.println("java原生序列化时间:" + (System.currentTimeMillis() - start) + " ms" );
        start =  System.currentTimeMillis();
        getSerializableObject();
        System.out.println("java原生反序列化时间:" + (System.currentTimeMillis() - start) + " ms");
    }

    public static void setSerializableObject() throws IOException{

        FileOutputStream fo = new FileOutputStream("D:/file2.bin");

        ObjectOutputStream so = new ObjectOutputStream(fo);

        for (int i = 0; i < 100000; i++) {
            Map<String,Integer> map = new HashMap<String, Integer>(2);
            map.put("zhang0", i);
            map.put("zhang1", i);
            so.writeObject(new Simple("zhang"+i,(i+1),map));
        }
        so.flush();
        so.close();
    }

    public static void getSerializableObject(){
        FileInputStream fi;
        try {
            fi = new FileInputStream("D:/file2.bin");
            ObjectInputStream si = new ObjectInputStream(fi);

            Simple simple =null;
            while((simple=(Simple)si.readObject()) != null){
                //System.out.println(simple.getAge() + "  " + simple.getName());
            }
            fi.close();
            si.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            //e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}