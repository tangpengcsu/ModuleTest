package com.szkingdom.hadoop;

import com.google.common.base.Strings;
import com.szkingdom.hadoop.base.HdfsFile;
import com.szkingdom.hadoop.bean.FieldInfo;
import org.apache.hadoop.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

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
public class HdfsWriterJob {


    public void process(List<Map<String, Object>> dataList, String filePath, int partitionNum, FileSystem fileSystem,
                        List<FieldInfo> infos) throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();

        int totalSize = dataList.size();

        int stepSize = getStepSize(totalSize,partitionNum);

        System.out.println("step.size:"+stepSize);
        int currentStep = 0;
        int currentPartition = 0;
        List<FutureTask<Boolean>> taskList =new ArrayList<>();
        FutureTask<Boolean> task;
        while (currentStep < totalSize && currentPartition<partitionNum) {
            int endStep = getEndStep(currentStep,stepSize,totalSize);
            List<Map<String,Object>> partitonData = dataList.subList(currentStep,endStep);
            String file = getPath(filePath, currentPartition);
            Callable<Boolean> callable = new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    new HdfsFile(fileSystem, file, infos).write(partitonData);
                    return true;
                }
            };
            task = new FutureTask<Boolean>(callable);
            taskList.add(task);
            executor.submit(task);
            System.out.println(currentPartition+"-----"+currentStep+"->"+endStep);
            currentPartition++;

            currentStep = endStep;

        }
      for(int i =0;i<taskList.size();i++){
          FutureTask<Boolean> t =  taskList.get(i);
          System.out.println(t.get());
      }


    }

    public int getStepSize(int totalSize,int partitionNum){
        int stepSize = totalSize / partitionNum;

        if(totalSize< partitionNum){
            stepSize = totalSize;
        }
       // int mode = totalSize % partitionNum;
        if ((totalSize % partitionNum) != 0) {
            stepSize = stepSize + 1;
        }
        return stepSize;
    }

    public int getEndStep(int currentStep, int stepSize, int totalSize){
        int result = currentStep + stepSize;
        if(result > totalSize){
            result = totalSize;
        }
        return result;

    }
    public String getPath(String path, int partition) {
        String result;
        /*part-00000*/
        String partitionFile = "part-" + Strings.padStart(String.valueOf(partition), 5, '0');

        if (path.endsWith("/")) {
            result = path.trim() + partitionFile;
        } else {
            result = path.trim() + "/" + partitionFile;
        }
        return result;
    }


}
