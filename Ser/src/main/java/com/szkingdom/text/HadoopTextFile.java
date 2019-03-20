package com.szkingdom.text;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.szkingdom.Reader;
import com.szkingdom.Writer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
public class HadoopTextFile implements Writer<List<Map<String, Object>>>, Reader<List<Map<String, String>>> {

    private FileSystem fileSystem;
    private String filePath;
    private Map<Integer, String> mapper;//key 为写入字段的顺序，value为字段名

    private HadoopTextFile() {
    }

    public HadoopTextFile(FileSystem fileSystem, String filePath, Map<Integer, String> mapper) {
        this.fileSystem = fileSystem;
        this.filePath = filePath;
        this.mapper = mapper;
    }


    /**
     * @param data Map 中，key 为写入字段名，value为字段值
     * @throws IOException
     */
    @Override
    public void write(List<Map<String, Object>> data) throws IOException {

        FSDataOutputStream fout = null;
        BufferedWriter out = null;
        Path path = null;
//        int size = data.size();
        try {

            path = new Path(filePath);
            fout = fileSystem.create(path);
            out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));

            for (Map<String, Object> v : data) {
                List<String> result = new ArrayList<>();
                mapper.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey()
                        ).forEachOrdered(e->{
                        result.add(v.get(e.getValue()).toString());
                });
                String value = Joiner.on(HadoopCfg.DEFAULT_SEPARATOR).join(result);
                out.write(value);
                out.newLine();
                out.flush();
            }
        } finally {
            if (out != null) {
                out.close();
            }
            if (fout != null) {
                fout.close();
            }
        }

    }

    /**
     * @return 字典，字段值的集合
     * @throws IOException
     */
    @Override
    public List<Map<String, String>> read() throws IOException {

        List<Map<String, String>> result = new ArrayList<>();
        FSDataInputStream fin = null;
        BufferedReader in = null;
        Map<String, String> map;

        String line;
        try {
            fin = fileSystem.open(new Path(filePath));
            in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));

            while ((line = in.readLine()) != null) {
                map = new HashMap<>();
                Iterator<String> it = Splitter.on(HadoopCfg.DEFAULT_SEPARATOR).split(line).iterator();
                int i = 0;
                while (it.hasNext()) {
                    String v = it.next();
                    map.put(mapper.get(i + 1), v);//字段序号是从1 开始
                    i++;
                }
                if (map != null && !map.isEmpty()) {
                    result.add(map);
                }
            }

        } finally {
            if (in != null) {
                in.close();
            }
            if (fin != null) {
                fin.close();
            }
        }
        return result;
    }
}
