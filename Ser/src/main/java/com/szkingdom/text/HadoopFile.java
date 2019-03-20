package com.szkingdom.text;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.szkingdom.Reader;
import com.szkingdom.Writer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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
public class HadoopFile implements Writer<List<Map<String, Object>>>, Reader<List<Map<String, Object>>> {

    private FileSystem fileSystem;
    private String filePath;
    private HbaseTableStructInfo[] hbaseTableStructInfos; //按字段序号排序后的

    private HadoopFile() {
    }

    /**
     *
     * @param fileSystem
     * @param filePath
     * @param hbaseTableStructInfos
     */
    public HadoopFile(FileSystem fileSystem, String filePath, HbaseTableStructInfo[] hbaseTableStructInfos) {
        this.fileSystem = fileSystem;
        this.filePath = filePath;
        this.hbaseTableStructInfos = hbaseTableStructInfos;
    }

    public HadoopFile(FileSystem fileSystem, String filePath, List<HbaseTableStructInfo> hbaseTableStructInfos) {
        this.fileSystem = fileSystem;
        this.filePath = filePath;
        this.hbaseTableStructInfos =
                hbaseTableStructInfos.stream().sorted(Comparator.comparing(HbaseTableStructInfo::getFieldSn)).collect(Collectors.toList()).toArray(new HbaseTableStructInfo[hbaseTableStructInfos.size()]);
    }

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

                for (int i = 0; i < hbaseTableStructInfos.length; i++) {
                    result.add(getValue(v, hbaseTableStructInfos[i].getFieldCode(),
                            hbaseTableStructInfos[i].getDefaultValue()));
                }
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

    @Override
    public List<Map<String, Object>> read() throws IOException {
        List<Map<String, Object>> result = new ArrayList<>();
        FSDataInputStream fin = null;
        BufferedReader in = null;
        Map<String, Object> map;

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
                    HbaseTableStructInfo info = hbaseTableStructInfos[i];
                    map.put(info.getFieldCode(), getData(v, info.getDataType(), info.getDecimalDigits()));
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

    /**
     * 补全字段默认值
     *
     * @param data         数据
     * @param fieldCode    字段名
     * @param defalutValue 字典默认值
     * @return
     */
    public String getValue(Map<String, Object> data, String fieldCode, String defalutValue) {
        String value;
        if (data.containsKey(fieldCode)) {
            value = data.getOrDefault(fieldCode, defalutValue).toString();
        } else {
            value = defalutValue;
        }
        return value;
    }

    /**
     * 转换字符串至指定数据类型
     * //数据类型
     * //SMALLINT-16位整型数
     * //INTEGER-32位整型数
     * //BIGINT-64位整型数
     * //NUMBER
     * //CHAR
     * //VARCHAR
     * //TIMESTAMP
     * //DATE
     * //TIME
     *
     * @param value         待转换的值
     * @param dataType      数据类型
     * @param decimalDigits 数值类型的精度
     * @return
     */
    public Object getData(String value, String dataType, Integer decimalDigits) {
        Object result;

        if (Objects.equal(dataType, "SMALLINT")) {
            result = Short.valueOf(value.trim());
        } else if (Objects.equal(dataType, "INTEGER")) {
            result = Integer.valueOf(value.trim());
        } else if (Objects.equal(dataType, "BIGINT")) {
            result = Long.valueOf(value.trim());
        } else if (Objects.equal(dataType, "NUMBER")) {
            BigDecimal b = new BigDecimal(value);
            b.setScale(decimalDigits);
            result = b;
        } else if (Objects.equal(dataType, "CHAR")) {
            result = Character.valueOf(value.trim().charAt(0));
        } else if (Objects.equal(dataType, "TIMESTAMP")) {
            result = Timestamp.valueOf(value);
        } else if (Objects.equal(dataType, "DATE")) {
            result = Date.valueOf(value);
        } else if (Objects.equal(dataType, "TIME")) {
            result = Time.valueOf(value);
        } else {
            result = value;
        }
        return result;
    }
}
