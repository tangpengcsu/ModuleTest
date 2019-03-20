/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2018-04-27
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-04-27     1.0.1.0    tang.peng        创建
 * ----------------------------------------------------------------
 */

import net.sf.json.JSONArray;
import org.apache.poi.ss.usermodel.*;

import java.io.*;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * excel表格转成json
 *
 * @author LiYonghui
 * @ClassName: old.App
 * @Description:TODO(这里用一句话描述这个类的作用)
 * @date 2017年1月6日 下午4:42:43
 */
public class App
{
  //常亮，用作第一种模板类型，如下图
  private static final int HEADER_VALUE_TYPE_Z = 1;
  //第二种模板类型，如下图
  private static final int HEADER_VALUE_TYPE_S = 2;

  public static void main(String[] args) throws Exception
  {

   /* String path = Class.class.getResource("/" + args[0])
        .getPath();*/
     //String path ="E:\\\\Users\\\\Documents\\\\大数据分析平台\\\\test(4).xlsx";
     String path =args[0];
     String outputPath = args[1];

   // InputStream path = Class.class.getClassLoader().getResourceAsStream("/"+args[0]);

    //InputStream outputPath = Class.class.getClassLoader().getResourceAsStream("/"+args[1]);
   // String outputPath =  Class.class.getResource("/")    .getPath()+args[1];
    // path = "E:\\Users\\Documents\\大数据分析平台\\test.xlsx"

    System.out.println("读取文件" + path);
    File dir = new File(path);
    App excelHelper = getExcel2JSONHelper();
    //dir文件，0代表是第一行为保存到数据库或者实体类的表头，一般为英文的字符串，2代表是第二种模板，
    JSONArray jsonArray = excelHelper.readExcle(dir, 0, 2);

    StringBuilder json = new StringBuilder();

    String param = jsonArray.toString().substring(1, jsonArray.toString().length() - 1);
    json.append(param);
    System.out.println(json);
    String outputString = URLEncoder.encode(json.toString(), "UTF-8");
    String prefix = "$SPARK_HOME/bin/spark-submit   --class com.szkingdom.kdbi.job.core.AnalyseApp --master yarn   --deploy-mode cluster --conf  spark.sql.shuffle.partitions=36  --files " +
        "hdfs://192.168.50.88:9000/jars/job.properties  --executor-memory 1G   --num-executors 3  --executor-cores 4  hdfs://192.168.50.88:9000/jars/kdbi-job-core-1.0-SNAPSHOT.jar ";

    System.out.println(outputString);


    write(outputPath, outputString, "UTF-8");
  }

  public static void write(String filePath, String content, String encoding)
  {
    try
    {
      File file = new File(filePath);
      file.delete();
      file.createNewFile();
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(file), encoding));
      writer.write(content);
      writer.close();
    } catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * 获取一个实例
   */
  private static App getExcel2JSONHelper()
  {
    return new App();
  }

  /**
   * 文件过滤
   *
   * @throws
   * @Title: fileNameFileter
   * @Description: TODO(这里用一句话描述这个方法的作用)
   * @param:
   * @author LiYonghui
   * @date 2017年1月6日 下午4:45:42
   * @return: void
   */
  private boolean fileNameFileter(File file)
  {
    boolean endsWith = false;
    if (file != null)
    {
      String fileName = file.getName();
      endsWith = fileName.endsWith(".xls") || fileName.endsWith(".xlsx");
    }
    return endsWith;
  }

  /**
   * 获取表头行
   *
   * @throws
   * @Title: getHeaderRow
   * @Description: TODO(这里用一句话描述这个方法的作用)
   * @param: @param sheet
   * @param: @param index
   * @param: @return
   * @author LiYonghui
   * @date 2017年1月6日 下午5:05:24
   * @return: Row
   */
  private Row getHeaderRow(Sheet sheet, int index)
  {
    Row headerRow = null;
    if (sheet != null)
    {
      headerRow = sheet.getRow(index);
    }
    return headerRow;
  }

  /**
   * 获取表格中单元格的value
   *
   * @throws
   * @Title: getCellValue
   * @Description: TODO(这里用一句话描述这个方法的作用)
   * @param: @param row
   * @param: @param cellIndex
   * @param: @param formula
   * @param: @return
   * @author LiYonghui
   * @date 2017年1月6日 下午5:40:28
   * @return: Object
   */
  private Object getCellValue(Row row, int cellIndex, FormulaEvaluator formula)
  {
    Cell cell = row.getCell(cellIndex);
    if (cell != null)
    {
      switch (cell.getCellType())
      {
        //String类型
        case Cell.CELL_TYPE_STRING:
          return cell.getRichStringCellValue()
              .getString();

        //number类型
        case Cell.CELL_TYPE_NUMERIC:
          if (DateUtil.isCellDateFormatted(cell))
          {
            return cell.getDateCellValue()
                .getTime();
          } else
          {
            return cell.getNumericCellValue();
          }
          //boolean类型
        case Cell.CELL_TYPE_BOOLEAN:
          return cell.getBooleanCellValue();
        //公式
        case Cell.CELL_TYPE_FORMULA:
          return formula.evaluate(cell)
              .getNumberValue();
        default:
          return null;
      }
    }
    return null;
  }

  /**
   * 获取表头value
   *
   * @throws
   * @Title: getHeaderCellValue
   * @Description: TODO(这里用一句话描述这个方法的作用)
   * @param: @param headerRow
   * @param: @param cellIndex 英文表头所在的行，从0开始计算哦
   * @param: @param type 表头的类型第一种 姓名（name）英文于实体类或者数据库中的变量一致
   * @param: @return
   * @author LiYonghui
   * @date 2017年1月6日 下午6:12:21
   * @return: String
   */
  private String getHeaderCellValue(Row headerRow, int cellIndex, int type)
  {
    Cell cell = headerRow.getCell(cellIndex);
    String headerValue = null;
    if (cell != null)
    {
      //第一种模板类型
      if (type == HEADER_VALUE_TYPE_Z)
      {
        headerValue = cell.getRichStringCellValue()
            .getString();
        int l_bracket = headerValue.indexOf("（");
        int r_bracket = headerValue.indexOf("）");
        if (l_bracket == -1)
        {
          l_bracket = headerValue.indexOf("(");
        }
        if (r_bracket == -1)
        {
          r_bracket = headerValue.indexOf(")");
        }
        headerValue = headerValue.substring(l_bracket + 1, r_bracket);
      } else if (type == HEADER_VALUE_TYPE_S)
      {
        //第二种模板类型
        headerValue = cell.getRichStringCellValue()
            .getString();
      }
    }
    return headerValue;
  }

  /**
   * 读取excel表格
   *
   * @throws
   * @Title: readExcle
   * @Description: TODO(这里用一句话描述这个方法的作用)
   * @param: @param file
   * @param: @param headerIndex
   * @param: @param headType 表头的类型第一种 姓名（name）英文于实体类或者数据库中的变量一致
   * @author LiYonghui
   * @date 2017年1月6日 下午6:13:27
   * @return: void
   */
  public JSONArray readExcle(File file, int headerIndex, int headType)
  {
    List<Map<String, Object>> lists = new ArrayList<Map<String, Object>>();
    List<Map<String, Object>> taskList = new ArrayList<Map<String, Object>>();
    String jobCode = "";
    String jobType = "";

    if (!fileNameFileter(file))
    {
      return null;
    } else
    {
      try
      {
        //加载excel表格
        WorkbookFactory wbFactory = new WorkbookFactory();
        Workbook wb = wbFactory.create(file);
        //读取第一个sheet页
        Sheet sheet = wb.getSheetAt(0);
        //读取表头行
        Row headerRow = getHeaderRow(sheet, headerIndex);
        //读取数据
        FormulaEvaluator formula = wb.getCreationHelper()
            .createFormulaEvaluator();
        for (int r = headerIndex + 1; r <= sheet.getLastRowNum(); r++)
        {
          Row dataRow = sheet.getRow(r);
          Map<String, Object> map = new HashMap<String, Object>();


          for (int h = 0; h < dataRow.getLastCellNum(); h++)
          {
            //表头为key
            String key = getHeaderCellValue(headerRow, h, headType);
            //数据为value
            Object value = getCellValue(dataRow, h, formula);
            if (!key.equals("") && !key.equals("null") && key != null)
            {
              if (value != null)
              {
                StringBuilder tmp = new StringBuilder(value.toString());

                if (tmp.charAt(0) == '"')
                {
                  tmp.deleteCharAt(0);
                }
                if (tmp.charAt(tmp.length() - 1) == '"')
                {
                  tmp.deleteCharAt(tmp.length() - 1);
                }


                value = tmp.toString();
              } else if (value == null)
              {
                value = "";
              }
              //除去换行
          /*    value = value.toString()
                  .replaceAll("\\n", " ");*/
              if (key.equals("jobCode"))
              {
                jobCode = value.toString();

              } else if (key.equals("jobType"))
              {
                jobType = value.toString();

              } else
              {
                if (key.equals("inputParams") || key.equals("extraParams"))
                {
                  List<Map<String, Object>> inputParaLlists = new ArrayList<Map<String, Object>>();
                  if (value != null && !value.toString()
                      .trim()
                      .equals(""))
                  {
                    String[] items = value.toString()
                        .split("\\|");
                    for (String item : items)
                    {
                      String[] args = item.split(",");


                      for (String arg : args)
                      {
                        Map<String, Object> inputParamsMap = new HashMap<String, Object>();
                        String[] param = arg.split("=");

                        if (param.length == 2)
                        {
                          String k = param[0].trim();
                          String v = param[1].trim();
                          inputParamsMap.put("key", k);

                          inputParamsMap.put("value", v);

                        }
                        inputParaLlists.add(inputParamsMap);
                      }

                    }
                  } else
                  {
                    Map<String, Object> inputParamsMap = new HashMap<String, Object>();
                    inputParamsMap.put("key", "");
                    inputParamsMap.put("value", "");
                    inputParaLlists.add(inputParamsMap);
                  }
                  value = inputParaLlists;

                }else if (key.equals("prioritySn")||key.equals("handleMode") || key.equals("phase"))
                {
                  value = Float.valueOf(value.toString())
                      .longValue();
                }

                map.put(key, value);
              }
            }
          }
          // 读取每一行数据完
          taskList.add(map);
        }


        Map<String, Object> taskMap = new HashMap<String, Object>();

        taskMap.put("jobCode", jobCode);
        taskMap.put("jobType", jobType);
        taskMap.put("jobInfo", taskList);

        lists.add(taskMap);

      } catch (Exception e)
      {
        e.printStackTrace();
      }
    }
    JSONArray jsonArray = JSONArray.fromObject(lists);
    return jsonArray;
  }
}