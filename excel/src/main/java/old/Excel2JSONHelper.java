package old; /**
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * excel表格转成json
 * @ClassName: old.App
 * @Description:TODO(这里用一句话描述这个类的作用)
 * @author LiYonghui
 * @date 2017年1月6日 下午4:42:43
 */
public class Excel2JSONHelper {
  //常亮，用作第一种模板类型，如下图
  private static final int HEADER_VALUE_TYPE_Z=1;
  //第二种模板类型，如下图
  private static final int HEADER_VALUE_TYPE_S=2;
  public static void main(String[] args) {
    String path = args[0];
    String outputPath = args[1];
    System.out.println(path);
   // path = "E:\\Users\\Documents\\大数据分析平台\\test.xlsx"
    File dir = new File(path);
    Excel2JSONHelper excelHelper = getExcel2JSONHelper();
    //dir文件，0代表是第一行为保存到数据库或者实体类的表头，一般为英文的字符串，2代表是第二种模板，
    JSONArray jsonArray = excelHelper.readExcle(dir, 0, 2);
    StringBuilder json = new StringBuilder("\"{\\\"taskInfo\\\":");
    json.append(jsonArray.toString().replaceAll("\\\\n"," ").replaceAll("\"","\\\\\"")).append("}\"");

    System.out.println(json);

    write(outputPath,json.toString(),"UTF-8");
  }
  public static void write(String filePath,String content,String encoding){
    try {
      File file = new File(filePath);
      file.delete();
      file.createNewFile();
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(file), encoding));
      writer.write(content);
      writer.close();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   *
   * 获取一个实例
   */
  private static Excel2JSONHelper getExcel2JSONHelper(){
    return new Excel2JSONHelper();
  }

  /**
   * 文件过滤
   * @Title: fileNameFileter
   * @Description: TODO(这里用一句话描述这个方法的作用)
   * @param:
   * @author LiYonghui
   * @date 2017年1月6日 下午4:45:42
   * @return: void
   * @throws
   */
  private boolean fileNameFileter(File file){
    boolean endsWith = false;
    if(file != null){
      String fileName = file.getName();
      endsWith = fileName.endsWith(".xls") || fileName.endsWith(".xlsx");
    }
    return endsWith;
  }

  /**
   * 获取表头行
   * @Title: getHeaderRow
   * @Description: TODO(这里用一句话描述这个方法的作用)
   * @param: @param sheet
   * @param: @param index
   * @param: @return
   * @author LiYonghui
   * @date 2017年1月6日 下午5:05:24
   * @return: Row
   * @throws
   */
  private Row getHeaderRow(Sheet sheet, int index){
    Row headerRow = null;
    if(sheet!=null){
      headerRow = sheet.getRow(index);
    }
    return headerRow;
  }

  /**
   * 获取表格中单元格的value
   * @Title: getCellValue
   * @Description: TODO(这里用一句话描述这个方法的作用)
   * @param: @param row
   * @param: @param cellIndex
   * @param: @param formula
   * @param: @return
   * @author LiYonghui
   * @date 2017年1月6日 下午5:40:28
   * @return: Object
   * @throws
   */
  private Object getCellValue(Row row,int cellIndex,FormulaEvaluator formula){
    Cell cell = row.getCell(cellIndex);
    if(cell != null){
      switch (cell.getCellType()) {
        //String类型
        case Cell.CELL_TYPE_STRING:
          return cell.getRichStringCellValue().getString();

        //number类型
        case Cell.CELL_TYPE_NUMERIC:
          if (DateUtil.isCellDateFormatted(cell)) {
            return cell.getDateCellValue().getTime();
          } else {
            return cell.getNumericCellValue();
          }
          //boolean类型
        case Cell.CELL_TYPE_BOOLEAN:
          return cell.getBooleanCellValue();
        //公式
        case Cell.CELL_TYPE_FORMULA:
          return formula.evaluate(cell).getNumberValue();
        default:
          return null;
      }
    }
    return null;
  }

  /**
   * 获取表头value
   * @Title: getHeaderCellValue
   * @Description: TODO(这里用一句话描述这个方法的作用)
   * @param: @param headerRow
   * @param: @param cellIndex 英文表头所在的行，从0开始计算哦
   * @param: @param type 表头的类型第一种 姓名（name）英文于实体类或者数据库中的变量一致
   * @param: @return
   * @author LiYonghui
   * @date 2017年1月6日 下午6:12:21
   * @return: String
   * @throws
   */
  private String getHeaderCellValue(Row headerRow,int cellIndex,int type){
    Cell cell = headerRow.getCell(cellIndex);
    String headerValue = null;
    if(cell != null){
      //第一种模板类型
      if(type == HEADER_VALUE_TYPE_Z){
        headerValue = cell.getRichStringCellValue().getString();
        int l_bracket = headerValue.indexOf("（");
        int r_bracket = headerValue.indexOf("）");
        if(l_bracket == -1){
          l_bracket = headerValue.indexOf("(");
        }
        if(r_bracket == -1){
          r_bracket = headerValue.indexOf(")");
        }
        headerValue = headerValue.substring(l_bracket+1, r_bracket);
      }else if(type == HEADER_VALUE_TYPE_S){
        //第二种模板类型
        headerValue = cell.getRichStringCellValue().getString();
      }
    }
    return headerValue;
  }

  /**
   * 读取excel表格
   * @Title: readExcle
   * @Description: TODO(这里用一句话描述这个方法的作用)
   * @param: @param file
   * @param: @param headerIndex
   * @param: @param headType 表头的类型第一种 姓名（name）英文于实体类或者数据库中的变量一致
   * @author LiYonghui
   * @date 2017年1月6日 下午6:13:27
   * @return: void
   * @throws
   */
  public JSONArray readExcle(File file,int headerIndex,int headType){
    List<Map<String, Object>> lists = new ArrayList<Map<String, Object>>();
    if(!fileNameFileter(file)){
      return null;
    }else{
      try {
        //加载excel表格
        WorkbookFactory wbFactory = new WorkbookFactory();
        Workbook wb = wbFactory.create(file);
        //读取第一个sheet页
        Sheet sheet = wb.getSheetAt(0);
        //读取表头行
        Row headerRow = getHeaderRow(sheet, headerIndex);
        //读取数据
        FormulaEvaluator formula = wb.getCreationHelper().createFormulaEvaluator();
        for(int r = headerIndex+1; r<= sheet.getLastRowNum();r++){
          Row dataRow = sheet.getRow(r);
          Map<String, Object> map = new HashMap<String, Object>();
          for(int h = 0; h<dataRow.getLastCellNum();h++){
            //表头为key
            String key = getHeaderCellValue(headerRow,h,headType);
            //数据为value
            Object value = getCellValue(dataRow, h, formula);
            if(!key.equals("") && !key.equals("null") && key != null ){
              if(key.equals("inputParams") || key.equals("extraParams")){
                List<Map<String, Object>> inputParaLlists = new ArrayList<Map<String, Object>>();
                if(value!=null && !value.toString().trim().equals("")){
                  String[] items =value.toString().split("\\|");
                  for (String item : items)
                  {
                    String[] args= item.split(",");


                    for (String arg : args)
                    {
                      Map<String, Object> inputParamsMap = new HashMap<String, Object>();
                     String[] param =  arg.split("=");

                      if(param.length==2){
                        String k = param[0].trim();
                        if(k.startsWith("\"")){
                          k = k.substring(1);

                        }
                        String v =param[1].trim();
                        if(v.endsWith("\"")){
                          v = v.substring(0,v.length()-2);
                        }
                        if(key.equals("extraParams")){
                          inputParamsMap.put("key", k);
                        }else{
                          inputParamsMap.put("name", k);
                        }

                        inputParamsMap.put("value",v);

                      }
                      inputParaLlists.add(inputParamsMap);
                    }

                  }
                }else{
                  Map<String, Object> inputParamsMap = new HashMap<String, Object>();
                  inputParamsMap.put("name","");
                  inputParamsMap.put("value","");
                  inputParaLlists.add(inputParamsMap);
                }
                value = inputParaLlists;
              }else if(key.equals("jobSn")|| key.equals("prioritySn")){
                value = Float.valueOf(value.toString()).intValue();
              }else if(key.equals("handleMode")){
                value = String.valueOf(Float.valueOf(value.toString()).intValue());
              }else if(value!=null){
                StringBuilder tmp = new StringBuilder(value.toString());

                if(tmp.charAt(0)=='"'){
                  tmp.deleteCharAt(0);
                }
                if(tmp.charAt(tmp.length()-1)=='"'){
                  tmp.deleteCharAt(tmp.length()-1);
                }


                value = tmp.toString();
              }else if(value ==null){
                value = "";
              }


              map.put(key, value);
            }
          }
          lists.add(map);
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    JSONArray jsonArray = JSONArray.fromObject(lists);
    return jsonArray;
  }
}