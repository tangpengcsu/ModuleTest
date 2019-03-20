import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2018-12-07
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-12-07     1.0.1.0    tang.peng        创建
 * ----------------------------------------------------------------
 */
public class JsonBinder {

  private static Logger logger = LoggerFactory.getLogger(JsonBinder.class);

  private ObjectMapper mapper;

  public JsonBinder(JsonInclude.Include inclusion) {
    mapper = new ObjectMapper();
    //设置输出包含的属性

    mapper.setDefaultPropertyInclusion(inclusion);
    //设置输入时忽略JSON字符串中存在而Java对象实际没有的属性
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  }

  public String  readNode(String json,String fieldName){
    try
    {
      JsonNode tree =mapper.readTree(json);
      JsonNode filedNode = tree.get(fieldName);
      return filedNode.asText();
    } catch (IOException e)
    {
      return null;
    }

  }

  /**
   * 创建输出全部属性到Json字符串的Binder.
   */
  public static JsonBinder buildNormalBinder() {
    return new JsonBinder(JsonInclude.Include.ALWAYS);
  }

  /**
   * 创建只输出非空属性到Json字符串的Binder.
   */
  public static JsonBinder buildNonNullBinder() {
    return new JsonBinder(JsonInclude.Include.NON_NULL);
  }

  /**
   * 创建只输出初始值被改变的属性到Json字符串的Binder.
   */
  public static JsonBinder buildNonDefaultBinder() {
    return new JsonBinder(JsonInclude.Include.NON_DEFAULT);
  }

  /**
   * 如果JSON字符串为Null或"null"字符串,返回Null.
   * 如果JSON字符串为"[]",返回空集合.
   *
   * 如需读取集合如List/Map,且不是List<String>这种简单类型时使用如下语句:
   * List<MyBean> beanList = binder.getMapper().readValue(listString, new TypeReference<List<MyBean>>() {});
   */
  public <T> T fromJson(String jsonString, Class<T> clazz) {
    if (jsonString==null || jsonString.isEmpty()) {
      return null;
    }

    try {
      return mapper.readValue(jsonString, clazz);
    } catch (IOException e) {
      System.out.println("parse json string error:" + jsonString+ e);
      return null;
    }
  }

  /**
   * 如果对象为Null,返回"null".
   * 如果集合为空集合,返回"[]".
   */
  public String toJson(Object object) {

    try {
      return mapper.writeValueAsString(object);
    } catch (IOException e) {
      logger.warn("write to json string error:" + object, e);
      return null;
    }
  }

  /**
   * 设置转换日期类型的format pattern,如果不设置默认打印Timestamp毫秒数.
   */
  public void setDateFormat(String pattern) {
    if (pattern!=null && !pattern.isEmpty()) {
      DateFormat df = new SimpleDateFormat(pattern);
      mapper.getSerializationConfig().with(df);
      mapper.getDeserializationConfig().with(df);
    }
  }

  /**
   * 取出Mapper做进一步的设置或使用其他序列化API.
   */
  public ObjectMapper getMapper() {
    return mapper;
  }
}


