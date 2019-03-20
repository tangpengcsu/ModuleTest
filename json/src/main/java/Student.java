import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;
import com.fasterxml.jackson.annotation.JsonProperty;

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
enum MyEnum {
  V1, V2, V3, @JsonEnumDefaultValue DEFAULT;
}

  enum ColorEnum {
  RED("red","红色"),GREEN("green","绿色"),BLUE("blue","蓝色");
  //防止字段值被修改，增加的字段也统一final表示常量
  private final String key;
  private final String value;

  private ColorEnum(String key,String value){
    this.key = key;
    this.value = value;
  }
  //根据key获取枚举
  public static ColorEnum getEnumByKey(String key){
    if(null == key){
      return null;
    }
    for(ColorEnum temp:ColorEnum.values()){
      if(temp.getKey().equals(key)){
        return temp;
      }
    }
    return null;
  }
  public String getKey() {
    return key;
  }
  public String getValue() {
    return value;
  }
}

public class Student {
  @JsonProperty("color")
  private ColorEnum color;
  @JsonProperty("enum")
  public MyEnum myEnum;
  @JsonProperty("student.name")
  private String studentName;
  @JsonProperty("student.age")
  private Integer studentAge;

  public String getStudentName() {
    return studentName;
  }

  public void setStudentName(String studentName) {
    this.studentName = studentName;
  }

  public Integer getStudentAge() {
    return studentAge;
  }

  public void setStudentAge(Integer studentAge) {
    this.studentAge = studentAge;
  }

  public ColorEnum getColor()
  {
    return color;
  }

  public void setColor(ColorEnum color)
  {
    this.color = color;
  }

  public MyEnum getMyEnum()
  {
    return myEnum;
  }

  public void setMyEnum(MyEnum myEnum)
  {
    this.myEnum = myEnum;
  }

  @Override
  public String toString()
  {
    return "Student{" +
        "color=" + color +
        ", myEnum=" + myEnum +
        ", studentName='" + studentName + '\'' +
        ", studentAge=" + studentAge +
        '}';
  }
}
