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
public class Course {
  @JsonProperty("course.name")
  private String courseName;
  @JsonProperty("course.code")
  private Integer code;

  public String getCourseName() {
    return courseName;
  }

  public void setCourseName(String courseName) {
    this.courseName = courseName;
  }

  public Integer getCode() {
    return code;
  }

  public void setCode(Integer code) {
    this.code = code;
  }

  @Override
  public String toString()
  {
    return "Course{" +
        "courseName='" + courseName + '\'' +
        ", code=" + code +
        '}';
  }
}
