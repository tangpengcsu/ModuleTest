import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

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
public class Teacher {

  @JsonProperty("teacher.name")
  private String teacherName;
  @JsonProperty("teacher.age")
  private Integer teacherAge;
  @JsonProperty("course.info")
  private Course course;
  @JsonProperty("students")
  private List<Student> students;

  public String getTeacherName() {
    return teacherName;
  }

  public void setTeacherName(String teacherName) {
    this.teacherName = teacherName;
  }

  public Integer getTeacherAge() {
    return teacherAge;
  }

  public void setTeacherAge(Integer teacherAge) {
    this.teacherAge = teacherAge;
  }

  public Course getCourse() {
    return course;
  }

  public void setCourse(Course course) {
    this.course = course;
  }

  public List<Student> getStudents() {
    return students;
  }

  public void setStudents(List<Student> students) {
    this.students = students;
  }

  @Override
  public String toString()
  {
    return "Teacher{" +
        "teacherName='" + teacherName + '\'' +
        ", teacherAge=" + teacherAge +
        ", course=" + course +
        ", students=" + students +
        '}';
  }
}
