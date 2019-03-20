import com.fasterxml.jackson.databind.ObjectMapper;

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
public class Test1111
{
 public static void main(String[] args) throws Exception{
   String json = "{\n" +
       "  \"studentName\":\"zhangsan\",\n" +
       "  \"studentAge\":12\n" +
       "}";
   Test1111.fromJson();
 }

  private static JsonBinder binder = JsonBinder.buildNonDefaultBinder();
  public void toJson() throws Exception {

  }

  /**
   * 从Json字符串反序列化对象/集合.
   */

  @SuppressWarnings("unchecked")
  public static void fromJson() throws Exception {

    //Map
    String mapString = "{\n" +
        "  \"teacher.name\":\"t1\",\n" +
        "  \"teacher.age\":1,\n" +
        "  \"course.info\":{\n" +
        "    \"course.name\":\"math\",\n" +
        "    \"course.code\":1\n" +
        "  },\n" +
        "  \"students\":[\n" +
        "    {\n" +
        "      \"student.name\":\"zansan\",\n" +
        "      \"student.age\":23,\n" +
        "      \"enum\":1\n" +
        "    },{\n" +
        "      \"student.name\":\"lishi1\",\n" +
        "      \"student.age\":25,\n" +
        "      \"enum\":\"V3\"\n" +
        "    },{\n" +
        "      \"student.name\":\"lishi3\",\n" +
        "      \"student.age\":25,\n" +
        "      \"color\":\"RED\"\n" +
        "      \n" +
        "    },{\n" +
        "      \"student.name\":\"lishi2\",\n" +
        "      \"student.age\":25,\n" +
        "      \"enum\":\"V3\"\n" +
        "    }\n" +
        "  ]\n" +
        "}";

 /*   Map<String, Object> map = binder.fromJson(mapString, HashMap.class);
    System.out.println("Map:");*/
  /*  for (Map.Entry<String, Object> entry : map.entrySet()) {
      System.out.println(entry.getKey() + " " + entry.getValue());
    }*/
    Teacher s = binder.fromJson(mapString,Teacher.class);
  System.out.println(s);

    String node = binder.readNode(mapString,"teacher.name");
    System.out.println(node);

  }
  /**
   * json deserialize
   * @param json
   * @param mapClazz
   * @return
   */
  public static Object jsonDeserialize(final String json, final Class<?> mapClazz) {
    ObjectMapper om = new ObjectMapper();
    try {
      return om.readValue(json, mapClazz);
    } catch (Exception ex) {
      return null;
    }
  }
  /**
   * json字符串-简单对象与JavaBean_obj之间的转换
   */
/*  public static void testJSONStrToJavaBeanObj(){

    Student student = JSON.parseObject(JSON_OBJ_STR, new TypeReference<Student>() {});
    //Student student1 = JSONObject.parseObject(JSON_OBJ_STR, new TypeReference<Student>() {});//因为JSONObject继承了JSON，所以这样也是可以的

    System.out.println(student.getStudentName()+":"+student.getStudentAge());

  }*/

/*  *//**
   * 复杂json格式字符串与JavaBean_obj之间的转换
   *//*
  public static void testComplexJSONStrToJavaBean(){

    Teacher teacher = JSON.parseObject(COMPLEX_JSON_STR, new TypeReference<Teacher>() {});
    //Teacher teacher1 = JSON.parseObject(COMPLEX_JSON_STR, new TypeReference<Teacher>() {});//因为JSONObject继承了JSON，所以这样也是可以的
    String teacherName = teacher.getTeacherName();
    Integer teacherAge = teacher.getTeacherAge();
    Course course = teacher.getCourse();
    List<Student> students = teacher.getStudents();
  }*/
}
