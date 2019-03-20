/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2018-05-24
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-05-24     1.0.1.0    tang.peng        创建
 * ----------------------------------------------------------------
 */
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FileStreamSourceConnectorTest extends EasyMockSupport {

  private static final String SINGLE_TOPIC = "test";
  private static final String MULTIPLE_TOPICS = "test1,test2";
  private static final String FILENAME = "tmp.txt";

  private FileStreamSourceConnector connector;
  private ConnectorContext ctx;
  private Map<String, String> sourceProperties;

  @Before
  public void setup() {
    connector = new FileStreamSourceConnector();
    ctx = createMock(ConnectorContext.class);
    connector.initialize(ctx);

    sourceProperties = new HashMap<>();
    sourceProperties.put(FileStreamSourceConnector.TOPIC_CONFIG, SINGLE_TOPIC);
    sourceProperties.put(FileStreamSourceConnector.FILE_CONFIG, FILENAME);
  }

  @Test
  public void testConnectorConfigValidation() {
    replayAll();
    List<ConfigValue> configValues = connector.config().validate(sourceProperties);
    for (ConfigValue val : configValues) {
      assertEquals("Config property errors: " + val.errorMessages(), 0, val.errorMessages().size());
    }
    verifyAll();
  }

  @Test
  public void testSourceTasks() {
    replayAll();

    connector.start(sourceProperties);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
    assertEquals(1, taskConfigs.size());
    assertEquals(FILENAME,
        taskConfigs.get(0).get(FileStreamSourceConnector.FILE_CONFIG));
    assertEquals(SINGLE_TOPIC,
        taskConfigs.get(0).get(FileStreamSourceConnector.TOPIC_CONFIG));

    // Should be able to return fewer than requested #
    taskConfigs = connector.taskConfigs(2);
    assertEquals(1, taskConfigs.size());
    assertEquals(FILENAME,
        taskConfigs.get(0).get(FileStreamSourceConnector.FILE_CONFIG));
    assertEquals(SINGLE_TOPIC,
        taskConfigs.get(0).get(FileStreamSourceConnector.TOPIC_CONFIG));

    verifyAll();
  }

  @Test
  public void testSourceTasksStdin() {
    EasyMock.replay(ctx);

    sourceProperties.remove(FileStreamSourceConnector.FILE_CONFIG);
    connector.start(sourceProperties);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
    assertEquals(1, taskConfigs.size());
    assertNull(taskConfigs.get(0).get(FileStreamSourceConnector.FILE_CONFIG));

    EasyMock.verify(ctx);
  }

  @Test(expected = ConfigException.class)
  public void testMultipleSourcesInvalid() {
    sourceProperties.put(FileStreamSourceConnector.TOPIC_CONFIG, MULTIPLE_TOPICS);
    connector.start(sourceProperties);
  }

  @Test
  public void testTaskClass() {
    EasyMock.replay(ctx);

    connector.start(sourceProperties);
    assertEquals(FileStreamSourceTask.class, connector.taskClass());

    EasyMock.verify(ctx);
  }

  @Test(expected = ConfigException.class)
  public void testMissingTopic() {
    sourceProperties.remove(FileStreamSourceConnector.TOPIC_CONFIG);
    connector.start(sourceProperties);
  }

  @Test(expected = ConfigException.class)
  public void testBlankTopic() {
    // Because of trimming this tests is same as testing for empty string.
    sourceProperties.put(FileStreamSourceConnector.TOPIC_CONFIG, "     ");
    connector.start(sourceProperties);
  }

  @Test(expected = ConfigException.class)
  public void testInvalidBatchSize() {
    sourceProperties.put(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG, "abcd");
    connector.start(sourceProperties);
  }
}