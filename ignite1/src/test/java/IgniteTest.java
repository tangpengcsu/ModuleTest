import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import ignite.MyCounterService;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.dialect.MySQLDialect;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.ServiceResource;
import org.junit.Test;


/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2019/4/2
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2019/4/2     1.0.1.0       tang.peng     创建
 * ----------------------------------------------------------------
 */
public class IgniteTest {
    @Test
    public void test(){

        MysqlDataSource mysqlDataSource  = new MysqlDataSource();
        mysqlDataSource.setURL("jdbc:mysql://[host]:[port]/[database]");
        mysqlDataSource.setUser("");
        mysqlDataSource.setPassword("");

        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName("PersonCache");
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);
        cacheConfiguration.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        JdbcType jdbcType = new JdbcType();
        jdbcType.setCacheName("PersonCache");
        jdbcType.setKeyType(Integer.class);
        //jdbcType.setValueType();
        jdbcType.setDatabaseSchema("d1");
        jdbcType.setDatabaseTable("t1");

        CacheJdbcPojoStoreFactory cacheJdbcPojoStoreFactory = new CacheJdbcPojoStoreFactory();
        cacheJdbcPojoStoreFactory.setDataSource(mysqlDataSource);
        cacheJdbcPojoStoreFactory.setDialect(new MySQLDialect());
        cacheJdbcPojoStoreFactory.setTypes(jdbcType);

        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        igniteConfiguration.setCacheConfiguration(cacheConfiguration);


        cacheConfiguration.setCacheStoreFactory(cacheJdbcPojoStoreFactory);
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start(igniteConfiguration)) {
            // Load data from person table into PersonCache.
            ignite.cache("PersonCache").loadCache(null);

            // Populate other caches

        }

    }
    @Test
    public void test1() throws Exception{
        Ignite ignite = Ignition.ignite();
        IgniteCompute compute = ignite.compute(ignite.cluster().forAttribute("service.node", true));
//        IgniteCompute compute = ignite.compute(); //未部署服务的节点会抛出空指针
        compute.run(new IgniteRunnable() {
            @ServiceResource(serviceName = "myCounterService", proxySticky = false) //非粘性代理
            private MyCounterService counterService;

            @Override
            public void run() {
                int newValue = counterService.increment();
                System.out.println("Incremented value : " + newValue);
            }
        });
    }
}
