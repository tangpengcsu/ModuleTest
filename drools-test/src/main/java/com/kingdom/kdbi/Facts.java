package com.kingdom.kdbi;

import org.apache.spark.sql.Row;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2018-11-13
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-11-13     1.0.1.0    tang.peng        创建
 * ----------------------------------------------------------------
 */
public class Facts implements Serializable
{


  Map<String,Object> item;

  String tagValue;



  public Facts()
  {
  }

  public Integer getInteger(String key){

    return (Integer)item.get(key);
  }

  public String getString(String key){
    return (String)item.get(key);
  }

  public BigDecimal getDecimal(String key){
    return (BigDecimal)item.get(key);
  }

  public List<Row> getListRow(String key){
    return (List<Row>)item.get(key);
  }


  public String getPartitionOffset() {
    return (String)item.get("partitionOffset");
  }
  public String getCustId()
  {
    return (String)item.get("CUST_ID");
  }

  public String getTagValue()
  {
    return tagValue;
  }

  public void setTagValue(String tagValue)
  {
    this.tagValue = tagValue;
  }

  public Map<String, Object> getItem()
  {
    return item;
  }

  public void setItem(Map<String, Object> item)
  {
    this.item = item;
  }

  public static void main(String[] args)
  {
    //从工厂中获得KieServices实例
    KieServices kieServices = KieServices.Factory.get();

    //从KieServices中获得KieContainer实例，其会加载kmodule.xml文件并load规则文件
    KieContainer kieContainer = kieServices.getKieClasspathContainer();

    //建立KieSession到规则文件的通信管道
    KieSession kSession = kieContainer.newKieSession("ksession-rules");

    Facts facts = new Facts();
    Map<String,Object> item = new HashMap();
    List<Row> lst = new ArrayList();
    Row row =   null;
    lst.add(row);
    List<Row> itemLst = new ArrayList();
    itemLst.add(row);
    item.put("MODEL_FACTOR_LIST",lst);
    item.put("MODEL_FACTOR_ITEM_LIST",itemLst);
    facts.setItem(item);
    //将实体类插入执行规则
    FactHandle fh = kSession.insert(facts);

    int num =kSession.fireAllRules();

  }
}
