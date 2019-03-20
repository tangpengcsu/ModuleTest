package com.szkingdom.text;

import java.sql.Timestamp;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2019/3/20
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2019/3/20     1.0.1.0       tang.peng     创建
 * ----------------------------------------------------------------
 */
public class HbaseTableStructInfo {
    private String tableCode;
    private Integer fieldSn;
    private String fieldCode;
    private String fieldName;
    private String fieldDesc;
    private String dataType;
    private Integer fieldLength;
    private Integer decimalDigits;
    private String defaultValue;
    private Character regionFlag;
    private Integer regionFieldSn;
    private Timestamp updateTime;


    public String getTableCode() {
        return tableCode;
    }

    public void setTableCode(String tableCode) {
        this.tableCode = tableCode;
    }

    public Integer getFieldSn() {
        return fieldSn;
    }

    public void setFieldSn(Integer fieldSn) {
        this.fieldSn = fieldSn;
    }

    public String getFieldCode() {
        return fieldCode;
    }

    public void setFieldCode(String fieldCode) {
        this.fieldCode = fieldCode;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldDesc() {
        return fieldDesc;
    }

    public void setFieldDesc(String fieldDesc) {
        this.fieldDesc = fieldDesc;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Integer getFieldLength() {
        return fieldLength;
    }

    public void setFieldLength(Integer fieldLength) {
        this.fieldLength = fieldLength;
    }

    public Integer getDecimalDigits() {
        return decimalDigits;
    }

    public void setDecimalDigits(Integer decimalDigits) {
        this.decimalDigits = decimalDigits;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public Character getRegionFlag() {
        return regionFlag;
    }

    public void setRegionFlag(Character regionFlag) {
        this.regionFlag = regionFlag;
    }

    public Integer getRegionFieldSn() {
        return regionFieldSn;
    }

    public void setRegionFieldSn(Integer regionFieldSn) {
        this.regionFieldSn = regionFieldSn;
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Timestamp updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return "HbaseTableStructInfo{" +
                "tableCode='" + tableCode + '\'' +
                ", fieldSn=" + fieldSn +
                ", fieldCode='" + fieldCode + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", fieldDesc='" + fieldDesc + '\'' +
                ", dataType='" + dataType + '\'' +
                ", fieldLength=" + fieldLength +
                ", decimalDigits=" + decimalDigits +
                ", defaultValue='" + defaultValue + '\'' +
                ", regionFlag=" + regionFlag +
                ", regionFieldSn=" + regionFieldSn +
                ", updateTime=" + updateTime +
                '}';
    }
}
