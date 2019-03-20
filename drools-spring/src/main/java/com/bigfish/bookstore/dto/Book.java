package com.bigfish.bookstore.dto;

import java.io.Serializable;

/**
 * 
 */
@SuppressWarnings("serial")
public class Book implements Serializable {
	private String name;

	// 书的分类
	private String clz;

	// 销售区域
	private String salesArea;

	// 出版日期距今时间（单位：Y）
	private int years;

	// 报价
	private double basePrice;

	// 对外售价
	private double salesPrice;

	public Book() {
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getClz() {
		return clz;
	}

	public void setClz(String clz) {
		this.clz = clz;
	}

	public String getSalesArea() {
		return salesArea;
	}

	public void setSalesArea(String salesArea) {
		this.salesArea = salesArea;
	}

	public int getYears() {
		return years;
	}

	public void setYears(int years) {
		this.years = years;
	}

	public double getBasePrice() {
		return basePrice;
	}

	public void setBasePrice(double basePrice) {
		this.basePrice = basePrice;
	}

	public double getSalesPrice() {
		return salesPrice;
	}

	public void setSalesPrice(double salesPrice) {
		this.salesPrice = salesPrice;
	}

}