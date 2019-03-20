package com.bigfish.bookstore.service;

import org.kie.api.cdi.KSession;
import org.kie.api.runtime.KieSession;
import org.springframework.stereotype.Service;

import com.bigfish.bookstore.dto.Book;

@Service
public class BookService {

	@KSession("bookprice_ksession")
	private KieSession kSession;

	/**
	 * 获取一本书的当前实际售价
	 * 
	 * @param b
	 * @return
	 */
	public double getBookSalePrice(Book b) {
		if (b == null) {
			throw new NullPointerException("Book can not be null.");
		}
		
		kSession.insert(b);
		kSession.fireAllRules();

		return b.getSalesPrice();
	}
}
