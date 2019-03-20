package com.bigfish.bookstore.web;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.bigfish.bookstore.dto.Book;
import com.bigfish.bookstore.service.BookService;

@Controller
@RequestMapping("/book")
public class BookController {

	@Autowired
	private BookService bookService;

	/**
	 * 购买一本书
	 * 
	 * @return
	 */
	@RequestMapping(value = "/order")
	public String orderOneBook(HttpServletRequest request) {
        Book b = new Book();
        b.setBasePrice(120.50);
        b.setClz("computer");
        b.setName("C plus programing");
        b.setSalesArea("China");
        b.setYears(2);
        
        double realPrice = bookService.getBookSalePrice(b);
        request.setAttribute("book", b);
        System.out.println(b.getName() + ":" + realPrice);
        
		return "book/order_detail";
	}
}
