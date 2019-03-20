<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Drools Result</title>
</head>
<body>
    <hr>
    <ul>you have order one book, the book detail as below: 
      <li>book name:${book.name}</li>
      <li>book class:${book.clz}</li>
      <li>sales Area:${book.salesArea}</li>
      <li>years:${book.years}</li>
      <li>base Price:${book.basePrice}</li>
      
    </ul>
    <hr>
    <h1>sale price:${book.salesPrice}</h1>	
</body>
</html>