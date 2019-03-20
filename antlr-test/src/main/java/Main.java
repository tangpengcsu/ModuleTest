/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2018-08-24
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-08-24     1.0.1.0    tang.peng        创建
 * ----------------------------------------------------------------
 */

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

public class Main {

  public static void run(String expr) throws Exception{

    //对每一个输入的字符串，构造一个 ANTLRStringStream 流 in
    ANTLRInputStream in = new ANTLRInputStream(expr);

    //用 in 构造词法分析器 lexer，词法分析的作用是产生记号
    DemoLexer lexer = new DemoLexer(in);

    //用词法分析器 lexer 构造一个记号流 tokens
    CommonTokenStream tokens = new CommonTokenStream(lexer);

    //再使用 tokens 构造语法分析器 parser,至此已经完成词法分析和语法分析的准备工作
    DemoParser parser = new DemoParser(tokens);
    DemoParser.ExprContext tree = parser.expr();
    DemoBaseVisitor mv =  new DemoBaseVisitor();
    mv.visit(tree);
    //最终调用语法分析器的规则 prog，完成对表达式的验证
    DemoParser.ProgContext p = parser.prog();
    p.stat();
  }

  public static void main(String[] args) throws Exception{

    String[] testStr={
        "2",
        "a+b+3",
        "(a-b)+3",
        "a+(b*3"
    };
    run(testStr[2]);
   /* for (String s:testStr){
      System.out.println("Input expr:"+s);
      run(s);
    }*/
  }
}
