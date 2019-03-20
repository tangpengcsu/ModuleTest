// Generated from D:/workspace/IdeaProjects/ModuleTest/antlr-test/src/main/resources\FunctionExprParser.g4 by ANTLR 4.7
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link FunctionExprParserParser}.
 */
public interface FunctionExprParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link FunctionExprParserParser#exp}.
	 * @param ctx the parse tree
	 */
	void enterExp(FunctionExprParserParser.ExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link FunctionExprParserParser#exp}.
	 * @param ctx the parse tree
	 */
	void exitExp(FunctionExprParserParser.ExpContext ctx);
	/**
	 * Enter a parse tree produced by {@link FunctionExprParserParser#term}.
	 * @param ctx the parse tree
	 */
	void enterTerm(FunctionExprParserParser.TermContext ctx);
	/**
	 * Exit a parse tree produced by {@link FunctionExprParserParser#term}.
	 * @param ctx the parse tree
	 */
	void exitTerm(FunctionExprParserParser.TermContext ctx);
	/**
	 * Enter a parse tree produced by the {@code columnName}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 */
	void enterColumnName(FunctionExprParserParser.ColumnNameContext ctx);
	/**
	 * Exit a parse tree produced by the {@code columnName}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 */
	void exitColumnName(FunctionExprParserParser.ColumnNameContext ctx);
	/**
	 * Enter a parse tree produced by the {@code char}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 */
	void enterChar(FunctionExprParserParser.CharContext ctx);
	/**
	 * Exit a parse tree produced by the {@code char}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 */
	void exitChar(FunctionExprParserParser.CharContext ctx);
	/**
	 * Enter a parse tree produced by the {@code number}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 */
	void enterNumber(FunctionExprParserParser.NumberContext ctx);
	/**
	 * Exit a parse tree produced by the {@code number}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 */
	void exitNumber(FunctionExprParserParser.NumberContext ctx);
	/**
	 * Enter a parse tree produced by the {@code ngNumber}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 */
	void enterNgNumber(FunctionExprParserParser.NgNumberContext ctx);
	/**
	 * Exit a parse tree produced by the {@code ngNumber}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 */
	void exitNgNumber(FunctionExprParserParser.NgNumberContext ctx);
	/**
	 * Enter a parse tree produced by the {@code func}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 */
	void enterFunc(FunctionExprParserParser.FuncContext ctx);
	/**
	 * Exit a parse tree produced by the {@code func}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 */
	void exitFunc(FunctionExprParserParser.FuncContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenthesis}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 */
	void enterParenthesis(FunctionExprParserParser.ParenthesisContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenthesis}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 */
	void exitParenthesis(FunctionExprParserParser.ParenthesisContext ctx);
	/**
	 * Enter a parse tree produced by {@link FunctionExprParserParser#funCall}.
	 * @param ctx the parse tree
	 */
	void enterFunCall(FunctionExprParserParser.FunCallContext ctx);
	/**
	 * Exit a parse tree produced by {@link FunctionExprParserParser#funCall}.
	 * @param ctx the parse tree
	 */
	void exitFunCall(FunctionExprParserParser.FunCallContext ctx);
	/**
	 * Enter a parse tree produced by {@link FunctionExprParserParser#params}.
	 * @param ctx the parse tree
	 */
	void enterParams(FunctionExprParserParser.ParamsContext ctx);
	/**
	 * Exit a parse tree produced by {@link FunctionExprParserParser#params}.
	 * @param ctx the parse tree
	 */
	void exitParams(FunctionExprParserParser.ParamsContext ctx);
}