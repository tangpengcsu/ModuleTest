// Generated from D:/workspace/IdeaProjects/ModuleTest/antlr-test/src/main/resources\FunctionExprParser.g4 by ANTLR 4.7
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link FunctionExprParserParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface FunctionExprParserVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link FunctionExprParserParser#exp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExp(FunctionExprParserParser.ExpContext ctx);
	/**
	 * Visit a parse tree produced by {@link FunctionExprParserParser#term}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTerm(FunctionExprParserParser.TermContext ctx);
	/**
	 * Visit a parse tree produced by the {@code columnName}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnName(FunctionExprParserParser.ColumnNameContext ctx);
	/**
	 * Visit a parse tree produced by the {@code char}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitChar(FunctionExprParserParser.CharContext ctx);
	/**
	 * Visit a parse tree produced by the {@code number}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumber(FunctionExprParserParser.NumberContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ngNumber}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNgNumber(FunctionExprParserParser.NgNumberContext ctx);
	/**
	 * Visit a parse tree produced by the {@code func}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunc(FunctionExprParserParser.FuncContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parenthesis}
	 * labeled alternative in {@link FunctionExprParserParser#factor}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenthesis(FunctionExprParserParser.ParenthesisContext ctx);
	/**
	 * Visit a parse tree produced by {@link FunctionExprParserParser#funCall}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunCall(FunctionExprParserParser.FunCallContext ctx);
	/**
	 * Visit a parse tree produced by {@link FunctionExprParserParser#params}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParams(FunctionExprParserParser.ParamsContext ctx);
}