// Generated from D:/workspace/IdeaProjects/ModuleTest/antlr-test/src/main/resources\Demo.g4 by ANTLR 4.7
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link DemoParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface DemoVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link DemoParser#prog}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitProg(DemoParser.ProgContext ctx);
	/**
	 * Visit a parse tree produced by {@link DemoParser#stat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStat(DemoParser.StatContext ctx);
	/**
	 * Visit a parse tree produced by {@link DemoParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpr(DemoParser.ExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link DemoParser#multExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultExpr(DemoParser.MultExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link DemoParser#atom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAtom(DemoParser.AtomContext ctx);
	/**
	 * Visit a parse tree produced by {@link DemoParser#ws}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWs(DemoParser.WsContext ctx);
}