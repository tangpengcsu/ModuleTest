package com.szkingdom.sql.parser
import com.szkingdom.sql.parser.SqlBaseParser._
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode}


/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2018-08-25
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-08-25     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */
class AstBuilder(arg:Any)  extends SqlBaseBaseVisitor[AnyRef]{


  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }


  override def visitChildren(node: RuleNode): AnyRef = {
    if (node.getChildCount == 1) {
      node.getChild(0).accept(this)
    } else {
      null
    }
  }

  override def visitSingleStatement(ctx: SingleStatementContext): AnyRef = super
    .visitSingleStatement(ctx)

  override def visitSingleExpression(ctx: SingleExpressionContext): AnyRef = super
    .visitSingleExpression(ctx)

  override def visitSingleTableIdentifier(ctx: SingleTableIdentifierContext): AnyRef = super
    .visitSingleTableIdentifier(ctx)

  override def visitSingleFunctionIdentifier(ctx: SingleFunctionIdentifierContext): AnyRef = super
    .visitSingleFunctionIdentifier(ctx)

  override def visitSingleDataType(ctx: SingleDataTypeContext): AnyRef = super
    .visitSingleDataType(ctx)

  override def visitSingleTableSchema(ctx: SingleTableSchemaContext): AnyRef = super
    .visitSingleTableSchema(ctx)

  override def visitStatementDefault(ctx: StatementDefaultContext): AnyRef = super
    .visitStatementDefault(ctx)

  override def visitUse(ctx: UseContext): AnyRef = super.visitUse(ctx)

  override def visitCreateDatabase(ctx: CreateDatabaseContext): AnyRef = super
    .visitCreateDatabase(ctx)

  override def visitSetDatabaseProperties(ctx: SetDatabasePropertiesContext): AnyRef = super
    .visitSetDatabaseProperties(ctx)

  override def visitDropDatabase(ctx: DropDatabaseContext): AnyRef = super
    .visitDropDatabase(ctx)

  override def visitCreateTable(ctx: CreateTableContext): AnyRef = super
    .visitCreateTable(ctx)

  override def visitCreateHiveTable(ctx: CreateHiveTableContext): AnyRef = super
    .visitCreateHiveTable(ctx)

  override def visitCreateTableLike(ctx: CreateTableLikeContext): AnyRef = super
    .visitCreateTableLike(ctx)

  override def visitAnalyze(ctx: AnalyzeContext): AnyRef = super
    .visitAnalyze(ctx)

  override def visitAddTableColumns(ctx: AddTableColumnsContext): AnyRef = super
    .visitAddTableColumns(ctx)

  override def visitRenameTable(ctx: RenameTableContext): AnyRef = super
    .visitRenameTable(ctx)

  override def visitSetTableProperties(ctx: SetTablePropertiesContext): AnyRef = super
    .visitSetTableProperties(ctx)

  override def visitUnsetTableProperties(ctx: UnsetTablePropertiesContext): AnyRef = super
    .visitUnsetTableProperties(ctx)

  override def visitChangeColumn(ctx: ChangeColumnContext): AnyRef = super
    .visitChangeColumn(ctx)

  override def visitSetTableSerDe(ctx: SetTableSerDeContext): AnyRef = super
    .visitSetTableSerDe(ctx)

  override def visitAddTablePartition(ctx: AddTablePartitionContext): AnyRef = super
    .visitAddTablePartition(ctx)

  override def visitRenameTablePartition(ctx: RenameTablePartitionContext): AnyRef = super
    .visitRenameTablePartition(ctx)

  override def visitDropTablePartitions(ctx: DropTablePartitionsContext): AnyRef = super
    .visitDropTablePartitions(ctx)

  override def visitSetTableLocation(ctx: SetTableLocationContext): AnyRef = super
    .visitSetTableLocation(ctx)

  override def visitRecoverPartitions(ctx: RecoverPartitionsContext): AnyRef = super
    .visitRecoverPartitions(ctx)

  override def visitDropTable(ctx: DropTableContext): AnyRef = super
    .visitDropTable(ctx)

  override def visitCreateView(ctx: CreateViewContext): AnyRef = super
    .visitCreateView(ctx)

  override def visitCreateTempViewUsing(ctx: CreateTempViewUsingContext): AnyRef = super
    .visitCreateTempViewUsing(ctx)

  override def visitAlterViewQuery(ctx: AlterViewQueryContext): AnyRef = super
    .visitAlterViewQuery(ctx)

  override def visitCreateFunction(ctx: CreateFunctionContext): AnyRef = super
    .visitCreateFunction(ctx)

  override def visitDropFunction(ctx: DropFunctionContext): AnyRef = super
    .visitDropFunction(ctx)

  override def visitExplain(ctx: ExplainContext): AnyRef = super
    .visitExplain(ctx)

  override def visitShowTables(ctx: ShowTablesContext): AnyRef = super
    .visitShowTables(ctx)

  override def visitShowTable(ctx: ShowTableContext): AnyRef = super
    .visitShowTable(ctx)

  override def visitShowDatabases(ctx: ShowDatabasesContext): AnyRef = super
    .visitShowDatabases(ctx)

  override def visitShowTblProperties(ctx: ShowTblPropertiesContext): AnyRef = super
    .visitShowTblProperties(ctx)

  override def visitShowColumns(ctx: ShowColumnsContext): AnyRef = super
    .visitShowColumns(ctx)

  override def visitShowPartitions(ctx: ShowPartitionsContext): AnyRef = super
    .visitShowPartitions(ctx)

  override def visitShowFunctions(ctx: ShowFunctionsContext): AnyRef = super
    .visitShowFunctions(ctx)

  override def visitShowCreateTable(ctx: ShowCreateTableContext): AnyRef = super
    .visitShowCreateTable(ctx)

  override def visitDescribeFunction(ctx: DescribeFunctionContext): AnyRef = super
    .visitDescribeFunction(ctx)

  override def visitDescribeDatabase(ctx: DescribeDatabaseContext): AnyRef = super
    .visitDescribeDatabase(ctx)

  override def visitDescribeTable(ctx: DescribeTableContext): AnyRef = super
    .visitDescribeTable(ctx)

  override def visitRefreshTable(ctx: RefreshTableContext): AnyRef = super
    .visitRefreshTable(ctx)

  override def visitRefreshResource(ctx: RefreshResourceContext): AnyRef = super
    .visitRefreshResource(ctx)

  override def visitCacheTable(ctx: CacheTableContext): AnyRef = super
    .visitCacheTable(ctx)

  override def visitUncacheTable(ctx: UncacheTableContext): AnyRef = super
    .visitUncacheTable(ctx)

  override def visitClearCache(ctx: ClearCacheContext): AnyRef = super
    .visitClearCache(ctx)

  override def visitLoadData(ctx: LoadDataContext): AnyRef = super
    .visitLoadData(ctx)

  override def visitTruncateTable(ctx: TruncateTableContext): AnyRef = super
    .visitTruncateTable(ctx)

  override def visitRepairTable(ctx: RepairTableContext): AnyRef = super
    .visitRepairTable(ctx)

  override def visitManageResource(ctx: ManageResourceContext): AnyRef = super
    .visitManageResource(ctx)

  override def visitFailNativeCommand(ctx: FailNativeCommandContext): AnyRef = super
    .visitFailNativeCommand(ctx)

  override def visitSetConfiguration(ctx: SetConfigurationContext): AnyRef = super
    .visitSetConfiguration(ctx)

  override def visitResetConfiguration(ctx: ResetConfigurationContext): AnyRef = super
    .visitResetConfiguration(ctx)

  override def visitUnsupportedHiveNativeCommands(ctx: UnsupportedHiveNativeCommandsContext): AnyRef = super
    .visitUnsupportedHiveNativeCommands(ctx)

  override def visitCreateTableHeader(ctx: CreateTableHeaderContext): AnyRef = super
    .visitCreateTableHeader(ctx)

  override def visitBucketSpec(ctx: BucketSpecContext): AnyRef = super
    .visitBucketSpec(ctx)

  override def visitSkewSpec(ctx: SkewSpecContext): AnyRef = super
    .visitSkewSpec(ctx)

  override def visitLocationSpec(ctx: LocationSpecContext): AnyRef = super
    .visitLocationSpec(ctx)

  override def visitQuery(ctx: QueryContext): AnyRef = super
    .visitQuery(ctx)

  override def visitInsertOverwriteTable(ctx: InsertOverwriteTableContext): AnyRef = super
    .visitInsertOverwriteTable(ctx)

  override def visitInsertIntoTable(ctx: InsertIntoTableContext): AnyRef = super
    .visitInsertIntoTable(ctx)

  override def visitInsertOverwriteHiveDir(ctx: InsertOverwriteHiveDirContext): AnyRef = super
    .visitInsertOverwriteHiveDir(ctx)

  override def visitInsertOverwriteDir(ctx: InsertOverwriteDirContext): AnyRef = super
    .visitInsertOverwriteDir(ctx)

  override def visitPartitionSpecLocation(ctx: PartitionSpecLocationContext): AnyRef = super
    .visitPartitionSpecLocation(ctx)

  override def visitPartitionSpec(ctx: PartitionSpecContext): AnyRef = super
    .visitPartitionSpec(ctx)

  override def visitPartitionVal(ctx: PartitionValContext): AnyRef = super
    .visitPartitionVal(ctx)

  override def visitDescribeFuncName(ctx: DescribeFuncNameContext): AnyRef = super
    .visitDescribeFuncName(ctx)

  override def visitDescribeColName(ctx: DescribeColNameContext): AnyRef = super
    .visitDescribeColName(ctx)

  override def visitCtes(ctx: CtesContext): AnyRef = super.visitCtes(ctx)

  override def visitNamedQuery(ctx: NamedQueryContext): AnyRef = super
    .visitNamedQuery(ctx)

  override def visitTableProvider(ctx: TableProviderContext): AnyRef = super
    .visitTableProvider(ctx)

  override def visitTablePropertyList(ctx: TablePropertyListContext): AnyRef = super
    .visitTablePropertyList(ctx)

  override def visitTableProperty(ctx: TablePropertyContext): AnyRef = super
    .visitTableProperty(ctx)

  override def visitTablePropertyKey(ctx: TablePropertyKeyContext): AnyRef = super
    .visitTablePropertyKey(ctx)

  override def visitTablePropertyValue(ctx: TablePropertyValueContext): AnyRef = super
    .visitTablePropertyValue(ctx)

  override def visitConstantList(ctx: ConstantListContext): AnyRef = super
    .visitConstantList(ctx)

  override def visitNestedConstantList(ctx: NestedConstantListContext): AnyRef = super
    .visitNestedConstantList(ctx)

  override def visitCreateFileFormat(ctx: CreateFileFormatContext): AnyRef = super
    .visitCreateFileFormat(ctx)

  override def visitTableFileFormat(ctx: TableFileFormatContext): AnyRef = super
    .visitTableFileFormat(ctx)

  override def visitGenericFileFormat(ctx: GenericFileFormatContext): AnyRef = super
    .visitGenericFileFormat(ctx)

  override def visitStorageHandler(ctx: StorageHandlerContext): AnyRef = super
    .visitStorageHandler(ctx)

  override def visitResource(ctx: ResourceContext): AnyRef = super
    .visitResource(ctx)

  override def visitSingleInsertQuery(ctx: SingleInsertQueryContext): AnyRef = super
    .visitSingleInsertQuery(ctx)

  override def visitMultiInsertQuery(ctx: MultiInsertQueryContext): AnyRef = super
    .visitMultiInsertQuery(ctx)

  override def visitQueryOrganization(ctx: QueryOrganizationContext): AnyRef = super
    .visitQueryOrganization(ctx)

  override def visitMultiInsertQueryBody(ctx: MultiInsertQueryBodyContext): AnyRef = super
    .visitMultiInsertQueryBody(ctx)

  override def visitQueryTermDefault(ctx: QueryTermDefaultContext): AnyRef = super
    .visitQueryTermDefault(ctx)

  override def visitSetOperation(ctx: SetOperationContext): AnyRef = super
    .visitSetOperation(ctx)

  override def visitQueryPrimaryDefault(ctx: QueryPrimaryDefaultContext): AnyRef = super
    .visitQueryPrimaryDefault(ctx)

  override def visitTable(ctx: TableContext): AnyRef = super
    .visitTable(ctx)

  override def visitInlineTableDefault1(ctx: InlineTableDefault1Context): AnyRef = super
    .visitInlineTableDefault1(ctx)

  override def visitSubquery(ctx: SubqueryContext): AnyRef = super
    .visitSubquery(ctx)

  override def visitSortItem(ctx: SortItemContext): AnyRef = super
    .visitSortItem(ctx)

  override def visitQuerySpecification(ctx: QuerySpecificationContext): AnyRef = super
    .visitQuerySpecification(ctx)

  override def visitHint(ctx: HintContext): AnyRef = super.visitHint(ctx)

  override def visitHintStatement(ctx: HintStatementContext): AnyRef = super
    .visitHintStatement(ctx)

  override def visitFromClause(ctx: FromClauseContext): AnyRef = super
    .visitFromClause(ctx)

  override def visitAggregation(ctx: AggregationContext): AnyRef = super
    .visitAggregation(ctx)

  override def visitGroupingSet(ctx: GroupingSetContext): AnyRef = super
    .visitGroupingSet(ctx)

  override def visitPivotClause(ctx: PivotClauseContext): AnyRef = super
    .visitPivotClause(ctx)

  override def visitPivotColumn(ctx: PivotColumnContext): AnyRef = super
    .visitPivotColumn(ctx)

  override def visitPivotValue(ctx: PivotValueContext): AnyRef = super
    .visitPivotValue(ctx)

  override def visitLateralView(ctx: LateralViewContext): AnyRef = super
    .visitLateralView(ctx)

  override def visitSetQuantifier(ctx: SetQuantifierContext): AnyRef = super
    .visitSetQuantifier(ctx)

  override def visitRelation(ctx: RelationContext): AnyRef = super
    .visitRelation(ctx)

  override def visitJoinRelation(ctx: JoinRelationContext): AnyRef = super
    .visitJoinRelation(ctx)

  override def visitJoinType(ctx: JoinTypeContext): AnyRef = super
    .visitJoinType(ctx)

  override def visitJoinCriteria(ctx: JoinCriteriaContext): AnyRef = super
    .visitJoinCriteria(ctx)

  override def visitSample(ctx: SampleContext): AnyRef = super
    .visitSample(ctx)

  override def visitSampleByPercentile(ctx: SampleByPercentileContext): AnyRef = super
    .visitSampleByPercentile(ctx)

  override def visitSampleByRows(ctx: SampleByRowsContext): AnyRef = super
    .visitSampleByRows(ctx)

  override def visitSampleByBucket(ctx: SampleByBucketContext): AnyRef = super
    .visitSampleByBucket(ctx)

  override def visitSampleByBytes(ctx: SampleByBytesContext): AnyRef = super
    .visitSampleByBytes(ctx)

  override def visitIdentifierList(ctx: IdentifierListContext): AnyRef = super
    .visitIdentifierList(ctx)

  override def visitIdentifierSeq(ctx: IdentifierSeqContext): AnyRef = super
    .visitIdentifierSeq(ctx)

  override def visitOrderedIdentifierList(ctx: OrderedIdentifierListContext): AnyRef = super
    .visitOrderedIdentifierList(ctx)

  override def visitOrderedIdentifier(ctx: OrderedIdentifierContext): AnyRef = super
    .visitOrderedIdentifier(ctx)

  override def visitIdentifierCommentList(ctx: IdentifierCommentListContext): AnyRef = super
    .visitIdentifierCommentList(ctx)

  override def visitIdentifierComment(ctx: IdentifierCommentContext): AnyRef = super
    .visitIdentifierComment(ctx)

  override def visitTableName(ctx: TableNameContext): AnyRef = super
    .visitTableName(ctx)

  override def visitAliasedQuery(ctx: AliasedQueryContext): AnyRef = super
    .visitAliasedQuery(ctx)

  override def visitAliasedRelation(ctx: AliasedRelationContext): AnyRef = super
    .visitAliasedRelation(ctx)

  override def visitInlineTableDefault2(ctx: InlineTableDefault2Context): AnyRef = super
    .visitInlineTableDefault2(ctx)

  override def visitTableValuedFunction(ctx: TableValuedFunctionContext): AnyRef = super
    .visitTableValuedFunction(ctx)

  override def visitInlineTable(ctx: InlineTableContext): AnyRef = super
    .visitInlineTable(ctx)

  override def visitFunctionTable(ctx: FunctionTableContext): AnyRef = super
    .visitFunctionTable(ctx)

  override def visitTableAlias(ctx: TableAliasContext): AnyRef = super
    .visitTableAlias(ctx)

  override def visitRowFormatSerde(ctx: RowFormatSerdeContext): AnyRef = super
    .visitRowFormatSerde(ctx)

  override def visitRowFormatDelimited(ctx: RowFormatDelimitedContext): AnyRef = super
    .visitRowFormatDelimited(ctx)

  override def visitTableIdentifier(ctx: TableIdentifierContext): AnyRef = super
    .visitTableIdentifier(ctx)

  override def visitFunctionIdentifier(ctx: FunctionIdentifierContext): AnyRef = super
    .visitFunctionIdentifier(ctx)

  override def visitNamedExpression(ctx: NamedExpressionContext): AnyRef = super
    .visitNamedExpression(ctx)

  override def visitNamedExpressionSeq(ctx: NamedExpressionSeqContext): AnyRef = super
    .visitNamedExpressionSeq(ctx)

  override def visitExpression(ctx: ExpressionContext): AnyRef = super
    .visitExpression(ctx)

  override def visitLogicalNot(ctx: LogicalNotContext): AnyRef = super
    .visitLogicalNot(ctx)

  override def visitPredicated(ctx: PredicatedContext): AnyRef = super
    .visitPredicated(ctx)

  override def visitExists(ctx: ExistsContext): AnyRef = super
    .visitExists(ctx)

  override def visitLogicalBinary(ctx: LogicalBinaryContext): AnyRef = super
    .visitLogicalBinary(ctx)

  override def visitPredicate(ctx: PredicateContext): AnyRef = super
    .visitPredicate(ctx)

  override def visitValueExpressionDefault(ctx: ValueExpressionDefaultContext): AnyRef = super
    .visitValueExpressionDefault(ctx)

  override def visitComparison(ctx: ComparisonContext): AnyRef = super
    .visitComparison(ctx)

  override def visitArithmeticBinary(ctx: ArithmeticBinaryContext): AnyRef = super
    .visitArithmeticBinary(ctx)

  override def visitArithmeticUnary(ctx: ArithmeticUnaryContext): AnyRef = super
    .visitArithmeticUnary(ctx)

  override def visitStruct(ctx: StructContext): AnyRef = super
    .visitStruct(ctx)

  override def visitDereference(ctx: DereferenceContext): AnyRef = super
    .visitDereference(ctx)

  override def visitSimpleCase(ctx: SimpleCaseContext): AnyRef = super
    .visitSimpleCase(ctx)

  override def visitColumnReference(ctx: ColumnReferenceContext): AnyRef = super
    .visitColumnReference(ctx)

  override def visitRowConstructor(ctx: RowConstructorContext): AnyRef = super
    .visitRowConstructor(ctx)

  override def visitLast(ctx: LastContext): AnyRef = super.visitLast(ctx)

  override def visitStar(ctx: StarContext): AnyRef = super.visitStar(ctx)

  override def visitSubscript(ctx: SubscriptContext): AnyRef = super
    .visitSubscript(ctx)

  override def visitSubqueryExpression(ctx: SubqueryExpressionContext): AnyRef = super
    .visitSubqueryExpression(ctx)

  override def visitCast(ctx: CastContext): AnyRef = super.visitCast(ctx)

  override def visitConstantDefault(ctx: ConstantDefaultContext): AnyRef = super
    .visitConstantDefault(ctx)

  override def visitLambda(ctx: LambdaContext): AnyRef = super
    .visitLambda(ctx)

  override def visitParenthesizedExpression(ctx: ParenthesizedExpressionContext): AnyRef = super
    .visitParenthesizedExpression(ctx)

  override def visitExtract(ctx: ExtractContext): AnyRef = super
    .visitExtract(ctx)

  override def visitFunctionCall(ctx: FunctionCallContext): AnyRef = super
    .visitFunctionCall(ctx)

  override def visitSearchedCase(ctx: SearchedCaseContext): AnyRef = super
    .visitSearchedCase(ctx)

  override def visitPosition(ctx: PositionContext): AnyRef = super
    .visitPosition(ctx)

  override def visitFirst(ctx: FirstContext): AnyRef = super
    .visitFirst(ctx)

  override def visitNullLiteral(ctx: NullLiteralContext): AnyRef = super
    .visitNullLiteral(ctx)

  override def visitIntervalLiteral(ctx: IntervalLiteralContext): AnyRef = super
    .visitIntervalLiteral(ctx)

  override def visitTypeConstructor(ctx: TypeConstructorContext): AnyRef = super
    .visitTypeConstructor(ctx)

  override def visitNumericLiteral(ctx: NumericLiteralContext): AnyRef = super
    .visitNumericLiteral(ctx)

  override def visitBooleanLiteral(ctx: BooleanLiteralContext): AnyRef = super
    .visitBooleanLiteral(ctx)

  override def visitStringLiteral(ctx: StringLiteralContext): AnyRef = super
    .visitStringLiteral(ctx)

  override def visitComparisonOperator(ctx: ComparisonOperatorContext): AnyRef = super
    .visitComparisonOperator(ctx)

  override def visitArithmeticOperator(ctx: ArithmeticOperatorContext): AnyRef = super
    .visitArithmeticOperator(ctx)

  override def visitPredicateOperator(ctx: PredicateOperatorContext): AnyRef = super
    .visitPredicateOperator(ctx)

  override def visitBooleanValue(ctx: BooleanValueContext): AnyRef = super
    .visitBooleanValue(ctx)

  override def visitInterval(ctx: IntervalContext): AnyRef = super
    .visitInterval(ctx)

  override def visitIntervalField(ctx: IntervalFieldContext): AnyRef = super
    .visitIntervalField(ctx)

  override def visitIntervalValue(ctx: IntervalValueContext): AnyRef = super
    .visitIntervalValue(ctx)

  override def visitColPosition(ctx: ColPositionContext): AnyRef = super
    .visitColPosition(ctx)

  override def visitComplexDataType(ctx: ComplexDataTypeContext): AnyRef = super
    .visitComplexDataType(ctx)

  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): AnyRef = super
    .visitPrimitiveDataType(ctx)

  override def visitColTypeList(ctx: ColTypeListContext): AnyRef = super
    .visitColTypeList(ctx)

  override def visitColType(ctx: ColTypeContext): AnyRef = super
    .visitColType(ctx)

  override def visitComplexColTypeList(ctx: ComplexColTypeListContext): AnyRef = super
    .visitComplexColTypeList(ctx)

  override def visitComplexColType(ctx: ComplexColTypeContext): AnyRef = super
    .visitComplexColType(ctx)

  override def visitWhenClause(ctx: WhenClauseContext): AnyRef = super
    .visitWhenClause(ctx)

  override def visitWindows(ctx: WindowsContext): AnyRef = super
    .visitWindows(ctx)

  override def visitNamedWindow(ctx: NamedWindowContext): AnyRef = super
    .visitNamedWindow(ctx)

  override def visitWindowRef(ctx: WindowRefContext): AnyRef = super
    .visitWindowRef(ctx)

  override def visitWindowDef(ctx: WindowDefContext): AnyRef = super
    .visitWindowDef(ctx)

  override def visitWindowFrame(ctx: WindowFrameContext): AnyRef = super
    .visitWindowFrame(ctx)

  override def visitFrameBound(ctx: FrameBoundContext): AnyRef = super
    .visitFrameBound(ctx)

  override def visitQualifiedName(ctx: QualifiedNameContext): AnyRef = super
    .visitQualifiedName(ctx)

  override def visitIdentifier(ctx: IdentifierContext): AnyRef = super
    .visitIdentifier(ctx)

  override def visitUnquotedIdentifier(ctx: UnquotedIdentifierContext): AnyRef = super
    .visitUnquotedIdentifier(ctx)

  override def visitQuotedIdentifierAlternative(ctx: QuotedIdentifierAlternativeContext): AnyRef = super
    .visitQuotedIdentifierAlternative(ctx)

  override def visitQuotedIdentifier(ctx: QuotedIdentifierContext): AnyRef = super
    .visitQuotedIdentifier(ctx)

  override def visitDecimalLiteral(ctx: DecimalLiteralContext): AnyRef = super
    .visitDecimalLiteral(ctx)

  override def visitIntegerLiteral(ctx: IntegerLiteralContext): AnyRef = super
    .visitIntegerLiteral(ctx)

  override def visitBigIntLiteral(ctx: BigIntLiteralContext): AnyRef = super
    .visitBigIntLiteral(ctx)

  override def visitSmallIntLiteral(ctx: SmallIntLiteralContext): AnyRef = super
    .visitSmallIntLiteral(ctx)

  override def visitTinyIntLiteral(ctx: TinyIntLiteralContext): AnyRef = super
    .visitTinyIntLiteral(ctx)

  override def visitDoubleLiteral(ctx: DoubleLiteralContext): AnyRef = super
    .visitDoubleLiteral(ctx)

  override def visitBigDecimalLiteral(ctx: BigDecimalLiteralContext): AnyRef = super
    .visitBigDecimalLiteral(ctx)

  override def visitNonReserved(ctx: NonReservedContext): AnyRef = super
    .visitNonReserved(ctx)
}
