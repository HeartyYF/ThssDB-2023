// Generated from SQL.g4 by ANTLR 4.8

package cn.edu.thssdb.sql;

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SQLParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.8", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, EQ=6, NE=7, LT=8, GT=9, LE=10, 
		GE=11, ADD=12, SUB=13, MUL=14, DIV=15, AND=16, OR=17, T_INT=18, T_LONG=19, 
		T_FLOAT=20, T_DOUBLE=21, T_STRING=22, K_ADD=23, K_ALL=24, K_AS=25, K_BEGIN=26, 
		K_BY=27, K_CHECKPOINT=28, K_COLUMN=29, K_COMMIT=30, K_CREATE=31, K_DATABASE=32, 
		K_DATABASES=33, K_DELETE=34, K_DISTINCT=35, K_DROP=36, K_EXISTS=37, K_FROM=38, 
		K_GRANT=39, K_IF=40, K_IDENTIFIED=41, K_INSERT=42, K_INTO=43, K_JOIN=44, 
		K_KEY=45, K_NOT=46, K_NULL=47, K_ON=48, K_PRIMARY=49, K_QUIT=50, K_REVOKE=51, 
		K_ROLLBACK=52, K_SAVEPOINT=53, K_SELECT=54, K_SET=55, K_SHOW=56, K_TABLE=57, 
		K_TO=58, K_TRANSACTION=59, K_UPDATE=60, K_USE=61, K_USER=62, K_VALUES=63, 
		K_VIEW=64, K_WHERE=65, IDENTIFIER=66, NUMERIC_LITERAL=67, EXPONENT=68, 
		STRING_LITERAL=69, SINGLE_LINE_COMMENT=70, MULTILINE_COMMENT=71, SPACES=72;
	public static final int
		RULE_parse = 0, RULE_sqlStmtList = 1, RULE_sqlStmt = 2, RULE_createDbStmt = 3, 
		RULE_dropDbStmt = 4, RULE_createUserStmt = 5, RULE_dropUserStmt = 6, RULE_commitStmt = 7, 
		RULE_beginTransactionStmt = 8, RULE_rollbackStmt = 9, RULE_savepointStmt = 10, 
		RULE_checkpointStmt = 11, RULE_createTableStmt = 12, RULE_grantStmt = 13, 
		RULE_revokeStmt = 14, RULE_useDbStmt = 15, RULE_deleteStmt = 16, RULE_dropTableStmt = 17, 
		RULE_showDbStmt = 18, RULE_quitStmt = 19, RULE_showTableStmt = 20, RULE_insertStmt = 21, 
		RULE_valueEntry = 22, RULE_selectStmt = 23, RULE_createViewStmt = 24, 
		RULE_dropViewStmt = 25, RULE_updateStmt = 26, RULE_columnDef = 27, RULE_typeName = 28, 
		RULE_columnConstraint = 29, RULE_multipleCondition = 30, RULE_condition = 31, 
		RULE_comparer = 32, RULE_comparator = 33, RULE_expression = 34, RULE_tableConstraint = 35, 
		RULE_resultColumn = 36, RULE_tableQuery = 37, RULE_authLevel = 38, RULE_literalValue = 39, 
		RULE_columnFullName = 40, RULE_databaseName = 41, RULE_tableName = 42, 
		RULE_userName = 43, RULE_columnName = 44, RULE_viewName = 45, RULE_password = 46;
	private static String[] makeRuleNames() {
		return new String[] {
			"parse", "sqlStmtList", "sqlStmt", "createDbStmt", "dropDbStmt", "createUserStmt", 
			"dropUserStmt", "commitStmt", "beginTransactionStmt", "rollbackStmt", 
			"savepointStmt", "checkpointStmt", "createTableStmt", "grantStmt", "revokeStmt", 
			"useDbStmt", "deleteStmt", "dropTableStmt", "showDbStmt", "quitStmt", 
			"showTableStmt", "insertStmt", "valueEntry", "selectStmt", "createViewStmt", 
			"dropViewStmt", "updateStmt", "columnDef", "typeName", "columnConstraint", 
			"multipleCondition", "condition", "comparer", "comparator", "expression", 
			"tableConstraint", "resultColumn", "tableQuery", "authLevel", "literalValue", 
			"columnFullName", "databaseName", "tableName", "userName", "columnName", 
			"viewName", "password"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "';'", "'('", "','", "')'", "'.'", "'='", "'<>'", "'<'", "'>'", 
			"'<='", "'>='", "'+'", "'-'", "'*'", "'/'", "'&&'", "'||'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, "EQ", "NE", "LT", "GT", "LE", "GE", 
			"ADD", "SUB", "MUL", "DIV", "AND", "OR", "T_INT", "T_LONG", "T_FLOAT", 
			"T_DOUBLE", "T_STRING", "K_ADD", "K_ALL", "K_AS", "K_BEGIN", "K_BY", 
			"K_CHECKPOINT", "K_COLUMN", "K_COMMIT", "K_CREATE", "K_DATABASE", "K_DATABASES", 
			"K_DELETE", "K_DISTINCT", "K_DROP", "K_EXISTS", "K_FROM", "K_GRANT", 
			"K_IF", "K_IDENTIFIED", "K_INSERT", "K_INTO", "K_JOIN", "K_KEY", "K_NOT", 
			"K_NULL", "K_ON", "K_PRIMARY", "K_QUIT", "K_REVOKE", "K_ROLLBACK", "K_SAVEPOINT", 
			"K_SELECT", "K_SET", "K_SHOW", "K_TABLE", "K_TO", "K_TRANSACTION", "K_UPDATE", 
			"K_USE", "K_USER", "K_VALUES", "K_VIEW", "K_WHERE", "IDENTIFIER", "NUMERIC_LITERAL", 
			"EXPONENT", "STRING_LITERAL", "SINGLE_LINE_COMMENT", "MULTILINE_COMMENT", 
			"SPACES"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "SQL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public SQLParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class ParseContext extends ParserRuleContext {
		public SqlStmtListContext sqlStmtList() {
			return getRuleContext(SqlStmtListContext.class,0);
		}
		public ParseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parse; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterParse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitParse(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitParse(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParseContext parse() throws RecognitionException {
		ParseContext _localctx = new ParseContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_parse);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(94);
			sqlStmtList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SqlStmtListContext extends ParserRuleContext {
		public List<SqlStmtContext> sqlStmt() {
			return getRuleContexts(SqlStmtContext.class);
		}
		public SqlStmtContext sqlStmt(int i) {
			return getRuleContext(SqlStmtContext.class,i);
		}
		public SqlStmtListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sqlStmtList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterSqlStmtList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitSqlStmtList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitSqlStmtList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SqlStmtListContext sqlStmtList() throws RecognitionException {
		SqlStmtListContext _localctx = new SqlStmtListContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_sqlStmtList);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(99);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__0) {
				{
				{
				setState(96);
				match(T__0);
				}
				}
				setState(101);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(102);
			sqlStmt();
			setState(111);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,2,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(104); 
					_errHandler.sync(this);
					_la = _input.LA(1);
					do {
						{
						{
						setState(103);
						match(T__0);
						}
						}
						setState(106); 
						_errHandler.sync(this);
						_la = _input.LA(1);
					} while ( _la==T__0 );
					setState(108);
					sqlStmt();
					}
					} 
				}
				setState(113);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,2,_ctx);
			}
			setState(117);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__0) {
				{
				{
				setState(114);
				match(T__0);
				}
				}
				setState(119);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SqlStmtContext extends ParserRuleContext {
		public CreateTableStmtContext createTableStmt() {
			return getRuleContext(CreateTableStmtContext.class,0);
		}
		public CreateDbStmtContext createDbStmt() {
			return getRuleContext(CreateDbStmtContext.class,0);
		}
		public CreateUserStmtContext createUserStmt() {
			return getRuleContext(CreateUserStmtContext.class,0);
		}
		public DropDbStmtContext dropDbStmt() {
			return getRuleContext(DropDbStmtContext.class,0);
		}
		public DropUserStmtContext dropUserStmt() {
			return getRuleContext(DropUserStmtContext.class,0);
		}
		public CommitStmtContext commitStmt() {
			return getRuleContext(CommitStmtContext.class,0);
		}
		public BeginTransactionStmtContext beginTransactionStmt() {
			return getRuleContext(BeginTransactionStmtContext.class,0);
		}
		public RollbackStmtContext rollbackStmt() {
			return getRuleContext(RollbackStmtContext.class,0);
		}
		public SavepointStmtContext savepointStmt() {
			return getRuleContext(SavepointStmtContext.class,0);
		}
		public CheckpointStmtContext checkpointStmt() {
			return getRuleContext(CheckpointStmtContext.class,0);
		}
		public DeleteStmtContext deleteStmt() {
			return getRuleContext(DeleteStmtContext.class,0);
		}
		public DropTableStmtContext dropTableStmt() {
			return getRuleContext(DropTableStmtContext.class,0);
		}
		public InsertStmtContext insertStmt() {
			return getRuleContext(InsertStmtContext.class,0);
		}
		public SelectStmtContext selectStmt() {
			return getRuleContext(SelectStmtContext.class,0);
		}
		public CreateViewStmtContext createViewStmt() {
			return getRuleContext(CreateViewStmtContext.class,0);
		}
		public DropViewStmtContext dropViewStmt() {
			return getRuleContext(DropViewStmtContext.class,0);
		}
		public GrantStmtContext grantStmt() {
			return getRuleContext(GrantStmtContext.class,0);
		}
		public RevokeStmtContext revokeStmt() {
			return getRuleContext(RevokeStmtContext.class,0);
		}
		public UseDbStmtContext useDbStmt() {
			return getRuleContext(UseDbStmtContext.class,0);
		}
		public ShowDbStmtContext showDbStmt() {
			return getRuleContext(ShowDbStmtContext.class,0);
		}
		public ShowTableStmtContext showTableStmt() {
			return getRuleContext(ShowTableStmtContext.class,0);
		}
		public QuitStmtContext quitStmt() {
			return getRuleContext(QuitStmtContext.class,0);
		}
		public UpdateStmtContext updateStmt() {
			return getRuleContext(UpdateStmtContext.class,0);
		}
		public SqlStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sqlStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterSqlStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitSqlStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitSqlStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SqlStmtContext sqlStmt() throws RecognitionException {
		SqlStmtContext _localctx = new SqlStmtContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_sqlStmt);
		try {
			setState(143);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(120);
				createTableStmt();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(121);
				createDbStmt();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(122);
				createUserStmt();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(123);
				dropDbStmt();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(124);
				dropUserStmt();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(125);
				commitStmt();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(126);
				beginTransactionStmt();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(127);
				rollbackStmt();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(128);
				savepointStmt();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(129);
				checkpointStmt();
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(130);
				deleteStmt();
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(131);
				dropTableStmt();
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(132);
				insertStmt();
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(133);
				selectStmt();
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(134);
				createViewStmt();
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(135);
				dropViewStmt();
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(136);
				grantStmt();
				}
				break;
			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(137);
				revokeStmt();
				}
				break;
			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(138);
				useDbStmt();
				}
				break;
			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(139);
				showDbStmt();
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(140);
				showTableStmt();
				}
				break;
			case 22:
				enterOuterAlt(_localctx, 22);
				{
				setState(141);
				quitStmt();
				}
				break;
			case 23:
				enterOuterAlt(_localctx, 23);
				{
				setState(142);
				updateStmt();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateDbStmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLParser.K_CREATE, 0); }
		public TerminalNode K_DATABASE() { return getToken(SQLParser.K_DATABASE, 0); }
		public DatabaseNameContext databaseName() {
			return getRuleContext(DatabaseNameContext.class,0);
		}
		public CreateDbStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createDbStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterCreateDbStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitCreateDbStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitCreateDbStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateDbStmtContext createDbStmt() throws RecognitionException {
		CreateDbStmtContext _localctx = new CreateDbStmtContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_createDbStmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(145);
			match(K_CREATE);
			setState(146);
			match(K_DATABASE);
			setState(147);
			databaseName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DropDbStmtContext extends ParserRuleContext {
		public TerminalNode K_DROP() { return getToken(SQLParser.K_DROP, 0); }
		public TerminalNode K_DATABASE() { return getToken(SQLParser.K_DATABASE, 0); }
		public DatabaseNameContext databaseName() {
			return getRuleContext(DatabaseNameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLParser.K_IF, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLParser.K_EXISTS, 0); }
		public DropDbStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropDbStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterDropDbStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitDropDbStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitDropDbStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropDbStmtContext dropDbStmt() throws RecognitionException {
		DropDbStmtContext _localctx = new DropDbStmtContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_dropDbStmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(149);
			match(K_DROP);
			setState(150);
			match(K_DATABASE);
			setState(153);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_IF) {
				{
				setState(151);
				match(K_IF);
				setState(152);
				match(K_EXISTS);
				}
			}

			setState(155);
			databaseName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateUserStmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLParser.K_CREATE, 0); }
		public TerminalNode K_USER() { return getToken(SQLParser.K_USER, 0); }
		public UserNameContext userName() {
			return getRuleContext(UserNameContext.class,0);
		}
		public TerminalNode K_IDENTIFIED() { return getToken(SQLParser.K_IDENTIFIED, 0); }
		public TerminalNode K_BY() { return getToken(SQLParser.K_BY, 0); }
		public PasswordContext password() {
			return getRuleContext(PasswordContext.class,0);
		}
		public CreateUserStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createUserStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterCreateUserStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitCreateUserStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitCreateUserStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateUserStmtContext createUserStmt() throws RecognitionException {
		CreateUserStmtContext _localctx = new CreateUserStmtContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_createUserStmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(157);
			match(K_CREATE);
			setState(158);
			match(K_USER);
			setState(159);
			userName();
			setState(160);
			match(K_IDENTIFIED);
			setState(161);
			match(K_BY);
			setState(162);
			password();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DropUserStmtContext extends ParserRuleContext {
		public TerminalNode K_DROP() { return getToken(SQLParser.K_DROP, 0); }
		public TerminalNode K_USER() { return getToken(SQLParser.K_USER, 0); }
		public UserNameContext userName() {
			return getRuleContext(UserNameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLParser.K_IF, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLParser.K_EXISTS, 0); }
		public DropUserStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropUserStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterDropUserStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitDropUserStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitDropUserStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropUserStmtContext dropUserStmt() throws RecognitionException {
		DropUserStmtContext _localctx = new DropUserStmtContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_dropUserStmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(164);
			match(K_DROP);
			setState(165);
			match(K_USER);
			setState(168);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_IF) {
				{
				setState(166);
				match(K_IF);
				setState(167);
				match(K_EXISTS);
				}
			}

			setState(170);
			userName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CommitStmtContext extends ParserRuleContext {
		public TerminalNode K_COMMIT() { return getToken(SQLParser.K_COMMIT, 0); }
		public CommitStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_commitStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterCommitStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitCommitStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitCommitStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CommitStmtContext commitStmt() throws RecognitionException {
		CommitStmtContext _localctx = new CommitStmtContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_commitStmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(172);
			match(K_COMMIT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BeginTransactionStmtContext extends ParserRuleContext {
		public TerminalNode K_BEGIN() { return getToken(SQLParser.K_BEGIN, 0); }
		public TerminalNode K_TRANSACTION() { return getToken(SQLParser.K_TRANSACTION, 0); }
		public BeginTransactionStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_beginTransactionStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterBeginTransactionStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitBeginTransactionStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitBeginTransactionStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BeginTransactionStmtContext beginTransactionStmt() throws RecognitionException {
		BeginTransactionStmtContext _localctx = new BeginTransactionStmtContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_beginTransactionStmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(174);
			match(K_BEGIN);
			setState(175);
			match(K_TRANSACTION);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RollbackStmtContext extends ParserRuleContext {
		public TerminalNode K_ROLLBACK() { return getToken(SQLParser.K_ROLLBACK, 0); }
		public TerminalNode IDENTIFIER() { return getToken(SQLParser.IDENTIFIER, 0); }
		public RollbackStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rollbackStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterRollbackStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitRollbackStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitRollbackStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RollbackStmtContext rollbackStmt() throws RecognitionException {
		RollbackStmtContext _localctx = new RollbackStmtContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_rollbackStmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(177);
			match(K_ROLLBACK);
			setState(178);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SavepointStmtContext extends ParserRuleContext {
		public TerminalNode K_SAVEPOINT() { return getToken(SQLParser.K_SAVEPOINT, 0); }
		public TerminalNode IDENTIFIER() { return getToken(SQLParser.IDENTIFIER, 0); }
		public SavepointStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_savepointStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterSavepointStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitSavepointStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitSavepointStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SavepointStmtContext savepointStmt() throws RecognitionException {
		SavepointStmtContext _localctx = new SavepointStmtContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_savepointStmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(180);
			match(K_SAVEPOINT);
			setState(181);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CheckpointStmtContext extends ParserRuleContext {
		public TerminalNode K_CHECKPOINT() { return getToken(SQLParser.K_CHECKPOINT, 0); }
		public CheckpointStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_checkpointStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterCheckpointStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitCheckpointStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitCheckpointStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CheckpointStmtContext checkpointStmt() throws RecognitionException {
		CheckpointStmtContext _localctx = new CheckpointStmtContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_checkpointStmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(183);
			match(K_CHECKPOINT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateTableStmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLParser.K_CREATE, 0); }
		public TerminalNode K_TABLE() { return getToken(SQLParser.K_TABLE, 0); }
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public List<ColumnDefContext> columnDef() {
			return getRuleContexts(ColumnDefContext.class);
		}
		public ColumnDefContext columnDef(int i) {
			return getRuleContext(ColumnDefContext.class,i);
		}
		public TableConstraintContext tableConstraint() {
			return getRuleContext(TableConstraintContext.class,0);
		}
		public CreateTableStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTableStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterCreateTableStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitCreateTableStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitCreateTableStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateTableStmtContext createTableStmt() throws RecognitionException {
		CreateTableStmtContext _localctx = new CreateTableStmtContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_createTableStmt);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(185);
			match(K_CREATE);
			setState(186);
			match(K_TABLE);
			setState(187);
			tableName();
			setState(188);
			match(T__1);
			setState(189);
			columnDef();
			setState(194);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,7,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(190);
					match(T__2);
					setState(191);
					columnDef();
					}
					} 
				}
				setState(196);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,7,_ctx);
			}
			setState(199);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__2) {
				{
				setState(197);
				match(T__2);
				setState(198);
				tableConstraint();
				}
			}

			setState(201);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GrantStmtContext extends ParserRuleContext {
		public TerminalNode K_GRANT() { return getToken(SQLParser.K_GRANT, 0); }
		public List<AuthLevelContext> authLevel() {
			return getRuleContexts(AuthLevelContext.class);
		}
		public AuthLevelContext authLevel(int i) {
			return getRuleContext(AuthLevelContext.class,i);
		}
		public TerminalNode K_ON() { return getToken(SQLParser.K_ON, 0); }
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public TerminalNode K_TO() { return getToken(SQLParser.K_TO, 0); }
		public UserNameContext userName() {
			return getRuleContext(UserNameContext.class,0);
		}
		public GrantStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grantStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterGrantStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitGrantStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitGrantStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GrantStmtContext grantStmt() throws RecognitionException {
		GrantStmtContext _localctx = new GrantStmtContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_grantStmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(203);
			match(K_GRANT);
			setState(204);
			authLevel();
			setState(209);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(205);
				match(T__2);
				setState(206);
				authLevel();
				}
				}
				setState(211);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(212);
			match(K_ON);
			setState(213);
			tableName();
			setState(214);
			match(K_TO);
			setState(215);
			userName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RevokeStmtContext extends ParserRuleContext {
		public TerminalNode K_REVOKE() { return getToken(SQLParser.K_REVOKE, 0); }
		public List<AuthLevelContext> authLevel() {
			return getRuleContexts(AuthLevelContext.class);
		}
		public AuthLevelContext authLevel(int i) {
			return getRuleContext(AuthLevelContext.class,i);
		}
		public TerminalNode K_ON() { return getToken(SQLParser.K_ON, 0); }
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public TerminalNode K_FROM() { return getToken(SQLParser.K_FROM, 0); }
		public UserNameContext userName() {
			return getRuleContext(UserNameContext.class,0);
		}
		public RevokeStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revokeStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterRevokeStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitRevokeStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitRevokeStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RevokeStmtContext revokeStmt() throws RecognitionException {
		RevokeStmtContext _localctx = new RevokeStmtContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_revokeStmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(217);
			match(K_REVOKE);
			setState(218);
			authLevel();
			setState(223);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(219);
				match(T__2);
				setState(220);
				authLevel();
				}
				}
				setState(225);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(226);
			match(K_ON);
			setState(227);
			tableName();
			setState(228);
			match(K_FROM);
			setState(229);
			userName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UseDbStmtContext extends ParserRuleContext {
		public TerminalNode K_USE() { return getToken(SQLParser.K_USE, 0); }
		public DatabaseNameContext databaseName() {
			return getRuleContext(DatabaseNameContext.class,0);
		}
		public UseDbStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_useDbStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterUseDbStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitUseDbStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitUseDbStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UseDbStmtContext useDbStmt() throws RecognitionException {
		UseDbStmtContext _localctx = new UseDbStmtContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_useDbStmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(231);
			match(K_USE);
			setState(232);
			databaseName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DeleteStmtContext extends ParserRuleContext {
		public TerminalNode K_DELETE() { return getToken(SQLParser.K_DELETE, 0); }
		public TerminalNode K_FROM() { return getToken(SQLParser.K_FROM, 0); }
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public TerminalNode K_WHERE() { return getToken(SQLParser.K_WHERE, 0); }
		public MultipleConditionContext multipleCondition() {
			return getRuleContext(MultipleConditionContext.class,0);
		}
		public DeleteStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_deleteStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterDeleteStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitDeleteStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitDeleteStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DeleteStmtContext deleteStmt() throws RecognitionException {
		DeleteStmtContext _localctx = new DeleteStmtContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_deleteStmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(234);
			match(K_DELETE);
			setState(235);
			match(K_FROM);
			setState(236);
			tableName();
			setState(239);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WHERE) {
				{
				setState(237);
				match(K_WHERE);
				setState(238);
				multipleCondition(0);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DropTableStmtContext extends ParserRuleContext {
		public TerminalNode K_DROP() { return getToken(SQLParser.K_DROP, 0); }
		public TerminalNode K_TABLE() { return getToken(SQLParser.K_TABLE, 0); }
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLParser.K_IF, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLParser.K_EXISTS, 0); }
		public DropTableStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropTableStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterDropTableStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitDropTableStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitDropTableStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropTableStmtContext dropTableStmt() throws RecognitionException {
		DropTableStmtContext _localctx = new DropTableStmtContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_dropTableStmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(241);
			match(K_DROP);
			setState(242);
			match(K_TABLE);
			setState(245);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_IF) {
				{
				setState(243);
				match(K_IF);
				setState(244);
				match(K_EXISTS);
				}
			}

			setState(247);
			tableName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ShowDbStmtContext extends ParserRuleContext {
		public TerminalNode K_SHOW() { return getToken(SQLParser.K_SHOW, 0); }
		public TerminalNode K_DATABASES() { return getToken(SQLParser.K_DATABASES, 0); }
		public ShowDbStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showDbStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterShowDbStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitShowDbStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitShowDbStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ShowDbStmtContext showDbStmt() throws RecognitionException {
		ShowDbStmtContext _localctx = new ShowDbStmtContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_showDbStmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(249);
			match(K_SHOW);
			setState(250);
			match(K_DATABASES);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuitStmtContext extends ParserRuleContext {
		public TerminalNode K_QUIT() { return getToken(SQLParser.K_QUIT, 0); }
		public QuitStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quitStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterQuitStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitQuitStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitQuitStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuitStmtContext quitStmt() throws RecognitionException {
		QuitStmtContext _localctx = new QuitStmtContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_quitStmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(252);
			match(K_QUIT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ShowTableStmtContext extends ParserRuleContext {
		public TerminalNode K_SHOW() { return getToken(SQLParser.K_SHOW, 0); }
		public TerminalNode K_TABLE() { return getToken(SQLParser.K_TABLE, 0); }
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public ShowTableStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showTableStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterShowTableStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitShowTableStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitShowTableStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ShowTableStmtContext showTableStmt() throws RecognitionException {
		ShowTableStmtContext _localctx = new ShowTableStmtContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_showTableStmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(254);
			match(K_SHOW);
			setState(255);
			match(K_TABLE);
			setState(256);
			tableName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertStmtContext extends ParserRuleContext {
		public TerminalNode K_INSERT() { return getToken(SQLParser.K_INSERT, 0); }
		public TerminalNode K_INTO() { return getToken(SQLParser.K_INTO, 0); }
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public TerminalNode K_VALUES() { return getToken(SQLParser.K_VALUES, 0); }
		public List<ValueEntryContext> valueEntry() {
			return getRuleContexts(ValueEntryContext.class);
		}
		public ValueEntryContext valueEntry(int i) {
			return getRuleContext(ValueEntryContext.class,i);
		}
		public List<ColumnNameContext> columnName() {
			return getRuleContexts(ColumnNameContext.class);
		}
		public ColumnNameContext columnName(int i) {
			return getRuleContext(ColumnNameContext.class,i);
		}
		public InsertStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterInsertStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitInsertStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitInsertStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InsertStmtContext insertStmt() throws RecognitionException {
		InsertStmtContext _localctx = new InsertStmtContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_insertStmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(258);
			match(K_INSERT);
			setState(259);
			match(K_INTO);
			setState(260);
			tableName();
			setState(272);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(261);
				match(T__1);
				setState(262);
				columnName();
				setState(267);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(263);
					match(T__2);
					setState(264);
					columnName();
					}
					}
					setState(269);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(270);
				match(T__3);
				}
			}

			setState(274);
			match(K_VALUES);
			setState(275);
			valueEntry();
			setState(280);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(276);
				match(T__2);
				setState(277);
				valueEntry();
				}
				}
				setState(282);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueEntryContext extends ParserRuleContext {
		public List<LiteralValueContext> literalValue() {
			return getRuleContexts(LiteralValueContext.class);
		}
		public LiteralValueContext literalValue(int i) {
			return getRuleContext(LiteralValueContext.class,i);
		}
		public ValueEntryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueEntry; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterValueEntry(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitValueEntry(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitValueEntry(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueEntryContext valueEntry() throws RecognitionException {
		ValueEntryContext _localctx = new ValueEntryContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_valueEntry);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(283);
			match(T__1);
			setState(284);
			literalValue();
			setState(289);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(285);
				match(T__2);
				setState(286);
				literalValue();
				}
				}
				setState(291);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(292);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectStmtContext extends ParserRuleContext {
		public TerminalNode K_SELECT() { return getToken(SQLParser.K_SELECT, 0); }
		public List<ResultColumnContext> resultColumn() {
			return getRuleContexts(ResultColumnContext.class);
		}
		public ResultColumnContext resultColumn(int i) {
			return getRuleContext(ResultColumnContext.class,i);
		}
		public TerminalNode K_FROM() { return getToken(SQLParser.K_FROM, 0); }
		public List<TableQueryContext> tableQuery() {
			return getRuleContexts(TableQueryContext.class);
		}
		public TableQueryContext tableQuery(int i) {
			return getRuleContext(TableQueryContext.class,i);
		}
		public TerminalNode K_WHERE() { return getToken(SQLParser.K_WHERE, 0); }
		public MultipleConditionContext multipleCondition() {
			return getRuleContext(MultipleConditionContext.class,0);
		}
		public TerminalNode K_DISTINCT() { return getToken(SQLParser.K_DISTINCT, 0); }
		public TerminalNode K_ALL() { return getToken(SQLParser.K_ALL, 0); }
		public SelectStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterSelectStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitSelectStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitSelectStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectStmtContext selectStmt() throws RecognitionException {
		SelectStmtContext _localctx = new SelectStmtContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_selectStmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(294);
			match(K_SELECT);
			setState(296);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_ALL || _la==K_DISTINCT) {
				{
				setState(295);
				_la = _input.LA(1);
				if ( !(_la==K_ALL || _la==K_DISTINCT) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(298);
			resultColumn();
			setState(303);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(299);
				match(T__2);
				setState(300);
				resultColumn();
				}
				}
				setState(305);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(306);
			match(K_FROM);
			setState(307);
			tableQuery();
			setState(312);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(308);
				match(T__2);
				setState(309);
				tableQuery();
				}
				}
				setState(314);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(317);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WHERE) {
				{
				setState(315);
				match(K_WHERE);
				setState(316);
				multipleCondition(0);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateViewStmtContext extends ParserRuleContext {
		public TerminalNode K_CREATE() { return getToken(SQLParser.K_CREATE, 0); }
		public TerminalNode K_VIEW() { return getToken(SQLParser.K_VIEW, 0); }
		public ViewNameContext viewName() {
			return getRuleContext(ViewNameContext.class,0);
		}
		public TerminalNode K_AS() { return getToken(SQLParser.K_AS, 0); }
		public SelectStmtContext selectStmt() {
			return getRuleContext(SelectStmtContext.class,0);
		}
		public CreateViewStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createViewStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterCreateViewStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitCreateViewStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitCreateViewStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateViewStmtContext createViewStmt() throws RecognitionException {
		CreateViewStmtContext _localctx = new CreateViewStmtContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_createViewStmt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(319);
			match(K_CREATE);
			setState(320);
			match(K_VIEW);
			setState(321);
			viewName();
			setState(322);
			match(K_AS);
			setState(323);
			selectStmt();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DropViewStmtContext extends ParserRuleContext {
		public TerminalNode K_DROP() { return getToken(SQLParser.K_DROP, 0); }
		public TerminalNode K_VIEW() { return getToken(SQLParser.K_VIEW, 0); }
		public ViewNameContext viewName() {
			return getRuleContext(ViewNameContext.class,0);
		}
		public TerminalNode K_IF() { return getToken(SQLParser.K_IF, 0); }
		public TerminalNode K_EXISTS() { return getToken(SQLParser.K_EXISTS, 0); }
		public DropViewStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropViewStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterDropViewStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitDropViewStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitDropViewStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropViewStmtContext dropViewStmt() throws RecognitionException {
		DropViewStmtContext _localctx = new DropViewStmtContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_dropViewStmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(325);
			match(K_DROP);
			setState(326);
			match(K_VIEW);
			setState(329);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_IF) {
				{
				setState(327);
				match(K_IF);
				setState(328);
				match(K_EXISTS);
				}
			}

			setState(331);
			viewName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UpdateStmtContext extends ParserRuleContext {
		public TerminalNode K_UPDATE() { return getToken(SQLParser.K_UPDATE, 0); }
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public TerminalNode K_SET() { return getToken(SQLParser.K_SET, 0); }
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SQLParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode K_WHERE() { return getToken(SQLParser.K_WHERE, 0); }
		public MultipleConditionContext multipleCondition() {
			return getRuleContext(MultipleConditionContext.class,0);
		}
		public UpdateStmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_updateStmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterUpdateStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitUpdateStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitUpdateStmt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UpdateStmtContext updateStmt() throws RecognitionException {
		UpdateStmtContext _localctx = new UpdateStmtContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_updateStmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(333);
			match(K_UPDATE);
			setState(334);
			tableName();
			setState(335);
			match(K_SET);
			setState(336);
			columnName();
			setState(337);
			match(EQ);
			setState(338);
			expression(0);
			setState(341);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==K_WHERE) {
				{
				setState(339);
				match(K_WHERE);
				setState(340);
				multipleCondition(0);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnDefContext extends ParserRuleContext {
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public TypeNameContext typeName() {
			return getRuleContext(TypeNameContext.class,0);
		}
		public List<ColumnConstraintContext> columnConstraint() {
			return getRuleContexts(ColumnConstraintContext.class);
		}
		public ColumnConstraintContext columnConstraint(int i) {
			return getRuleContext(ColumnConstraintContext.class,i);
		}
		public ColumnDefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnDef; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterColumnDef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitColumnDef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitColumnDef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnDefContext columnDef() throws RecognitionException {
		ColumnDefContext _localctx = new ColumnDefContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_columnDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(343);
			columnName();
			setState(344);
			typeName();
			setState(348);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==K_NOT || _la==K_PRIMARY) {
				{
				{
				setState(345);
				columnConstraint();
				}
				}
				setState(350);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TypeNameContext extends ParserRuleContext {
		public TerminalNode T_INT() { return getToken(SQLParser.T_INT, 0); }
		public TerminalNode T_LONG() { return getToken(SQLParser.T_LONG, 0); }
		public TerminalNode T_FLOAT() { return getToken(SQLParser.T_FLOAT, 0); }
		public TerminalNode T_DOUBLE() { return getToken(SQLParser.T_DOUBLE, 0); }
		public TerminalNode T_STRING() { return getToken(SQLParser.T_STRING, 0); }
		public TerminalNode NUMERIC_LITERAL() { return getToken(SQLParser.NUMERIC_LITERAL, 0); }
		public TypeNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_typeName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterTypeName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitTypeName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitTypeName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypeNameContext typeName() throws RecognitionException {
		TypeNameContext _localctx = new TypeNameContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_typeName);
		try {
			setState(359);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T_INT:
				enterOuterAlt(_localctx, 1);
				{
				setState(351);
				match(T_INT);
				}
				break;
			case T_LONG:
				enterOuterAlt(_localctx, 2);
				{
				setState(352);
				match(T_LONG);
				}
				break;
			case T_FLOAT:
				enterOuterAlt(_localctx, 3);
				{
				setState(353);
				match(T_FLOAT);
				}
				break;
			case T_DOUBLE:
				enterOuterAlt(_localctx, 4);
				{
				setState(354);
				match(T_DOUBLE);
				}
				break;
			case T_STRING:
				enterOuterAlt(_localctx, 5);
				{
				setState(355);
				match(T_STRING);
				setState(356);
				match(T__1);
				setState(357);
				match(NUMERIC_LITERAL);
				setState(358);
				match(T__3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnConstraintContext extends ParserRuleContext {
		public TerminalNode K_PRIMARY() { return getToken(SQLParser.K_PRIMARY, 0); }
		public TerminalNode K_KEY() { return getToken(SQLParser.K_KEY, 0); }
		public TerminalNode K_NOT() { return getToken(SQLParser.K_NOT, 0); }
		public TerminalNode K_NULL() { return getToken(SQLParser.K_NULL, 0); }
		public ColumnConstraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnConstraint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterColumnConstraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitColumnConstraint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitColumnConstraint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnConstraintContext columnConstraint() throws RecognitionException {
		ColumnConstraintContext _localctx = new ColumnConstraintContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_columnConstraint);
		try {
			setState(365);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_PRIMARY:
				enterOuterAlt(_localctx, 1);
				{
				setState(361);
				match(K_PRIMARY);
				setState(362);
				match(K_KEY);
				}
				break;
			case K_NOT:
				enterOuterAlt(_localctx, 2);
				{
				setState(363);
				match(K_NOT);
				setState(364);
				match(K_NULL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultipleConditionContext extends ParserRuleContext {
		public ConditionContext condition() {
			return getRuleContext(ConditionContext.class,0);
		}
		public List<MultipleConditionContext> multipleCondition() {
			return getRuleContexts(MultipleConditionContext.class);
		}
		public MultipleConditionContext multipleCondition(int i) {
			return getRuleContext(MultipleConditionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SQLParser.AND, 0); }
		public TerminalNode OR() { return getToken(SQLParser.OR, 0); }
		public MultipleConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multipleCondition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterMultipleCondition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitMultipleCondition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitMultipleCondition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultipleConditionContext multipleCondition() throws RecognitionException {
		return multipleCondition(0);
	}

	private MultipleConditionContext multipleCondition(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		MultipleConditionContext _localctx = new MultipleConditionContext(_ctx, _parentState);
		MultipleConditionContext _prevctx = _localctx;
		int _startState = 60;
		enterRecursionRule(_localctx, 60, RULE_multipleCondition, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(368);
			condition();
			}
			_ctx.stop = _input.LT(-1);
			setState(378);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,27,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(376);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
					case 1:
						{
						_localctx = new MultipleConditionContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_multipleCondition);
						setState(370);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(371);
						match(AND);
						setState(372);
						multipleCondition(3);
						}
						break;
					case 2:
						{
						_localctx = new MultipleConditionContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_multipleCondition);
						setState(373);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(374);
						match(OR);
						setState(375);
						multipleCondition(2);
						}
						break;
					}
					} 
				}
				setState(380);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,27,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class ConditionContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public ComparatorContext comparator() {
			return getRuleContext(ComparatorContext.class,0);
		}
		public ConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_condition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterCondition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitCondition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitCondition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConditionContext condition() throws RecognitionException {
		ConditionContext _localctx = new ConditionContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_condition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(381);
			expression(0);
			setState(382);
			comparator();
			setState(383);
			expression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComparerContext extends ParserRuleContext {
		public ColumnFullNameContext columnFullName() {
			return getRuleContext(ColumnFullNameContext.class,0);
		}
		public LiteralValueContext literalValue() {
			return getRuleContext(LiteralValueContext.class,0);
		}
		public ComparerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparer; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterComparer(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitComparer(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitComparer(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparerContext comparer() throws RecognitionException {
		ComparerContext _localctx = new ComparerContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_comparer);
		try {
			setState(387);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(385);
				columnFullName();
				}
				break;
			case K_NULL:
			case NUMERIC_LITERAL:
			case STRING_LITERAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(386);
				literalValue();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComparatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(SQLParser.EQ, 0); }
		public TerminalNode NE() { return getToken(SQLParser.NE, 0); }
		public TerminalNode LE() { return getToken(SQLParser.LE, 0); }
		public TerminalNode GE() { return getToken(SQLParser.GE, 0); }
		public TerminalNode LT() { return getToken(SQLParser.LT, 0); }
		public TerminalNode GT() { return getToken(SQLParser.GT, 0); }
		public ComparatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterComparator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitComparator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitComparator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparatorContext comparator() throws RecognitionException {
		ComparatorContext _localctx = new ComparatorContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_comparator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(389);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << NE) | (1L << LT) | (1L << GT) | (1L << LE) | (1L << GE))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public ComparerContext comparer() {
			return getRuleContext(ComparerContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode MUL() { return getToken(SQLParser.MUL, 0); }
		public TerminalNode DIV() { return getToken(SQLParser.DIV, 0); }
		public TerminalNode ADD() { return getToken(SQLParser.ADD, 0); }
		public TerminalNode SUB() { return getToken(SQLParser.SUB, 0); }
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		return expression(0);
	}

	private ExpressionContext expression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
		ExpressionContext _prevctx = _localctx;
		int _startState = 68;
		enterRecursionRule(_localctx, 68, RULE_expression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(397);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case K_NULL:
			case IDENTIFIER:
			case NUMERIC_LITERAL:
			case STRING_LITERAL:
				{
				setState(392);
				comparer();
				}
				break;
			case T__1:
				{
				setState(393);
				match(T__1);
				setState(394);
				expression(0);
				setState(395);
				match(T__3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(407);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,31,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(405);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
					case 1:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(399);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(400);
						_la = _input.LA(1);
						if ( !(_la==MUL || _la==DIV) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(401);
						expression(4);
						}
						break;
					case 2:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(402);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(403);
						_la = _input.LA(1);
						if ( !(_la==ADD || _la==SUB) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(404);
						expression(3);
						}
						break;
					}
					} 
				}
				setState(409);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,31,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class TableConstraintContext extends ParserRuleContext {
		public TerminalNode K_PRIMARY() { return getToken(SQLParser.K_PRIMARY, 0); }
		public TerminalNode K_KEY() { return getToken(SQLParser.K_KEY, 0); }
		public List<ColumnNameContext> columnName() {
			return getRuleContexts(ColumnNameContext.class);
		}
		public ColumnNameContext columnName(int i) {
			return getRuleContext(ColumnNameContext.class,i);
		}
		public TableConstraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableConstraint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterTableConstraint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitTableConstraint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitTableConstraint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableConstraintContext tableConstraint() throws RecognitionException {
		TableConstraintContext _localctx = new TableConstraintContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_tableConstraint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(410);
			match(K_PRIMARY);
			setState(411);
			match(K_KEY);
			setState(412);
			match(T__1);
			setState(413);
			columnName();
			setState(418);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(414);
				match(T__2);
				setState(415);
				columnName();
				}
				}
				setState(420);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(421);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ResultColumnContext extends ParserRuleContext {
		public TerminalNode MUL() { return getToken(SQLParser.MUL, 0); }
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public ColumnFullNameContext columnFullName() {
			return getRuleContext(ColumnFullNameContext.class,0);
		}
		public ResultColumnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_resultColumn; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterResultColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitResultColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitResultColumn(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ResultColumnContext resultColumn() throws RecognitionException {
		ResultColumnContext _localctx = new ResultColumnContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_resultColumn);
		try {
			setState(429);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(423);
				match(MUL);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(424);
				tableName();
				setState(425);
				match(T__4);
				setState(426);
				match(MUL);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(428);
				columnFullName();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableQueryContext extends ParserRuleContext {
		public List<TableNameContext> tableName() {
			return getRuleContexts(TableNameContext.class);
		}
		public TableNameContext tableName(int i) {
			return getRuleContext(TableNameContext.class,i);
		}
		public TerminalNode K_ON() { return getToken(SQLParser.K_ON, 0); }
		public MultipleConditionContext multipleCondition() {
			return getRuleContext(MultipleConditionContext.class,0);
		}
		public List<TerminalNode> K_JOIN() { return getTokens(SQLParser.K_JOIN); }
		public TerminalNode K_JOIN(int i) {
			return getToken(SQLParser.K_JOIN, i);
		}
		public TableQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterTableQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitTableQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitTableQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableQueryContext tableQuery() throws RecognitionException {
		TableQueryContext _localctx = new TableQueryContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_tableQuery);
		int _la;
		try {
			setState(442);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(431);
				tableName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(432);
				tableName();
				setState(435); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(433);
					match(K_JOIN);
					setState(434);
					tableName();
					}
					}
					setState(437); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==K_JOIN );
				setState(439);
				match(K_ON);
				setState(440);
				multipleCondition(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AuthLevelContext extends ParserRuleContext {
		public TerminalNode K_SELECT() { return getToken(SQLParser.K_SELECT, 0); }
		public TerminalNode K_INSERT() { return getToken(SQLParser.K_INSERT, 0); }
		public TerminalNode K_UPDATE() { return getToken(SQLParser.K_UPDATE, 0); }
		public TerminalNode K_DELETE() { return getToken(SQLParser.K_DELETE, 0); }
		public TerminalNode K_DROP() { return getToken(SQLParser.K_DROP, 0); }
		public AuthLevelContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_authLevel; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterAuthLevel(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitAuthLevel(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitAuthLevel(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AuthLevelContext authLevel() throws RecognitionException {
		AuthLevelContext _localctx = new AuthLevelContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_authLevel);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(444);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << K_DELETE) | (1L << K_DROP) | (1L << K_INSERT) | (1L << K_SELECT) | (1L << K_UPDATE))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LiteralValueContext extends ParserRuleContext {
		public TerminalNode NUMERIC_LITERAL() { return getToken(SQLParser.NUMERIC_LITERAL, 0); }
		public TerminalNode STRING_LITERAL() { return getToken(SQLParser.STRING_LITERAL, 0); }
		public TerminalNode K_NULL() { return getToken(SQLParser.K_NULL, 0); }
		public LiteralValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literalValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterLiteralValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitLiteralValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitLiteralValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LiteralValueContext literalValue() throws RecognitionException {
		LiteralValueContext _localctx = new LiteralValueContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_literalValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(446);
			_la = _input.LA(1);
			if ( !(((((_la - 47)) & ~0x3f) == 0 && ((1L << (_la - 47)) & ((1L << (K_NULL - 47)) | (1L << (NUMERIC_LITERAL - 47)) | (1L << (STRING_LITERAL - 47)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnFullNameContext extends ParserRuleContext {
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public ColumnFullNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnFullName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterColumnFullName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitColumnFullName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitColumnFullName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnFullNameContext columnFullName() throws RecognitionException {
		ColumnFullNameContext _localctx = new ColumnFullNameContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_columnFullName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(451);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
			case 1:
				{
				setState(448);
				tableName();
				setState(449);
				match(T__4);
				}
				break;
			}
			setState(453);
			columnName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatabaseNameContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(SQLParser.IDENTIFIER, 0); }
		public DatabaseNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_databaseName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterDatabaseName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitDatabaseName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitDatabaseName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatabaseNameContext databaseName() throws RecognitionException {
		DatabaseNameContext _localctx = new DatabaseNameContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_databaseName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(455);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableNameContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(SQLParser.IDENTIFIER, 0); }
		public TableNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitTableName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitTableName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableNameContext tableName() throws RecognitionException {
		TableNameContext _localctx = new TableNameContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_tableName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(457);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UserNameContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(SQLParser.IDENTIFIER, 0); }
		public UserNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_userName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterUserName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitUserName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitUserName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UserNameContext userName() throws RecognitionException {
		UserNameContext _localctx = new UserNameContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_userName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(459);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnNameContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(SQLParser.IDENTIFIER, 0); }
		public ColumnNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterColumnName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitColumnName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitColumnName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnNameContext columnName() throws RecognitionException {
		ColumnNameContext _localctx = new ColumnNameContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_columnName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(461);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ViewNameContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(SQLParser.IDENTIFIER, 0); }
		public ViewNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_viewName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterViewName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitViewName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitViewName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ViewNameContext viewName() throws RecognitionException {
		ViewNameContext _localctx = new ViewNameContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_viewName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(463);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PasswordContext extends ParserRuleContext {
		public TerminalNode STRING_LITERAL() { return getToken(SQLParser.STRING_LITERAL, 0); }
		public PasswordContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_password; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterPassword(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitPassword(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SQLVisitor ) return ((SQLVisitor<? extends T>)visitor).visitPassword(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PasswordContext password() throws RecognitionException {
		PasswordContext _localctx = new PasswordContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_password);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(465);
			match(STRING_LITERAL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 30:
			return multipleCondition_sempred((MultipleConditionContext)_localctx, predIndex);
		case 34:
			return expression_sempred((ExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean multipleCondition_sempred(MultipleConditionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 2);
		case 1:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return precpred(_ctx, 3);
		case 3:
			return precpred(_ctx, 2);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3J\u01d6\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\3\2\3\2\3\3\7\3d\n\3\f\3\16\3g\13\3\3"+
		"\3\3\3\6\3k\n\3\r\3\16\3l\3\3\7\3p\n\3\f\3\16\3s\13\3\3\3\7\3v\n\3\f\3"+
		"\16\3y\13\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3"+
		"\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4\u0092\n\4\3\5\3\5\3\5\3\5\3\6\3"+
		"\6\3\6\3\6\5\6\u009c\n\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3"+
		"\b\3\b\5\b\u00ab\n\b\3\b\3\b\3\t\3\t\3\n\3\n\3\n\3\13\3\13\3\13\3\f\3"+
		"\f\3\f\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\16\3\16\7\16\u00c3\n\16\f\16"+
		"\16\16\u00c6\13\16\3\16\3\16\5\16\u00ca\n\16\3\16\3\16\3\17\3\17\3\17"+
		"\3\17\7\17\u00d2\n\17\f\17\16\17\u00d5\13\17\3\17\3\17\3\17\3\17\3\17"+
		"\3\20\3\20\3\20\3\20\7\20\u00e0\n\20\f\20\16\20\u00e3\13\20\3\20\3\20"+
		"\3\20\3\20\3\20\3\21\3\21\3\21\3\22\3\22\3\22\3\22\3\22\5\22\u00f2\n\22"+
		"\3\23\3\23\3\23\3\23\5\23\u00f8\n\23\3\23\3\23\3\24\3\24\3\24\3\25\3\25"+
		"\3\26\3\26\3\26\3\26\3\27\3\27\3\27\3\27\3\27\3\27\3\27\7\27\u010c\n\27"+
		"\f\27\16\27\u010f\13\27\3\27\3\27\5\27\u0113\n\27\3\27\3\27\3\27\3\27"+
		"\7\27\u0119\n\27\f\27\16\27\u011c\13\27\3\30\3\30\3\30\3\30\7\30\u0122"+
		"\n\30\f\30\16\30\u0125\13\30\3\30\3\30\3\31\3\31\5\31\u012b\n\31\3\31"+
		"\3\31\3\31\7\31\u0130\n\31\f\31\16\31\u0133\13\31\3\31\3\31\3\31\3\31"+
		"\7\31\u0139\n\31\f\31\16\31\u013c\13\31\3\31\3\31\5\31\u0140\n\31\3\32"+
		"\3\32\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3\33\5\33\u014c\n\33\3\33\3\33"+
		"\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\5\34\u0158\n\34\3\35\3\35\3\35"+
		"\7\35\u015d\n\35\f\35\16\35\u0160\13\35\3\36\3\36\3\36\3\36\3\36\3\36"+
		"\3\36\3\36\5\36\u016a\n\36\3\37\3\37\3\37\3\37\5\37\u0170\n\37\3 \3 \3"+
		" \3 \3 \3 \3 \3 \3 \7 \u017b\n \f \16 \u017e\13 \3!\3!\3!\3!\3\"\3\"\5"+
		"\"\u0186\n\"\3#\3#\3$\3$\3$\3$\3$\3$\5$\u0190\n$\3$\3$\3$\3$\3$\3$\7$"+
		"\u0198\n$\f$\16$\u019b\13$\3%\3%\3%\3%\3%\3%\7%\u01a3\n%\f%\16%\u01a6"+
		"\13%\3%\3%\3&\3&\3&\3&\3&\3&\5&\u01b0\n&\3\'\3\'\3\'\3\'\6\'\u01b6\n\'"+
		"\r\'\16\'\u01b7\3\'\3\'\3\'\5\'\u01bd\n\'\3(\3(\3)\3)\3*\3*\3*\5*\u01c6"+
		"\n*\3*\3*\3+\3+\3,\3,\3-\3-\3.\3.\3/\3/\3\60\3\60\3\60\2\4>F\61\2\4\6"+
		"\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRT"+
		"VXZ\\^\2\b\4\2\32\32%%\3\2\b\r\3\2\20\21\3\2\16\17\7\2$$&&,,88>>\5\2\61"+
		"\61EEGG\2\u01e4\2`\3\2\2\2\4e\3\2\2\2\6\u0091\3\2\2\2\b\u0093\3\2\2\2"+
		"\n\u0097\3\2\2\2\f\u009f\3\2\2\2\16\u00a6\3\2\2\2\20\u00ae\3\2\2\2\22"+
		"\u00b0\3\2\2\2\24\u00b3\3\2\2\2\26\u00b6\3\2\2\2\30\u00b9\3\2\2\2\32\u00bb"+
		"\3\2\2\2\34\u00cd\3\2\2\2\36\u00db\3\2\2\2 \u00e9\3\2\2\2\"\u00ec\3\2"+
		"\2\2$\u00f3\3\2\2\2&\u00fb\3\2\2\2(\u00fe\3\2\2\2*\u0100\3\2\2\2,\u0104"+
		"\3\2\2\2.\u011d\3\2\2\2\60\u0128\3\2\2\2\62\u0141\3\2\2\2\64\u0147\3\2"+
		"\2\2\66\u014f\3\2\2\28\u0159\3\2\2\2:\u0169\3\2\2\2<\u016f\3\2\2\2>\u0171"+
		"\3\2\2\2@\u017f\3\2\2\2B\u0185\3\2\2\2D\u0187\3\2\2\2F\u018f\3\2\2\2H"+
		"\u019c\3\2\2\2J\u01af\3\2\2\2L\u01bc\3\2\2\2N\u01be\3\2\2\2P\u01c0\3\2"+
		"\2\2R\u01c5\3\2\2\2T\u01c9\3\2\2\2V\u01cb\3\2\2\2X\u01cd\3\2\2\2Z\u01cf"+
		"\3\2\2\2\\\u01d1\3\2\2\2^\u01d3\3\2\2\2`a\5\4\3\2a\3\3\2\2\2bd\7\3\2\2"+
		"cb\3\2\2\2dg\3\2\2\2ec\3\2\2\2ef\3\2\2\2fh\3\2\2\2ge\3\2\2\2hq\5\6\4\2"+
		"ik\7\3\2\2ji\3\2\2\2kl\3\2\2\2lj\3\2\2\2lm\3\2\2\2mn\3\2\2\2np\5\6\4\2"+
		"oj\3\2\2\2ps\3\2\2\2qo\3\2\2\2qr\3\2\2\2rw\3\2\2\2sq\3\2\2\2tv\7\3\2\2"+
		"ut\3\2\2\2vy\3\2\2\2wu\3\2\2\2wx\3\2\2\2x\5\3\2\2\2yw\3\2\2\2z\u0092\5"+
		"\32\16\2{\u0092\5\b\5\2|\u0092\5\f\7\2}\u0092\5\n\6\2~\u0092\5\16\b\2"+
		"\177\u0092\5\20\t\2\u0080\u0092\5\22\n\2\u0081\u0092\5\24\13\2\u0082\u0092"+
		"\5\26\f\2\u0083\u0092\5\30\r\2\u0084\u0092\5\"\22\2\u0085\u0092\5$\23"+
		"\2\u0086\u0092\5,\27\2\u0087\u0092\5\60\31\2\u0088\u0092\5\62\32\2\u0089"+
		"\u0092\5\64\33\2\u008a\u0092\5\34\17\2\u008b\u0092\5\36\20\2\u008c\u0092"+
		"\5 \21\2\u008d\u0092\5&\24\2\u008e\u0092\5*\26\2\u008f\u0092\5(\25\2\u0090"+
		"\u0092\5\66\34\2\u0091z\3\2\2\2\u0091{\3\2\2\2\u0091|\3\2\2\2\u0091}\3"+
		"\2\2\2\u0091~\3\2\2\2\u0091\177\3\2\2\2\u0091\u0080\3\2\2\2\u0091\u0081"+
		"\3\2\2\2\u0091\u0082\3\2\2\2\u0091\u0083\3\2\2\2\u0091\u0084\3\2\2\2\u0091"+
		"\u0085\3\2\2\2\u0091\u0086\3\2\2\2\u0091\u0087\3\2\2\2\u0091\u0088\3\2"+
		"\2\2\u0091\u0089\3\2\2\2\u0091\u008a\3\2\2\2\u0091\u008b\3\2\2\2\u0091"+
		"\u008c\3\2\2\2\u0091\u008d\3\2\2\2\u0091\u008e\3\2\2\2\u0091\u008f\3\2"+
		"\2\2\u0091\u0090\3\2\2\2\u0092\7\3\2\2\2\u0093\u0094\7!\2\2\u0094\u0095"+
		"\7\"\2\2\u0095\u0096\5T+\2\u0096\t\3\2\2\2\u0097\u0098\7&\2\2\u0098\u009b"+
		"\7\"\2\2\u0099\u009a\7*\2\2\u009a\u009c\7\'\2\2\u009b\u0099\3\2\2\2\u009b"+
		"\u009c\3\2\2\2\u009c\u009d\3\2\2\2\u009d\u009e\5T+\2\u009e\13\3\2\2\2"+
		"\u009f\u00a0\7!\2\2\u00a0\u00a1\7@\2\2\u00a1\u00a2\5X-\2\u00a2\u00a3\7"+
		"+\2\2\u00a3\u00a4\7\35\2\2\u00a4\u00a5\5^\60\2\u00a5\r\3\2\2\2\u00a6\u00a7"+
		"\7&\2\2\u00a7\u00aa\7@\2\2\u00a8\u00a9\7*\2\2\u00a9\u00ab\7\'\2\2\u00aa"+
		"\u00a8\3\2\2\2\u00aa\u00ab\3\2\2\2\u00ab\u00ac\3\2\2\2\u00ac\u00ad\5X"+
		"-\2\u00ad\17\3\2\2\2\u00ae\u00af\7 \2\2\u00af\21\3\2\2\2\u00b0\u00b1\7"+
		"\34\2\2\u00b1\u00b2\7=\2\2\u00b2\23\3\2\2\2\u00b3\u00b4\7\66\2\2\u00b4"+
		"\u00b5\7D\2\2\u00b5\25\3\2\2\2\u00b6\u00b7\7\67\2\2\u00b7\u00b8\7D\2\2"+
		"\u00b8\27\3\2\2\2\u00b9\u00ba\7\36\2\2\u00ba\31\3\2\2\2\u00bb\u00bc\7"+
		"!\2\2\u00bc\u00bd\7;\2\2\u00bd\u00be\5V,\2\u00be\u00bf\7\4\2\2\u00bf\u00c4"+
		"\58\35\2\u00c0\u00c1\7\5\2\2\u00c1\u00c3\58\35\2\u00c2\u00c0\3\2\2\2\u00c3"+
		"\u00c6\3\2\2\2\u00c4\u00c2\3\2\2\2\u00c4\u00c5\3\2\2\2\u00c5\u00c9\3\2"+
		"\2\2\u00c6\u00c4\3\2\2\2\u00c7\u00c8\7\5\2\2\u00c8\u00ca\5H%\2\u00c9\u00c7"+
		"\3\2\2\2\u00c9\u00ca\3\2\2\2\u00ca\u00cb\3\2\2\2\u00cb\u00cc\7\6\2\2\u00cc"+
		"\33\3\2\2\2\u00cd\u00ce\7)\2\2\u00ce\u00d3\5N(\2\u00cf\u00d0\7\5\2\2\u00d0"+
		"\u00d2\5N(\2\u00d1\u00cf\3\2\2\2\u00d2\u00d5\3\2\2\2\u00d3\u00d1\3\2\2"+
		"\2\u00d3\u00d4\3\2\2\2\u00d4\u00d6\3\2\2\2\u00d5\u00d3\3\2\2\2\u00d6\u00d7"+
		"\7\62\2\2\u00d7\u00d8\5V,\2\u00d8\u00d9\7<\2\2\u00d9\u00da\5X-\2\u00da"+
		"\35\3\2\2\2\u00db\u00dc\7\65\2\2\u00dc\u00e1\5N(\2\u00dd\u00de\7\5\2\2"+
		"\u00de\u00e0\5N(\2\u00df\u00dd\3\2\2\2\u00e0\u00e3\3\2\2\2\u00e1\u00df"+
		"\3\2\2\2\u00e1\u00e2\3\2\2\2\u00e2\u00e4\3\2\2\2\u00e3\u00e1\3\2\2\2\u00e4"+
		"\u00e5\7\62\2\2\u00e5\u00e6\5V,\2\u00e6\u00e7\7(\2\2\u00e7\u00e8\5X-\2"+
		"\u00e8\37\3\2\2\2\u00e9\u00ea\7?\2\2\u00ea\u00eb\5T+\2\u00eb!\3\2\2\2"+
		"\u00ec\u00ed\7$\2\2\u00ed\u00ee\7(\2\2\u00ee\u00f1\5V,\2\u00ef\u00f0\7"+
		"C\2\2\u00f0\u00f2\5> \2\u00f1\u00ef\3\2\2\2\u00f1\u00f2\3\2\2\2\u00f2"+
		"#\3\2\2\2\u00f3\u00f4\7&\2\2\u00f4\u00f7\7;\2\2\u00f5\u00f6\7*\2\2\u00f6"+
		"\u00f8\7\'\2\2\u00f7\u00f5\3\2\2\2\u00f7\u00f8\3\2\2\2\u00f8\u00f9\3\2"+
		"\2\2\u00f9\u00fa\5V,\2\u00fa%\3\2\2\2\u00fb\u00fc\7:\2\2\u00fc\u00fd\7"+
		"#\2\2\u00fd\'\3\2\2\2\u00fe\u00ff\7\64\2\2\u00ff)\3\2\2\2\u0100\u0101"+
		"\7:\2\2\u0101\u0102\7;\2\2\u0102\u0103\5V,\2\u0103+\3\2\2\2\u0104\u0105"+
		"\7,\2\2\u0105\u0106\7-\2\2\u0106\u0112\5V,\2\u0107\u0108\7\4\2\2\u0108"+
		"\u010d\5Z.\2\u0109\u010a\7\5\2\2\u010a\u010c\5Z.\2\u010b\u0109\3\2\2\2"+
		"\u010c\u010f\3\2\2\2\u010d\u010b\3\2\2\2\u010d\u010e\3\2\2\2\u010e\u0110"+
		"\3\2\2\2\u010f\u010d\3\2\2\2\u0110\u0111\7\6\2\2\u0111\u0113\3\2\2\2\u0112"+
		"\u0107\3\2\2\2\u0112\u0113\3\2\2\2\u0113\u0114\3\2\2\2\u0114\u0115\7A"+
		"\2\2\u0115\u011a\5.\30\2\u0116\u0117\7\5\2\2\u0117\u0119\5.\30\2\u0118"+
		"\u0116\3\2\2\2\u0119\u011c\3\2\2\2\u011a\u0118\3\2\2\2\u011a\u011b\3\2"+
		"\2\2\u011b-\3\2\2\2\u011c\u011a\3\2\2\2\u011d\u011e\7\4\2\2\u011e\u0123"+
		"\5P)\2\u011f\u0120\7\5\2\2\u0120\u0122\5P)\2\u0121\u011f\3\2\2\2\u0122"+
		"\u0125\3\2\2\2\u0123\u0121\3\2\2\2\u0123\u0124\3\2\2\2\u0124\u0126\3\2"+
		"\2\2\u0125\u0123\3\2\2\2\u0126\u0127\7\6\2\2\u0127/\3\2\2\2\u0128\u012a"+
		"\78\2\2\u0129\u012b\t\2\2\2\u012a\u0129\3\2\2\2\u012a\u012b\3\2\2\2\u012b"+
		"\u012c\3\2\2\2\u012c\u0131\5J&\2\u012d\u012e\7\5\2\2\u012e\u0130\5J&\2"+
		"\u012f\u012d\3\2\2\2\u0130\u0133\3\2\2\2\u0131\u012f\3\2\2\2\u0131\u0132"+
		"\3\2\2\2\u0132\u0134\3\2\2\2\u0133\u0131\3\2\2\2\u0134\u0135\7(\2\2\u0135"+
		"\u013a\5L\'\2\u0136\u0137\7\5\2\2\u0137\u0139\5L\'\2\u0138\u0136\3\2\2"+
		"\2\u0139\u013c\3\2\2\2\u013a\u0138\3\2\2\2\u013a\u013b\3\2\2\2\u013b\u013f"+
		"\3\2\2\2\u013c\u013a\3\2\2\2\u013d\u013e\7C\2\2\u013e\u0140\5> \2\u013f"+
		"\u013d\3\2\2\2\u013f\u0140\3\2\2\2\u0140\61\3\2\2\2\u0141\u0142\7!\2\2"+
		"\u0142\u0143\7B\2\2\u0143\u0144\5\\/\2\u0144\u0145\7\33\2\2\u0145\u0146"+
		"\5\60\31\2\u0146\63\3\2\2\2\u0147\u0148\7&\2\2\u0148\u014b\7B\2\2\u0149"+
		"\u014a\7*\2\2\u014a\u014c\7\'\2\2\u014b\u0149\3\2\2\2\u014b\u014c\3\2"+
		"\2\2\u014c\u014d\3\2\2\2\u014d\u014e\5\\/\2\u014e\65\3\2\2\2\u014f\u0150"+
		"\7>\2\2\u0150\u0151\5V,\2\u0151\u0152\79\2\2\u0152\u0153\5Z.\2\u0153\u0154"+
		"\7\b\2\2\u0154\u0157\5F$\2\u0155\u0156\7C\2\2\u0156\u0158\5> \2\u0157"+
		"\u0155\3\2\2\2\u0157\u0158\3\2\2\2\u0158\67\3\2\2\2\u0159\u015a\5Z.\2"+
		"\u015a\u015e\5:\36\2\u015b\u015d\5<\37\2\u015c\u015b\3\2\2\2\u015d\u0160"+
		"\3\2\2\2\u015e\u015c\3\2\2\2\u015e\u015f\3\2\2\2\u015f9\3\2\2\2\u0160"+
		"\u015e\3\2\2\2\u0161\u016a\7\24\2\2\u0162\u016a\7\25\2\2\u0163\u016a\7"+
		"\26\2\2\u0164\u016a\7\27\2\2\u0165\u0166\7\30\2\2\u0166\u0167\7\4\2\2"+
		"\u0167\u0168\7E\2\2\u0168\u016a\7\6\2\2\u0169\u0161\3\2\2\2\u0169\u0162"+
		"\3\2\2\2\u0169\u0163\3\2\2\2\u0169\u0164\3\2\2\2\u0169\u0165\3\2\2\2\u016a"+
		";\3\2\2\2\u016b\u016c\7\63\2\2\u016c\u0170\7/\2\2\u016d\u016e\7\60\2\2"+
		"\u016e\u0170\7\61\2\2\u016f\u016b\3\2\2\2\u016f\u016d\3\2\2\2\u0170=\3"+
		"\2\2\2\u0171\u0172\b \1\2\u0172\u0173\5@!\2\u0173\u017c\3\2\2\2\u0174"+
		"\u0175\f\4\2\2\u0175\u0176\7\22\2\2\u0176\u017b\5> \5\u0177\u0178\f\3"+
		"\2\2\u0178\u0179\7\23\2\2\u0179\u017b\5> \4\u017a\u0174\3\2\2\2\u017a"+
		"\u0177\3\2\2\2\u017b\u017e\3\2\2\2\u017c\u017a\3\2\2\2\u017c\u017d\3\2"+
		"\2\2\u017d?\3\2\2\2\u017e\u017c\3\2\2\2\u017f\u0180\5F$\2\u0180\u0181"+
		"\5D#\2\u0181\u0182\5F$\2\u0182A\3\2\2\2\u0183\u0186\5R*\2\u0184\u0186"+
		"\5P)\2\u0185\u0183\3\2\2\2\u0185\u0184\3\2\2\2\u0186C\3\2\2\2\u0187\u0188"+
		"\t\3\2\2\u0188E\3\2\2\2\u0189\u018a\b$\1\2\u018a\u0190\5B\"\2\u018b\u018c"+
		"\7\4\2\2\u018c\u018d\5F$\2\u018d\u018e\7\6\2\2\u018e\u0190\3\2\2\2\u018f"+
		"\u0189\3\2\2\2\u018f\u018b\3\2\2\2\u0190\u0199\3\2\2\2\u0191\u0192\f\5"+
		"\2\2\u0192\u0193\t\4\2\2\u0193\u0198\5F$\6\u0194\u0195\f\4\2\2\u0195\u0196"+
		"\t\5\2\2\u0196\u0198\5F$\5\u0197\u0191\3\2\2\2\u0197\u0194\3\2\2\2\u0198"+
		"\u019b\3\2\2\2\u0199\u0197\3\2\2\2\u0199\u019a\3\2\2\2\u019aG\3\2\2\2"+
		"\u019b\u0199\3\2\2\2\u019c\u019d\7\63\2\2\u019d\u019e\7/\2\2\u019e\u019f"+
		"\7\4\2\2\u019f\u01a4\5Z.\2\u01a0\u01a1\7\5\2\2\u01a1\u01a3\5Z.\2\u01a2"+
		"\u01a0\3\2\2\2\u01a3\u01a6\3\2\2\2\u01a4\u01a2\3\2\2\2\u01a4\u01a5\3\2"+
		"\2\2\u01a5\u01a7\3\2\2\2\u01a6\u01a4\3\2\2\2\u01a7\u01a8\7\6\2\2\u01a8"+
		"I\3\2\2\2\u01a9\u01b0\7\20\2\2\u01aa\u01ab\5V,\2\u01ab\u01ac\7\7\2\2\u01ac"+
		"\u01ad\7\20\2\2\u01ad\u01b0\3\2\2\2\u01ae\u01b0\5R*\2\u01af\u01a9\3\2"+
		"\2\2\u01af\u01aa\3\2\2\2\u01af\u01ae\3\2\2\2\u01b0K\3\2\2\2\u01b1\u01bd"+
		"\5V,\2\u01b2\u01b5\5V,\2\u01b3\u01b4\7.\2\2\u01b4\u01b6\5V,\2\u01b5\u01b3"+
		"\3\2\2\2\u01b6\u01b7\3\2\2\2\u01b7\u01b5\3\2\2\2\u01b7\u01b8\3\2\2\2\u01b8"+
		"\u01b9\3\2\2\2\u01b9\u01ba\7\62\2\2\u01ba\u01bb\5> \2\u01bb\u01bd\3\2"+
		"\2\2\u01bc\u01b1\3\2\2\2\u01bc\u01b2\3\2\2\2\u01bdM\3\2\2\2\u01be\u01bf"+
		"\t\6\2\2\u01bfO\3\2\2\2\u01c0\u01c1\t\7\2\2\u01c1Q\3\2\2\2\u01c2\u01c3"+
		"\5V,\2\u01c3\u01c4\7\7\2\2\u01c4\u01c6\3\2\2\2\u01c5\u01c2\3\2\2\2\u01c5"+
		"\u01c6\3\2\2\2\u01c6\u01c7\3\2\2\2\u01c7\u01c8\5Z.\2\u01c8S\3\2\2\2\u01c9"+
		"\u01ca\7D\2\2\u01caU\3\2\2\2\u01cb\u01cc\7D\2\2\u01ccW\3\2\2\2\u01cd\u01ce"+
		"\7D\2\2\u01ceY\3\2\2\2\u01cf\u01d0\7D\2\2\u01d0[\3\2\2\2\u01d1\u01d2\7"+
		"D\2\2\u01d2]\3\2\2\2\u01d3\u01d4\7G\2\2\u01d4_\3\2\2\2\'elqw\u0091\u009b"+
		"\u00aa\u00c4\u00c9\u00d3\u00e1\u00f1\u00f7\u010d\u0112\u011a\u0123\u012a"+
		"\u0131\u013a\u013f\u014b\u0157\u015e\u0169\u016f\u017a\u017c\u0185\u018f"+
		"\u0197\u0199\u01a4\u01af\u01b7\u01bc\u01c5";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}