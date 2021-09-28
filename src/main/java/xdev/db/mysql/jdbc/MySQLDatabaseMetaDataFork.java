package xdev.db.mysql.jdbc;

/*-
 * #%L
 * Sqlengine Database Adapter Mysql 5
 * %%
 * Copyright (C) 2003 - 2021 XDEV Software
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-3.0.html>.
 * #L%
 */


import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;

import com.mysql.jdbc.AssertionFailedException;
import com.mysql.jdbc.ByteArrayRow;
import com.mysql.jdbc.ExceptionInterceptor;
import com.mysql.jdbc.Field;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.MysqlErrorNumbers;
import com.mysql.jdbc.ResultSetImpl;
import com.mysql.jdbc.RowData;
import com.mysql.jdbc.RowDataStatic;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.StatementImpl;
import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.Util;

import xdev.db.DBMetaData.TableInfo;


class MySQLDatabaseMetaDataFork
{
	private final MySQLConnection			conn;
	private final ExceptionInterceptor	exceptionInterceptor;
	private String					database	= null;
	private String					quotedId	= null;
	private final String[]				tables;
	
	
	MySQLDatabaseMetaDataFork(final Connection conn, final String database, final TableInfo[] tables)
	{
		this.conn = getMySQLConnection(conn);
		this.exceptionInterceptor = this.conn.getExceptionInterceptor();
		this.database = database;
		this.quotedId = this.conn.supportsQuotedIdentifiers() ? getIdentifierQuoteString() : "";
		this.tables = new String[tables.length];
		for(int i = 0; i < tables.length; i++)
		{
			this.tables[i] = tables[i].getName();
		}
	}
	
	
	static MySQLConnection getMySQLConnection(Connection sqlCon)
	{
		while(sqlCon instanceof xdev.db.ConnectionWrapper)
		{
			sqlCon = ((xdev.db.ConnectionWrapper)sqlCon).getActualConnection();
		}
		return (MySQLConnection)sqlCon;
	}
	
	
	private String getIdentifierQuoteString()
	{
		if(this.conn.supportsQuotedIdentifiers())
		{
			if(!this.conn.useAnsiQuotedIdentifiers())
			{
				return "`";
			}
			
			return "\"";
		}
		
		return " ";
	}
	
	
	public ResultSet getExportedKeys(final String catalog, final String schema) throws SQLException
	{
		final Field[] fields = createFkMetadataFields();
		
		final ArrayList rows = new ArrayList();
		
		if(conn.versionMeetsMinimum(3,23,0))
		{
			final Statement stmt = this.conn.getMetadataSafeStatement();
			
			try
			{
				new IterateBlock(getCatalogIterator(catalog))
				{
					@Override
					void forEach(final Object catalogStr) throws SQLException
					{
						ResultSet fkresults = null;
						
						try
						{
							
							/*
							 * Get foreign key information for table
							 */
							if(conn.versionMeetsMinimum(3,23,50))
							{
								// we can use 'SHOW CREATE TABLE'
								
								fkresults = extractForeignKeyFromCreateTable(catalogStr.toString(),
										null);
							}
							else
							{
								final StringBuffer queryBuf = new StringBuffer("SHOW TABLE STATUS FROM ");
								queryBuf.append(quotedId);
								queryBuf.append(catalogStr.toString());
								queryBuf.append(quotedId);
								
								fkresults = stmt.executeQuery(queryBuf.toString());
							}
							
							/*
							 * Parse imported foreign key information
							 */
							
							while(fkresults.next())
							{
								final String tableType = fkresults.getString("Type");
								
								if((tableType != null)
										&& (tableType.equalsIgnoreCase("innodb") || tableType
												.equalsIgnoreCase("SUPPORTS_FK")))
								{
									final String comment = fkresults.getString("Comment").trim();
									
									if(comment != null)
									{
										for(final String table : MySQLDatabaseMetaDataFork.this.tables)
										{
											// lower-case table name might be
											// turned on
											final String tableNameWithCase = getTableNameWithCase(table);
											
											final StringTokenizer commentTokens = new StringTokenizer(
													comment,";",false);
											
											if(commentTokens.hasMoreTokens())
											{
												commentTokens.nextToken(); // Skip
												// InnoDB
												// comment
												
												while(commentTokens.hasMoreTokens())
												{
													final String keys = commentTokens.nextToken();
													getExportKeyResults(catalogStr.toString(),
															tableNameWithCase,keys,rows,
															fkresults.getString("Name"));
												}
											}
										}
									}
								}
							}
							
						}
						finally
						{
							if(fkresults != null)
							{
								try
								{
									fkresults.close();
								}
								catch(final SQLException sqlEx)
								{
									AssertionFailedException.shouldNotHappen(sqlEx);
								}
								
								fkresults = null;
							}
						}
					}
				}.doForAll();
			}
			finally
			{
				if(stmt != null)
				{
					stmt.close();
				}
			}
		}
		
		return buildResultSet(fields,rows);
	}
	
	
	private Field[] createFkMetadataFields()
	{
		final Field[] fields = new Field[14];
		fields[0] = newField("","PKTABLE_CAT",Types.CHAR,255);
		fields[1] = newField("","PKTABLE_SCHEM",Types.CHAR,0);
		fields[2] = newField("","PKTABLE_NAME",Types.CHAR,255);
		fields[3] = newField("","PKCOLUMN_NAME",Types.CHAR,32);
		fields[4] = newField("","FKTABLE_CAT",Types.CHAR,255);
		fields[5] = newField("","FKTABLE_SCHEM",Types.CHAR,0);
		fields[6] = newField("","FKTABLE_NAME",Types.CHAR,255);
		fields[7] = newField("","FKCOLUMN_NAME",Types.CHAR,32);
		fields[8] = newField("","KEY_SEQ",Types.SMALLINT,2);
		fields[9] = newField("","UPDATE_RULE",Types.SMALLINT,2);
		fields[10] = newField("","DELETE_RULE",Types.SMALLINT,2);
		fields[11] = newField("","FK_NAME",Types.CHAR,0);
		fields[12] = newField("","PK_NAME",Types.CHAR,0);
		fields[13] = newField("","DEFERRABILITY",Types.SMALLINT,2);
		return fields;
	}
	
	
	private void getExportKeyResults(final String catalog, final String exportingTable, final String keysComment,
			final List tuples, final String fkTableName) throws SQLException
	{
		getResultsImpl(catalog,exportingTable,keysComment,tuples,fkTableName,true);
	}
	
	
	
	class LocalAndReferencedColumns
	{
		String	constraintName;
		
		List	localColumnsList;
		
		String	referencedCatalog;
		
		List	referencedColumnsList;
		
		String	referencedTable;
		
		
		LocalAndReferencedColumns(final List localColumns, final List refColumns, final String constName,
				final String refCatalog, final String refTable)
		{
			this.localColumnsList = localColumns;
			this.referencedColumnsList = refColumns;
			this.constraintName = constName;
			this.referencedTable = refTable;
			this.referencedCatalog = refCatalog;
		}
	}
	
	
	private LocalAndReferencedColumns parseTableStatusIntoLocalAndReferencedColumns(
			String keysComment) throws SQLException
	{
		// keys will equal something like this:
		// (parent_service_id child_service_id) REFER
		// ds/subservices(parent_service_id child_service_id)
		//
		// simple-columned keys: (m) REFER
		// airline/tt(a)
		//
		// multi-columned keys : (m n) REFER
		// airline/vv(a b)
		//
		// parse of the string into three phases:
		// 1: parse the opening parentheses to determine how many results there
		// will be
		// 2: read in the schema name/table name
		// 3: parse the closing parentheses
		
		final String columnsDelimitter = ","; // what version did this change in?
		
		final char quoteChar = this.quotedId.length() == 0 ? 0 : this.quotedId.charAt(0);
		
		final int indexOfOpenParenLocalColumns = StringUtils.indexOfIgnoreCaseRespectQuotes(0,
				keysComment,"(",quoteChar,true);
		
		if(indexOfOpenParenLocalColumns == -1)
		{
			throw SQLError.createSQLException("Error parsing foreign keys definition,"
					+ " couldn't find start of local columns list.",
					SQLError.SQL_STATE_GENERAL_ERROR,exceptionInterceptor);
		}
		
		final String constraintName = removeQuotedId(keysComment
				.substring(0,indexOfOpenParenLocalColumns).trim());
		keysComment = keysComment.substring(indexOfOpenParenLocalColumns,keysComment.length());
		
		final String keysCommentTrimmed = keysComment.trim();
		
		final int indexOfCloseParenLocalColumns = StringUtils.indexOfIgnoreCaseRespectQuotes(0,
				keysCommentTrimmed,")",quoteChar,true);
		
		if(indexOfCloseParenLocalColumns == -1)
		{
			throw SQLError.createSQLException("Error parsing foreign keys definition,"
					+ " couldn't find end of local columns list.",SQLError.SQL_STATE_GENERAL_ERROR,
					exceptionInterceptor);
		}
		
		final String localColumnNamesString = keysCommentTrimmed.substring(1,
				indexOfCloseParenLocalColumns);
		
		final int indexOfRefer = StringUtils.indexOfIgnoreCaseRespectQuotes(0,keysCommentTrimmed,
				"REFER ",this.quotedId.charAt(0),true);
		
		if(indexOfRefer == -1)
		{
			throw SQLError.createSQLException("Error parsing foreign keys definition,"
					+ " couldn't find start of referenced tables list.",
					SQLError.SQL_STATE_GENERAL_ERROR,exceptionInterceptor);
		}
		
		final int indexOfOpenParenReferCol = StringUtils.indexOfIgnoreCaseRespectQuotes(indexOfRefer,
				keysCommentTrimmed,"(",quoteChar,false);
		
		if(indexOfOpenParenReferCol == -1)
		{
			throw SQLError.createSQLException("Error parsing foreign keys definition,"
					+ " couldn't find start of referenced columns list.",
					SQLError.SQL_STATE_GENERAL_ERROR,exceptionInterceptor);
		}
		
		final String referCatalogTableString = keysCommentTrimmed.substring(
				indexOfRefer + "REFER ".length(),indexOfOpenParenReferCol);
		
		final int indexOfSlash = StringUtils.indexOfIgnoreCaseRespectQuotes(0,referCatalogTableString,
				"/",this.quotedId.charAt(0),false);
		
		if(indexOfSlash == -1)
		{
			throw SQLError.createSQLException("Error parsing foreign keys definition,"
					+ " couldn't find name of referenced catalog.",
					SQLError.SQL_STATE_GENERAL_ERROR,exceptionInterceptor);
		}
		
		final String referCatalog = removeQuotedId(referCatalogTableString.substring(0,indexOfSlash));
		final String referTable = removeQuotedId(referCatalogTableString.substring(indexOfSlash + 1)
				.trim());
		
		final int indexOfCloseParenRefer = StringUtils.indexOfIgnoreCaseRespectQuotes(
				indexOfOpenParenReferCol,keysCommentTrimmed,")",quoteChar,true);
		
		if(indexOfCloseParenRefer == -1)
		{
			throw SQLError.createSQLException("Error parsing foreign keys definition,"
					+ " couldn't find end of referenced columns list.",
					SQLError.SQL_STATE_GENERAL_ERROR,exceptionInterceptor);
		}
		
		final String referColumnNamesString = keysCommentTrimmed.substring(indexOfOpenParenReferCol + 1,
				indexOfCloseParenRefer);
		
		final List referColumnsList = StringUtils.split(referColumnNamesString,columnsDelimitter,
				this.quotedId,this.quotedId,false);
		final List localColumnsList = StringUtils.split(localColumnNamesString,columnsDelimitter,
				this.quotedId,this.quotedId,false);
		
		return new LocalAndReferencedColumns(localColumnsList,referColumnsList,constraintName,
				referCatalog,referTable);
	}
	
	private static final int	PKTABLE_CAT		= 0;
	private static final int	PKTABLE_SCHEM	= 1;
	private static final int	PKTABLE_NAME	= 2;
	private static final int	PKCOLUMN_NAME	= 3;
	private static final int	FKTABLE_CAT		= 4;
	private static final int	FKTABLE_SCHEM	= 5;
	private static final int	FKTABLE_NAME	= 6;
	private static final int	FKCOLUMN_NAME	= 7;
	private static final int	KEY_SEQ			= 8;
	private static final int	UPDATE_RULE		= 9;
	private static final int	DELETE_RULE		= 10;
	private static final int	FK_NAME			= 11;
	private static final int	PK_NAME			= 12;
	private static final int	DEFERRABILITY	= 13;
	
	
	private void getResultsImpl(final String catalog, final String table, final String keysComment, final List tuples,
			final String fkTableName, final boolean isExport) throws SQLException
	{
		
		final LocalAndReferencedColumns parsedInfo = parseTableStatusIntoLocalAndReferencedColumns(keysComment);
		
		if(isExport && !parsedInfo.referencedTable.equals(table))
		{
			return;
		}
		
		if(parsedInfo.localColumnsList.size() != parsedInfo.referencedColumnsList.size())
		{
			throw SQLError.createSQLException("Error parsing foreign keys definition,"
					+ "number of local and referenced columns is not the same.",
					SQLError.SQL_STATE_GENERAL_ERROR,exceptionInterceptor);
		}
		
		final Iterator localColumnNames = parsedInfo.localColumnsList.iterator();
		final Iterator referColumnNames = parsedInfo.referencedColumnsList.iterator();
		
		int keySeqIndex = 1;
		
		while(localColumnNames.hasNext())
		{
			final byte[][] tuple = new byte[14][];
			final String lColumnName = removeQuotedId(localColumnNames.next().toString());
			final String rColumnName = removeQuotedId(referColumnNames.next().toString());
			tuple[FKTABLE_CAT] = ((catalog == null) ? new byte[0] : s2b(catalog));
			tuple[FKTABLE_SCHEM] = null;
			tuple[FKTABLE_NAME] = s2b((isExport) ? fkTableName : table);
			tuple[FKCOLUMN_NAME] = s2b(lColumnName);
			tuple[PKTABLE_CAT] = s2b(parsedInfo.referencedCatalog);
			tuple[PKTABLE_SCHEM] = null;
			tuple[PKTABLE_NAME] = s2b((isExport) ? table : parsedInfo.referencedTable);
			tuple[PKCOLUMN_NAME] = s2b(rColumnName);
			tuple[KEY_SEQ] = s2b(Integer.toString(keySeqIndex++));
			
			final int[] actions = getForeignKeyActions(keysComment);
			
			tuple[UPDATE_RULE] = s2b(Integer.toString(actions[1]));
			tuple[DELETE_RULE] = s2b(Integer.toString(actions[0]));
			tuple[FK_NAME] = s2b(parsedInfo.constraintName);
			tuple[PK_NAME] = null; // not available from show table status
			tuple[DEFERRABILITY] = s2b(Integer
					.toString(java.sql.DatabaseMetaData.importedKeyNotDeferrable));
			tuples.add(new ByteArrayRow(tuple,exceptionInterceptor));
		}
	}
	
	
	private int[] getForeignKeyActions(final String commentString)
	{
		final int[] actions = new int[]{java.sql.DatabaseMetaData.importedKeyNoAction,
				java.sql.DatabaseMetaData.importedKeyNoAction};
		
		final int lastParenIndex = commentString.lastIndexOf(")");
		
		if(lastParenIndex != (commentString.length() - 1))
		{
			final String cascadeOptions = commentString.substring(lastParenIndex + 1).trim()
					.toUpperCase(Locale.ENGLISH);
			
			actions[0] = getCascadeDeleteOption(cascadeOptions);
			actions[1] = getCascadeUpdateOption(cascadeOptions);
		}
		
		return actions;
	}
	
	
	private int getCascadeDeleteOption(final String cascadeOptions)
	{
		final int onDeletePos = cascadeOptions.indexOf("ON DELETE");
		
		if(onDeletePos != -1)
		{
			final String deleteOptions = cascadeOptions.substring(onDeletePos,cascadeOptions.length());
			
			if(deleteOptions.startsWith("ON DELETE CASCADE"))
			{
				return java.sql.DatabaseMetaData.importedKeyCascade;
			}
			else if(deleteOptions.startsWith("ON DELETE SET NULL"))
			{
				return java.sql.DatabaseMetaData.importedKeySetNull;
			}
			else if(deleteOptions.startsWith("ON DELETE RESTRICT"))
			{
				return java.sql.DatabaseMetaData.importedKeyRestrict;
			}
			else if(deleteOptions.startsWith("ON DELETE NO ACTION"))
			{
				return java.sql.DatabaseMetaData.importedKeyNoAction;
			}
		}
		
		return java.sql.DatabaseMetaData.importedKeyNoAction;
	}
	
	
	private int getCascadeUpdateOption(final String cascadeOptions)
	{
		final int onUpdatePos = cascadeOptions.indexOf("ON UPDATE");
		
		if(onUpdatePos != -1)
		{
			final String updateOptions = cascadeOptions.substring(onUpdatePos,cascadeOptions.length());
			
			if(updateOptions.startsWith("ON UPDATE CASCADE"))
			{
				return java.sql.DatabaseMetaData.importedKeyCascade;
			}
			else if(updateOptions.startsWith("ON UPDATE SET NULL"))
			{
				return java.sql.DatabaseMetaData.importedKeySetNull;
			}
			else if(updateOptions.startsWith("ON UPDATE RESTRICT"))
			{
				return java.sql.DatabaseMetaData.importedKeyRestrict;
			}
			else if(updateOptions.startsWith("ON UPDATE NO ACTION"))
			{
				return java.sql.DatabaseMetaData.importedKeyNoAction;
			}
		}
		
		return java.sql.DatabaseMetaData.importedKeyNoAction;
	}
	
	
	private String removeQuotedId(String s)
	{
		if(s == null)
		{
			return null;
		}
		
		if(this.quotedId.equals(""))
		{
			return s;
		}
		
		s = s.trim();
		
		int frontOffset = 0;
		int backOffset = s.length();
		final int quoteLength = this.quotedId.length();
		
		if(s.startsWith(this.quotedId))
		{
			frontOffset = quoteLength;
		}
		
		if(s.endsWith(this.quotedId))
		{
			backOffset -= quoteLength;
		}
		
		return s.substring(frontOffset,backOffset);
	}
	
	
	private ResultSet extractForeignKeyFromCreateTable(final String catalog, final String tableName)
			throws SQLException
	{
		final ArrayList tableList = new ArrayList();
		java.sql.Statement stmt = null;
		
		if(tableName != null)
		{
			tableList.add(tableName);
		}
		else
		{
			for(final String table : this.tables)
			{
				tableList.add(table);
			}
		}
		
		final ArrayList rows = new ArrayList();
		final Field[] fields = new Field[3];
		fields[0] = newField("","Name",Types.CHAR,Integer.MAX_VALUE);
		fields[1] = newField("","Type",Types.CHAR,255);
		fields[2] = newField("","Comment",Types.CHAR,Integer.MAX_VALUE);
		
		final int numTables = tableList.size();
		stmt = this.conn.getMetadataSafeStatement();
		
		String quoteChar = getIdentifierQuoteString();
		
		if(quoteChar == null)
		{
			quoteChar = "`";
		}
		
		ResultSet rs = null;
		try
		{
			for(int i = 0; i < numTables; i++)
			{
				String tableToExtract = (String)tableList.get(i);
				if(tableToExtract.indexOf(quoteChar) > 0)
				{
					tableToExtract = StringUtils.escapeQuote(tableToExtract,quoteChar);
				}
				
				final String query = new StringBuffer("SHOW CREATE TABLE ").append(quoteChar)
						.append(catalog).append(quoteChar).append(".").append(quoteChar)
						.append(tableToExtract).append(quoteChar).toString();
				
				try
				{
					rs = stmt.executeQuery(query);
				}
				catch(final SQLException sqlEx)
				{
					// Table might've disappeared on us, not really an error
					final String sqlState = sqlEx.getSQLState();
					
					if(!"42S02".equals(sqlState)
							&& sqlEx.getErrorCode() != MysqlErrorNumbers.ER_NO_SUCH_TABLE)
					{
						throw sqlEx;
					}
					
					continue;
				}
				
				while(rs.next())
				{
					extractForeignKeyForTable(rows,rs,catalog);
				}
			}
		}
		finally
		{
			if(rs != null)
			{
				rs.close();
			}
			
			rs = null;
			
			if(stmt != null)
			{
				stmt.close();
			}
			
			stmt = null;
		}
		
		return buildResultSet(fields,rows);
	}
	
	
	private java.sql.ResultSet buildResultSet(final Field[] fields, final java.util.ArrayList rows)
			throws SQLException
	{
		return buildResultSet(fields,rows,this.conn);
	}
	
	
	static java.sql.ResultSet buildResultSet(final Field[] fields, final java.util.ArrayList rows,
			final MySQLConnection c) throws SQLException
	{
		final int fieldsLength = fields.length;
		
		for(int i = 0; i < fieldsLength; i++) 
		{
			final int jdbcType = fields[i].getSQLType();
			
			switch(jdbcType)
			{
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
					fields[i].setCharacterSet(c.getCharacterSetMetadata());
				break;
				default:
					// do nothing
			}
			
			fields[i].setConnection(c);
			
			try
			{
				// fields[i].setUseOldNameMetadata(true);
				final Method method = fields[i].getClass().getDeclaredMethod("setUseOldNameMetadata",
						boolean.class);
				method.setAccessible(true);
				method.invoke(fields[i],true);
			}
			catch(final Exception e)
			{
				e.printStackTrace();
			}
		}
		
		return getResultSetInstance(c.getCatalog(),fields,new RowDataStatic(rows),c,null);
	}
	
	private static Constructor<?>	JDBC_4_RS_6_ARG_CTOR;
	static
	{
		try
		{
			JDBC_4_RS_6_ARG_CTOR = Class.forName("com.mysql.jdbc.JDBC4ResultSet").getConstructor(
					new Class[]{String.class,Field[].class,RowData.class,MySQLConnection.class,
			com.mysql.jdbc.StatementImpl.class});
		}
		catch(final Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	protected static ResultSetImpl getResultSetInstance(final String catalog, final Field[] fields,
			final RowData tuples, final MySQLConnection conn, final StatementImpl creatorStmt) throws SQLException
	{
		if(!Util.isJdbc4())
		{
			return new ResultSetImpl(catalog,fields,tuples,conn,creatorStmt);
		}
		
		return (ResultSetImpl)Util.handleNewInstance(JDBC_4_RS_6_ARG_CTOR,new Object[]{catalog,
				fields,tuples,conn,creatorStmt},conn.getExceptionInterceptor());
	}
	
	
	private List extractForeignKeyForTable(final ArrayList rows, final java.sql.ResultSet rs, final String catalog)
			throws SQLException
	{
		final byte[][] row = new byte[3][];
		row[0] = rs.getBytes(1);
		row[1] = s2b("SUPPORTS_FK");
		
		final String createTableString = rs.getString(2);
		final StringTokenizer lineTokenizer = new StringTokenizer(createTableString,"\n");
		final StringBuffer commentBuf = new StringBuffer("comment; ");
		boolean firstTime = true;
		
		String quoteChar = getIdentifierQuoteString();
		
		if(quoteChar == null)
		{
			quoteChar = "`";
		}
		
		while(lineTokenizer.hasMoreTokens())
		{
			String line = lineTokenizer.nextToken().trim();
			
			String constraintName = null;
			
			if(StringUtils.startsWithIgnoreCase(line,"CONSTRAINT"))
			{
				boolean usingBackTicks = true;
				int beginPos = line.indexOf(quoteChar);
				
				if(beginPos == -1)
				{
					beginPos = line.indexOf("\"");
					usingBackTicks = false;
				}
				
				if(beginPos != -1)
				{
					int endPos = -1;
					
					if(usingBackTicks)
					{
						endPos = line.indexOf(quoteChar,beginPos + 1);
					}
					else
					{
						endPos = line.indexOf("\"",beginPos + 1);
					}
					
					if(endPos != -1)
					{
						constraintName = line.substring(beginPos + 1,endPos);
						line = line.substring(endPos + 1,line.length()).trim();
					}
				}
			}
			
			if(line.startsWith("FOREIGN KEY"))
			{
				if(line.endsWith(","))
				{
					line = line.substring(0,line.length() - 1);
				}
				
				final char quote = this.quotedId.charAt(0);
				
				final int indexOfFK = line.indexOf("FOREIGN KEY");
				
				String localColumnName = null;
				String referencedCatalogName = this.quotedId + catalog + this.quotedId;
				String referencedTableName = null;
				String referencedColumnName = null;
				
				if(indexOfFK != -1)
				{
					final int afterFk = indexOfFK + "FOREIGN KEY".length();
					
					final int indexOfRef = StringUtils.indexOfIgnoreCaseRespectQuotes(afterFk,line,
							"REFERENCES",quote,true);
					
					if(indexOfRef != -1)
					{
						
						final int indexOfParenOpen = line.indexOf('(',afterFk);
						final int indexOfParenClose = StringUtils.indexOfIgnoreCaseRespectQuotes(
								indexOfParenOpen,line,")",quote,true);
						
						if(indexOfParenOpen == -1 || indexOfParenClose == -1)
						{
							// throw SQLError.createSQLException();
						}
						
						localColumnName = line.substring(indexOfParenOpen + 1,indexOfParenClose);
						
						final int afterRef = indexOfRef + "REFERENCES".length();
						
						final int referencedColumnBegin = StringUtils.indexOfIgnoreCaseRespectQuotes(
								afterRef,line,"(",quote,true);
						
						if(referencedColumnBegin != -1)
						{
							referencedTableName = line.substring(afterRef,referencedColumnBegin);
							
							final int referencedColumnEnd = StringUtils.indexOfIgnoreCaseRespectQuotes(
									referencedColumnBegin + 1,line,")",quote,true);
							
							if(referencedColumnEnd != -1)
							{
								referencedColumnName = line.substring(referencedColumnBegin + 1,
										referencedColumnEnd);
							}
							
							final int indexOfCatalogSep = StringUtils.indexOfIgnoreCaseRespectQuotes(0,
									referencedTableName,".",quote,true);
							
							if(indexOfCatalogSep != -1)
							{
								referencedCatalogName = referencedTableName.substring(0,
										indexOfCatalogSep);
								referencedTableName = referencedTableName
										.substring(indexOfCatalogSep + 1);
							}
						}
					}
				}
				
				if(!firstTime)
				{
					commentBuf.append("; ");
				}
				else
				{
					firstTime = false;
				}
				
				if(constraintName != null)
				{
					commentBuf.append(constraintName);
				}
				else
				{
					commentBuf.append("not_available");
				}
				
				commentBuf.append("(");
				commentBuf.append(localColumnName);
				commentBuf.append(") REFER ");
				commentBuf.append(referencedCatalogName);
				commentBuf.append("/");
				commentBuf.append(referencedTableName);
				commentBuf.append("(");
				commentBuf.append(referencedColumnName);
				commentBuf.append(")");
				
				final int lastParenIndex = line.lastIndexOf(")");
				
				if(lastParenIndex != (line.length() - 1))
				{
					final String cascadeOptions = line.substring(lastParenIndex + 1);
					commentBuf.append(" ");
					commentBuf.append(cascadeOptions);
				}
			}
		}
		
		row[2] = s2b(commentBuf.toString());
		rows.add(new ByteArrayRow(row,exceptionInterceptor));
		
		return rows;
	}
	
	
	private byte[] s2b(final String s) throws SQLException
	{
		if(s == null)
		{
			return null;
		}
		
		return StringUtils.getBytes(s,this.conn.getCharacterSetMetadata(),
				this.conn.getServerCharacterEncoding(),this.conn.parserKnowsUnicode(),this.conn,
				exceptionInterceptor);
	}
	
	
	private String getTableNameWithCase(final String table)
	{
		return this.conn.lowerCaseTableNames() ? table.toLowerCase() : table;
	}
	
	
	private IteratorWithCleanup getCatalogIterator(final String catalogSpec) throws SQLException
	{
		IteratorWithCleanup allCatalogsIter;
		if(catalogSpec != null)
		{
			if(!catalogSpec.equals(""))
			{
				allCatalogsIter = new SingleStringIterator(unQuoteQuotedIdentifier(catalogSpec));
			}
			else
			{
				// legacy mode of operation
				allCatalogsIter = new SingleStringIterator(this.database);
			}
		}
		else if(this.conn.getNullCatalogMeansCurrent())
		{
			
			allCatalogsIter = new SingleStringIterator(this.database);
		}
		else
		{
			allCatalogsIter = new ResultSetIterator(getCatalogs(),1);
		}
		
		return allCatalogsIter;
	}
	
	
	private java.sql.ResultSet getCatalogs() throws SQLException
	{
		java.sql.ResultSet results = null;
		java.sql.Statement stmt = null;
		
		try
		{
			stmt = this.conn.createStatement();
			stmt.setEscapeProcessing(false);
			results = stmt.executeQuery("SHOW DATABASES");
			
			final java.sql.ResultSetMetaData resultsMD = results.getMetaData();
			final Field[] fields = new Field[1];
			fields[0] = newField("","TABLE_CAT",Types.VARCHAR,resultsMD.getColumnDisplaySize(1));
			
			final ArrayList tuples = new ArrayList();
			
			while(results.next())
			{
				final byte[][] rowVal = new byte[1][];
				rowVal[0] = results.getBytes(1);
				tuples.add(new ByteArrayRow(rowVal,exceptionInterceptor));
			}
			
			return buildResultSet(fields,tuples);
		}
		finally
		{
			if(results != null)
			{
				try
				{
					results.close();
				}
				catch(final SQLException sqlEx)
				{
					AssertionFailedException.shouldNotHappen(sqlEx);
				}
				
				results = null;
			}
			
			if(stmt != null)
			{
				try
				{
					stmt.close();
				}
				catch(final SQLException sqlEx)
				{
					AssertionFailedException.shouldNotHappen(sqlEx);
				}
				
				stmt = null;
			}
		}
	}
	
	
	private String unQuoteQuotedIdentifier(final String identifier)
	{
		boolean trimQuotes = false;
		
		if(identifier != null)
		{
			
			// Backquotes are always valid identifier quotes
			if(identifier.startsWith("`") && identifier.endsWith("`"))
			{
				trimQuotes = true;
			}
			
			if(this.conn.useAnsiQuotedIdentifiers())
			{
				if(identifier.startsWith("\"") && identifier.endsWith("\""))
				{
					trimQuotes = true;
				}
			}
		}
		
		if(trimQuotes)
		{
			return identifier.substring(1,(identifier.length() - 1));
		}
		
		return identifier;
	}
	
	
	
	private abstract static class IterateBlock
	{
		IteratorWithCleanup	iteratorWithCleanup;
		Iterator			javaIterator;
		boolean				stopIterating	= false;
		
		
		IterateBlock(final IteratorWithCleanup i)
		{
			this.iteratorWithCleanup = i;
			this.javaIterator = null;
		}
		
		
		void doForAll() throws SQLException
		{
			if(this.iteratorWithCleanup != null)
			{
				try
				{
					while(this.iteratorWithCleanup.hasNext())
					{
						forEach(this.iteratorWithCleanup.next());
						
						if(this.stopIterating)
						{
							break;
						}
					}
				}
				finally
				{
					this.iteratorWithCleanup.close();
				}
			}
			else
			{
				while(this.javaIterator.hasNext())
				{
					forEach(this.javaIterator.next());
					
					if(this.stopIterating)
					{
						break;
					}
				}
			}
		}
		
		
		abstract void forEach(Object each) throws SQLException;
	}
	
	
	
	private static abstract class IteratorWithCleanup
	{
		abstract void close() throws SQLException;
		
		
		abstract boolean hasNext() throws SQLException;
		
		
		abstract Object next() throws SQLException;
	}
	
	
	
	private static class SingleStringIterator extends IteratorWithCleanup
	{
		boolean	onFirst	= true;
		
		String	value;
		
		
		SingleStringIterator(final String s)
		{
			value = s;
		}
		
		
		@Override
		void close() throws SQLException
		{
			// not needed
		}
		
		
		@Override
		boolean hasNext() throws SQLException
		{
			return onFirst;
		}
		
		
		@Override
		Object next() throws SQLException
		{
			onFirst = false;
			return value;
		}
	}
	
	
	
	private static class ResultSetIterator extends IteratorWithCleanup
	{
		int			colIndex;
		
		ResultSet	resultSet;
		
		
		ResultSetIterator(final ResultSet rs, final int index)
		{
			resultSet = rs;
			colIndex = index;
		}
		
		
		@Override
		void close() throws SQLException
		{
			resultSet.close();
		}
		
		
		@Override
		boolean hasNext() throws SQLException
		{
			return resultSet.next();
		}
		
		
		@Override
		Object next() throws SQLException
		{
			return resultSet.getObject(colIndex);
		}
	}
	
	private static Constructor<Field>	fieldConstructor;
	static
	{
		try
		{
			fieldConstructor = Field.class.getDeclaredConstructor(String.class,String.class,
					int.class,int.class);
			fieldConstructor.setAccessible(true);
		}
		catch(final Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	private static Field newField(final String tableName, final String columnName, final int jdbcType, final int length)
	{
		try
		{
			return fieldConstructor.newInstance(tableName,columnName,jdbcType,length);
		}
		catch(final Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}
}
