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


import static com.xdev.jadoth.sqlengine.SQL.LANG.DEFAULT_VALUES;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.dot;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.ASEXPRESSION;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.OMITALIAS;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.UNQUALIFIED;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.indent;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.isOmitAlias;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.isSingleLine;
import static com.xdev.jadoth.sqlengine.internal.interfaces.TableExpression.Utils.getAlias;

import com.xdev.jadoth.sqlengine.INSERT;
import com.xdev.jadoth.sqlengine.SELECT;
import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.DbmsConfiguration;
import com.xdev.jadoth.sqlengine.dbms.DbmsSyntax;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineInvalidIdentifier;
import com.xdev.jadoth.sqlengine.internal.AssignmentValuesClause;
import com.xdev.jadoth.sqlengine.internal.QueryPart;
import com.xdev.jadoth.sqlengine.internal.SqlColumn;
import com.xdev.jadoth.sqlengine.internal.SqlIdentifier;
import com.xdev.jadoth.sqlengine.internal.interfaces.TableExpression;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;



public class MySQLDMLAssembler extends StandardDMLAssembler<MySQLDbms>
{
	protected static final char	DELIM	= '`';
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	public MySQLDMLAssembler(final MySQLDbms dbms)
	{
		super(dbms);
	}
	
	
	@Override
	public StringBuilder assembleColumnQualifier(SqlColumn column, StringBuilder sb, int flags)
	{
		final TableExpression owner = column.getOwner();
		String qualifier = getAlias(owner);
		if(qualifier == null || QueryPart.isQualifyByTable(flags))
		{
			qualifier = owner.toString();
		}
		boolean delim = needsDelimiter(qualifier);
		if(delim)
		{
			sb.append(DELIM);
		}
		sb.append(qualifier);
		if(delim)
		{
			sb.append(DELIM);
		}
		sb.append(dot);
		return sb;
	}
	
	
	@Override
	public StringBuilder assembleColumn(SqlColumn column, StringBuilder sb, int indentLevel,
			int flags)
	{
		final TableExpression owner = column.getOwner();
		
		flags |= QueryPart.bitDelimitColumnIdentifiers(this.getDbmsAdaptor().getConfiguration()
				.isDelimitColumnIdentifiers());
		final String columnName = column.getColumnName();
		boolean delim = false;
		if(columnName != null && !"*".equals(columnName))
		{
			if(needsDelimiter(columnName))
			{
				delim = true;
			}
		}
		if(owner != null && !QueryPart.isUnqualified(flags))
		{
			this.assembleColumnQualifier(column,sb,flags);
		}
		if(delim)
		{
			sb.append(DELIM);
		}
		QueryPart.assembleObject(column.getExpressionObject(),this,sb,indentLevel,flags);
		if(delim)
		{
			sb.append(DELIM);
		}
		
		return sb;
	}
	
	
	@Override
	public StringBuilder assembleTableIdentifier(SqlTableIdentity table, StringBuilder sb,
			int indentLevel, int flags)
	{
		final DbmsAdaptor<?> dbms = this.getDbmsAdaptor();
		final DbmsSyntax<?> syntax = dbms.getSyntax();
		final DbmsConfiguration<?> config = dbms.getConfiguration();
		
		final SqlTableIdentity.Sql sql = table.sql();
		final String schema = sql.schema;
		final String name = sql.name;
		
		if(schema != null)
		{
			boolean delim = needsDelimiter(schema);
			if(delim)
			{
				sb.append(DELIM);
			}
			sb.append(schema);
			if(delim)
			{
				sb.append(DELIM);
			}
			sb.append(dot);
		}
		
		boolean delim = needsDelimiter(name);
		if(delim)
		{
			sb.append(DELIM);
		}
		sb.append(name);
		if(delim)
		{
			sb.append(DELIM);
		}
		
		if(!isOmitAlias(flags))
		{
			final String alias = sql.alias;
			if(alias != null && alias.length() > 0)
			{
				sb.append(_);
				if(config.isDelimitAliases() || config.isAutoEscapeReservedWords()
						&& syntax.isReservedWord(alias))
				{
					sb.append(DELIM).append(alias).append(DELIM);
				}
				else
				{
					sb.append(alias);
				}
			}
		}
		return sb;
	}
	
	
	private boolean needsDelimiter(String name)
	{
		if(MySQLDbms.SYNTAX.isKeyword(name))
		{
			return true;
		}
		
		try
		{
			SqlIdentifier.validateIdentifierString(name);
			return false;
		}
		catch(SQLEngineInvalidIdentifier e)
		{
			return true;
		}
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	/**
	 * @param query
	 * @param sb
	 * @param indentLevel
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @return
	 * @see net.jadoth.sqlengine.dbmsAdaptor.standard.StandardDMLAssembler#assembleSELECT(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, int, java.lang.String,
	 *      java.lang.String)
	 */
	@Override
	protected StringBuilder assembleSELECT(final SELECT query, final StringBuilder sb,
			final int indentLevel, final int flags, final String clauseSeperator,
			final String newLine)
	{
		indent(sb,indentLevel,isSingleLine(flags)).append(query.keyword());
		this.assembleSelectDISTINCT(query,sb,indentLevel,flags);
		this.assembleSelectItems(query,sb,flags,indentLevel,newLine);
		this.assembleSelectSqlClauses(query,sb,indentLevel,flags | ASEXPRESSION,clauseSeperator,
				newLine);
		this.assembleAppendSELECTs(query,sb,indentLevel,flags,clauseSeperator,newLine);
		this.assembleSelectRowLimit(query,sb,flags,clauseSeperator,newLine,indentLevel);
		return sb;
	}
	
	
	/**
	 * @param query
	 * @param sb
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @param indentLevel
	 * @return
	 * @see net.jadoth.sqlengine.dbmsAdaptor.standard.StandardDMLAssembler#assembleSelectRowLimit(com.xdev.jadoth.sqlengine.SELECT,
	 *      java.lang.StringBuilder, int, java.lang.String, java.lang.String,
	 *      int)
	 */
	@Override
	protected StringBuilder assembleSelectRowLimit(final SELECT query, final StringBuilder sb,
			final int flags, final String clauseSeperator, final String newLine,
			final int indentLevel)
	{
		final Integer offset = query.getOffsetSkipCount();
		final Integer limit = query.getFetchFirstRowCount();
		
		if(offset != null && limit != null)
		{
			sb.append(newLine).append(clauseSeperator).append("LIMIT ").append(limit)
					.append(" OFFSET ").append(offset);
		}
		else if(limit != null)
		{
			sb.append(newLine).append(clauseSeperator).append("LIMIT ").append(limit);
		}
		return sb;
	}
	
	
	@Override
	protected StringBuilder assembleINSERT(INSERT query, StringBuilder sb, int flags,
			String clauseSeperator, String newLine, int indentLevel)
	{
		indent(sb,indentLevel,isSingleLine(flags)).append(query.keyword()).append(_INTO_);
		
		this.assembleTableIdentifier(query.getTable(),sb,indentLevel,flags | OMITALIAS);
		sb.append(newLine);
		
		this.assembleAssignmentColumnsClause(query,query.getColumnsClause(),sb,indentLevel,flags
				| UNQUALIFIED);
		sb.append(newLine);
		
		final SELECT valueSelect = query.filterSelect();
		if(valueSelect != null)
		{
			sb.append(clauseSeperator);
			QueryPart.assembleObject(valueSelect,this,sb,indentLevel,flags);
		}
		else
		{
			final AssignmentValuesClause values = query.getValuesClause();
			if(values != null)
			{
				this.assembleAssignmentValuesClause(query,values,sb,indentLevel,flags);
			}
			else
			{
				indent(sb,indentLevel,isSingleLine(flags)).append(DEFAULT_VALUES);
			}
		}
		
		return sb;
	}
}
