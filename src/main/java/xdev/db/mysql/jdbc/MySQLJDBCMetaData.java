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


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;

import xdev.db.ColumnMetaData;
import xdev.db.DBException;
import xdev.db.DataType;
import xdev.db.Index;
import xdev.db.Index.IndexType;
import xdev.db.Result;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCMetaData;
import xdev.db.jdbc.JDBCResult;
import xdev.util.CollectionUtils;
import xdev.util.ProgressMonitor;
import xdev.vt.Cardinality;
import xdev.vt.EntityRelationship;
import xdev.vt.EntityRelationship.Entity;
import xdev.vt.EntityRelationshipModel;
import xdev.vt.VirtualTable;
import xdev.vt.VirtualTable.VirtualTableRow;


public class MySQLJDBCMetaData extends JDBCMetaData
{
	private static final long	serialVersionUID	= -4935821256152833016L;
	
	
	public MySQLJDBCMetaData(final MySQLJDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	
	@Override
	public TableMetaData[] getTableMetaData(final ProgressMonitor monitor, final int flags, final TableInfo... tables)
			throws DBException
	{
		if(tables == null || tables.length == 0)
		{
			return new TableMetaData[0];
		}
		
		final List<TableMetaData> list = new ArrayList(tables.length);
		
		try
		{
			final JDBCConnection jdbcConnection = (JDBCConnection)dataSource.openConnection();
			
			try
			{
				monitor.beginTask("",ProgressMonitor.UNKNOWN);
				
				final DatabaseMetaData meta = jdbcConnection.getConnection().getMetaData();
				
				final ResultSet rs = meta.getColumns(getCatalog(dataSource),getSchema(dataSource),null,
						null);
				final JDBCResult jrs = new JDBCResult(rs);
				
				// workaround for mysql-jdbc-(bug?)
				// COLUMN_NAME's length is 10, so the names get cut
				jrs.getMetadata(3).setLength(-1);
				
				final VirtualTable vtColumns = new VirtualTable(jrs,true);
				rs.close();
				final Map<String, List<VirtualTableRow>> columnMap = toMap(vtColumns,"TABLE_NAME");
				
				Map<String, List<VirtualTableRow>> indexMap = null;
				if((flags & INDICES) != 0 && !monitor.isCanceled())
				{
					final Result r = jdbcConnection.query(
							"SELECT TABLE_NAME,NON_UNIQUE,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME "
									+ "FROM INFORMATION_SCHEMA.STATISTICS "
									+ "WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME,SEQ_IN_INDEX",
							getCatalog(dataSource));
					
					// workaround for mysql-jdbc-(bug?)
					// COLUMN_NAME's length is 10, so the names get cut
					r.getMetadata(4).setLength(-1);
					
					final VirtualTable vtIndexInfo = new VirtualTable(r,true);
					rs.close();
					indexMap = toMap(vtIndexInfo,"TABLE_NAME");
				}
				
				monitor.beginTask("",tables.length);
				
				int done = 0;
				for(final TableInfo table : tables)
				{
					if(monitor.isCanceled())
					{
						break;
					}
					
					monitor.setTaskName(table.getName());
					try
					{
						list.add(getTableMetaData(jdbcConnection,meta,flags,table,columnMap,
								indexMap));
					}
					catch(final Exception e)
					{
						e.printStackTrace();
					}
					monitor.worked(++done);
				}
			}
			finally
			{
				jdbcConnection.close();
			}
		}
		catch(final SQLException e)
		{
			throw new DBException(dataSource,e);
		}
		
		monitor.done();
		
		return list.toArray(new TableMetaData[list.size()]);
	}
	
	
	protected TableMetaData getTableMetaData(final JDBCConnection jdbcConnection, final DatabaseMetaData meta,
			final int flags, final TableInfo table, final Map<String, List<VirtualTableRow>> columnMap,
			final Map<String, List<VirtualTableRow>> indexMap) throws DBException, SQLException
	{
		final String tableName = table.getName();
		
		final List<VirtualTableRow> columnRows = columnMap.get(tableName.toUpperCase());
		final int columnCount = columnRows.size();
		final ColumnMetaData[] columns = new ColumnMetaData[columnCount];
		for(int i = 0; i < columnCount; i++)
		{
			final VirtualTableRow dataRow = columnRows.get(i);
			final String columnName = (String)dataRow.get("COLUMN_NAME");
			final String caption = null;
			final int sqlType = ((Number)dataRow.get("DATA_TYPE")).intValue();
			final DataType type = DataType.get(sqlType);
			final Object lengthObj = dataRow.get("COLUMN_SIZE");
			final int length = lengthObj instanceof Number ? ((Number)lengthObj).intValue() : 0;
			final Object scaleObj = dataRow.get("DECIMAL_DIGITS");
			int scale = scaleObj instanceof Number ? ((Number)scaleObj).intValue() : 0;
			if(scale < 0)
			{
				scale = 0;
			}
			final String defaultStr = (String)dataRow.get("COLUMN_DEF");
			Object defaultValue = null;
			if(!(defaultStr == null || "null".equals(defaultStr)))
			{
				if(type.isString())
				{
					defaultValue = defaultStr;
				}
				else if(type.isDecimal())
				{
					try
					{
						defaultValue = Double.parseDouble(defaultStr);
					}
					catch(final NumberFormatException e)
					{
						e.printStackTrace();
					}
				}
				else if(type.isInt())
				{
					try
					{
						defaultValue = Integer.parseInt(defaultStr);
					}
					catch(final NumberFormatException e)
					{
						e.printStackTrace();
					}
				}
			}
			final boolean nullable = ((Number)dataRow.get("NULLABLE")).intValue() == DatabaseMetaData.columnNullable;
			final boolean autoIncrement = "Y".equalsIgnoreCase((String)dataRow.get("IS_AUTOINCREMENT"));
			
			columns[i] = new ColumnMetaData(tableName,columnName,caption,type,length,scale,
					defaultValue,nullable,autoIncrement);
		}
		
		final Map<IndexInfo, Set<String>> indexColumnMap = new LinkedHashMap();
		final int count = UNKNOWN_ROW_COUNT;
		
		if(table.getType() == TableType.TABLE)
		{
			if((flags & INDICES) != 0 && indexMap != null)
			{
				final List<VirtualTableRow> indexRows = indexMap.get(tableName.toUpperCase());
				if(indexRows != null && indexRows.size() > 0)
				{
					for(final VirtualTableRow pkRow : indexRows)
					{
						final String indexName = (String)pkRow.get("INDEX_NAME");
						final String columnName = (String)pkRow.get("COLUMN_NAME");
						if(indexName != null && columnName != null)
						{
							final boolean unique = ((Number)pkRow.get("NON_UNIQUE")).intValue() == 0;
							IndexType indexType;
							if("PRIMARY".equals(indexName) || "PRI".equals(indexName))
							{
								indexType = IndexType.PRIMARY_KEY;
							}
							else if(unique)
							{
								indexType = IndexType.UNIQUE;
							}
							else
							{
								indexType = IndexType.NORMAL;
							}
							final IndexInfo info = new IndexInfo(indexName,indexType);
							Set<String> columnNames = indexColumnMap.get(info);
							if(columnNames == null)
							{
								columnNames = new HashSet();
								indexColumnMap.put(info,columnNames);
							}
							columnNames.add(columnName);
						}
					}
				}
			}
		}
		
		final Index[] indices = new Index[indexColumnMap.size()];
		int i = 0;
		for(final IndexInfo indexInfo : indexColumnMap.keySet())
		{
			final Set<String> columnList = indexColumnMap.get(indexInfo);
			final String[] indexColumns = columnList.toArray(new String[columnList.size()]);
			indices[i++] = new Index(indexInfo.name,indexInfo.type,indexColumns);
		}
		
		return new TableMetaData(table,columns,indices,count);
	}
	
	
	@Override
	public EntityRelationshipModel getEntityRelationshipModel(final ProgressMonitor monitor,
			final TableInfo... tableInfos) throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		monitor.setTaskName("");
		
		final EntityRelationshipModel model = new EntityRelationshipModel();
		
		try
		{
			final List<String> tables = new ArrayList();
			for(final TableInfo table : tableInfos)
			{
				if(table.getType() == TableType.TABLE)
				{
					tables.add(table.getName());
				}
			}
			
			final ConnectionProvider connectionProvider = dataSource.getConnectionProvider();
			final Connection connection = connectionProvider.getConnection();
			
			try
			{
				final String catalog = getCatalog(dataSource);
				final String schema = getSchema(dataSource);
				
				final MySQLDatabaseMetaDataFork meta = new MySQLDatabaseMetaDataFork(connection,catalog,
						tableInfos);
				
				final ResultSet rs = meta.getExportedKeys(catalog,schema);
				try
				{
					String pkTable = null;
					String fkTable = null;
					final List<String> pkColumns = new ArrayList();
					final List<String> fkColumns = new ArrayList();
					
					while(rs.next())
					{
						final short keySeq = rs.getShort("KEY_SEQ");
						
						if(keySeq == 1 && pkColumns.size() > 0)
						{
							if(tables.contains(pkTable) && tables.contains(fkTable))
							{
								model.add(new EntityRelationship(new Entity(pkTable,pkColumns
										.toArray(new String[pkColumns.size()]),Cardinality.ONE),
										new Entity(fkTable,fkColumns.toArray(new String[fkColumns
												.size()]),Cardinality.MANY)));
								pkColumns.clear();
								fkColumns.clear();
							}
						}
						
						pkTable = rs.getString("PKTABLE_NAME");
						fkTable = rs.getString("FKTABLE_NAME");
						
						pkColumns.add(rs.getString("PKCOLUMN_NAME"));
						fkColumns.add(rs.getString("FKCOLUMN_NAME"));
					}
					
					if(pkColumns.size() > 0)
					{
						if(tables.contains(pkTable) && tables.contains(fkTable))
						{
							model.add(new EntityRelationship(new Entity(pkTable,pkColumns
									.toArray(new String[pkColumns.size()]),Cardinality.ONE),
									new Entity(fkTable,fkColumns.toArray(new String[fkColumns
											.size()]),Cardinality.MANY)));
							pkColumns.clear();
							fkColumns.clear();
						}
					}
				}
				finally
				{
					rs.close();
				}
			}
			finally
			{
				connection.close();
			}
		}
		catch(final SQLException e)
		{
			throw new DBException(dataSource,e);
		}
		
		monitor.done();
		
		return model;
	}
	
	
	private Map<String, List<VirtualTableRow>> toMap(final VirtualTable vt, final String columnName)
	{
		final Map<String, List<VirtualTableRow>> columnMap = new HashMap();
		final int tableNameColumnIndex = vt.getColumnIndex(columnName);
		for(final VirtualTableRow row : vt.rows())
		{
			CollectionUtils.accumulate(columnMap,
					((String)row.get(tableNameColumnIndex)).toUpperCase(),row);
		}
		return columnMap;
	}
	
	
	@Override
	protected void createTable(final JDBCConnection jdbcConnection, final TableMetaData table)
			throws DBException, SQLException
	{
		final List params = new ArrayList();
		
		final StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" (");
		
		final ColumnMetaData[] columns = table.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			
			final ColumnMetaData column = columns[i];
			appendEscapedName(column.getName(),sb);
			sb.append(" ");
			appendColumnDefinition(column,sb,params);
		}
		
		for(final Index index : table.getIndices())
		{
			sb.append(", ");
			appendIndexDefinition(index,sb);
		}
		
		sb.append(")");
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	
	
	@Override
	protected void addColumn(final JDBCConnection jdbcConnection, final TableMetaData table,
			final ColumnMetaData column, final ColumnMetaData columnBefore, final ColumnMetaData columnAfter)
			throws DBException, SQLException
	{
		final List params = new ArrayList();
		
		final StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD COLUMN ");
		appendEscapedName(column.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb,params);
		if(columnBefore == null)
		{
			sb.append(" FIRST");
		}
		else
		{
			sb.append(" AFTER ");
			appendEscapedName(columnBefore.getName(),sb);
		}
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	
	
	@Override
	protected void alterColumn(final JDBCConnection jdbcConnection, final TableMetaData table,
			final ColumnMetaData column, final ColumnMetaData existing) throws DBException, SQLException
	{
		final List params = new ArrayList();
		
		final StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" MODIFY COLUMN ");
		appendEscapedName(existing.getName(),sb);
		sb.append(" ");
		appendColumnDefinition(column,sb,params);
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	

	@SuppressWarnings("incomplete-switch")
	@Override
	public boolean equalsType(final ColumnMetaData clientColumn, final ColumnMetaData dbColumn)
	{
		final DataType clientType = clientColumn.getType();
		final DataType dbType = dbColumn.getType();
		
		if(clientType == dbType)
		{
			switch(clientType)
			{
				case TINYINT:
				case SMALLINT:
				case INTEGER:
				case BIGINT:
				case REAL:
				case FLOAT:
				case DOUBLE:
				case DATE:
				case TIME:
				case TIMESTAMP:
				case BOOLEAN:
				{
					return true;
				}
				
				case NUMERIC:
				case DECIMAL:
				{
					return clientColumn.getLength() == dbColumn.getLength()
							&& clientColumn.getScale() == dbColumn.getScale();
				}
				
				case CHAR:
				case VARCHAR:
				case BINARY:
				case VARBINARY:
				{
					return clientColumn.getLength() == dbColumn.getLength();
				}
				
				case CLOB:
				case LONGVARCHAR:
				{
					return equalsTextLengthRange(clientColumn,dbColumn);
				}
				
				case BLOB:
				case LONGVARBINARY:
				{
					return equalsBinaryLengthRange(clientColumn,dbColumn);
				}
			}
		}
		
		Boolean match = getTypeMatch(clientColumn,dbColumn);
		if(match != null)
		{
			return match;
		}
		
		match = getTypeMatch(dbColumn,clientColumn);
		if(match != null)
		{
			return match;
		}
		
		return false;
	}
	

	@SuppressWarnings("incomplete-switch")
	private Boolean getTypeMatch(final ColumnMetaData thisColumn, final ColumnMetaData thatColumn)
	{
		final DataType thisType = thisColumn.getType();
		final DataType thatType = thatColumn.getType();
		
		switch(thisType)
		{
			case CLOB:
			case LONGVARCHAR:
			{
				return (thatType == DataType.CLOB || thatType == DataType.LONGVARBINARY)
						&& equalsTextLengthRange(thisColumn,thatColumn);
			}
			
			case BLOB:
			case LONGVARBINARY:
			{
				return (thatType == DataType.BLOB || thatType == DataType.LONGVARBINARY)
						&& equalsBinaryLengthRange(thisColumn,thatColumn);
			}
		}
		
		return null;
	}
	
	
	private boolean equalsTextLengthRange(final ColumnMetaData clientColumn, final ColumnMetaData dbColumn)
	{
		final int clientLength = clientColumn.getLength();
		final int dbLength = dbColumn.getLength();
		
		if(clientLength == dbLength)
		{
			return true;
		}
		else if(clientLength <= 255)
		{
			return dbLength <= 255;
		}
		else if(clientLength <= 65535)
		{
			return dbLength <= 65535;
		}
		else if(clientLength <= 16777215)
		{
			return dbLength <= 16777215;
		}
		else if(clientLength > 16777215)
		{
			return dbLength > 16777215;
		}
		
		return false;
	}
	
	
	private boolean equalsBinaryLengthRange(final ColumnMetaData clientColumn, final ColumnMetaData dbColumn)
	{
		final int clientLength = clientColumn.getLength();
		final int dbLength = dbColumn.getLength();
		
		if(clientLength == dbLength)
		{
			return true;
		}
		else if(clientLength <= 255)
		{
			return dbLength <= 255;
		}
		else if(clientLength <= 65535)
		{
			return dbLength <= 65535;
		}
		else if(clientLength <= 16777215)
		{
			return dbLength <= 16777215;
		}
		else if(clientLength <= 4294967295L)
		{
			return dbLength <= 4294967295L;
		}
		else if(clientLength > 4294967295L)
		{
			return dbLength > 4294967295L;
		}
		 
		return false;
	}
	

	@SuppressWarnings("incomplete-switch")
	private void appendColumnDefinition(final ColumnMetaData column, final StringBuilder sb, final List params)
	{
		final DataType type = column.getType();
		switch(type)
		{
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case REAL:
			case FLOAT:
			case DOUBLE:
			case DATE:
			case TIME:
			case TIMESTAMP:
			{
				sb.append(type.name());
			}
			break;
			
			case NUMERIC:
			case DECIMAL:
			{
				sb.append(type.name());
				sb.append("(");
				sb.append(column.getLength());
				sb.append(",");
				sb.append(column.getScale());
				sb.append(")");
			}
			break;
			
			case BOOLEAN:
			{
				sb.append("BOOL");
			}
			break;
			
			case CHAR:
			case VARCHAR:
			case BINARY:
			case VARBINARY:
			{
				sb.append(type.name());
				sb.append("(");
				sb.append(column.getLength());
				sb.append(")");
			}
			break;
			
			case CLOB:
			case LONGVARCHAR:
			{
				final int length = column.getLength();
				if(length <= 255)
				{
					sb.append("TINYTEXT");
				}
				else if(length <= 65535)
				{
					sb.append("TEXT");
				}
				else if(length <= 16777215)
				{
					sb.append("MEDIUMTEXT");
				}
				else
				{
					sb.append("LONGTEXT");
				}
			}
			break;
			
			case BLOB:
			case LONGVARBINARY:
			{
				final int length = column.getLength();
				if(length <= 255)
				{
					sb.append("TINYBLOB");
				}
				else if(length <= 65535)
				{ 
					sb.append("BLOB");
				}
				else if(length <= 16777215)
				{
					sb.append("MEDIUMBLOB");
				}
				else if(length <= 4294967295L)
				{
					sb.append("LONGBLOB");
				}
				else
				{
					sb.append("BLOB");
				}
			}
			break;
		}
		
		if(column.isNullable())
		{
			sb.append(" NULL");
		}
		else
		{
			sb.append(" NOT NULL");
		}
		
		if(column.isAutoIncrement())
		{
			sb.append(" AUTO_INCREMENT");
		}
		else
		{
			final Object defaultValue = column.getDefaultValue();
			if(!(defaultValue == null && !column.isNullable()))
			{
				sb.append(" DEFAULT ");
				if(defaultValue == null)
				{
					sb.append("NULL");
				}
				else
				{
					sb.append("?");
					params.add(defaultValue);
				}
			}
		}
	}
	
	
	@Override
	protected void dropColumn(final JDBCConnection jdbcConnection, final TableMetaData table,
			final ColumnMetaData column) throws DBException, SQLException
	{
		final StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" DROP COLUMN ");
		appendEscapedName(column.getName(),sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	@Override
	protected void createIndex(final JDBCConnection jdbcConnection, final TableMetaData table, final Index index)
			throws DBException, SQLException
	{
		final StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD ");
		appendIndexDefinition(index,sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	private void appendIndexDefinition(final Index index, final StringBuilder sb)
	{
		switch(index.getType())
		{
			case PRIMARY_KEY:
			{
				sb.append("PRIMARY KEY");
			}
			break;
			
			case UNIQUE:
			{
				sb.append("UNIQUE INDEX");
			}
			break;
			
			case NORMAL:
			{
				sb.append("INDEX ");
			}
			break;
		}
		
		sb.append(" (");
		final String[] columns = index.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			appendEscapedName(columns[i],sb);
		}
		sb.append(")");
	}
	
	
	@Override
	protected void dropIndex(final JDBCConnection jdbcConnection, final TableMetaData table, final Index index)
			throws DBException, SQLException
	{
		final StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		
		if(index.getType() == IndexType.PRIMARY_KEY)
		{
			sb.append(" DROP PRIMARY KEY");
		}
		else
		{
			sb.append(" DROP INDEX ");
			appendEscapedName(index.getName(),sb);
		}
		
		jdbcConnection.write(sb.toString());
	}
	
	
	@Override
	protected void appendEscapedName(final String name, final StringBuilder sb)
	{
		sb.append("`");
		sb.append(name);
		sb.append("`");
	}
}
