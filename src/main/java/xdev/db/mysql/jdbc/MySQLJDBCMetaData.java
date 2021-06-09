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
	
	
	public MySQLJDBCMetaData(MySQLJDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	
	@Override
	public TableMetaData[] getTableMetaData(ProgressMonitor monitor, int flags, TableInfo... tables)
			throws DBException
	{
		if(tables == null || tables.length == 0)
		{
			return new TableMetaData[0];
		}
		
		List<TableMetaData> list = new ArrayList(tables.length);
		
		try
		{
			JDBCConnection jdbcConnection = (JDBCConnection)dataSource.openConnection();
			
			try
			{
				monitor.beginTask("",ProgressMonitor.UNKNOWN);
				
				DatabaseMetaData meta = jdbcConnection.getConnection().getMetaData();
				
				ResultSet rs = meta.getColumns(getCatalog(dataSource),getSchema(dataSource),null,
						null);
				JDBCResult jrs = new JDBCResult(rs);
				
				// workaround for mysql-jdbc-(bug?)
				// COLUMN_NAME's length is 10, so the names get cut
				jrs.getMetadata(3).setLength(-1);
				
				VirtualTable vtColumns = new VirtualTable(jrs,true);
				rs.close();
				Map<String, List<VirtualTableRow>> columnMap = toMap(vtColumns,"TABLE_NAME");
				
				Map<String, List<VirtualTableRow>> indexMap = null;
				if((flags & INDICES) != 0 && !monitor.isCanceled())
				{
					Result r = jdbcConnection.query(
							"SELECT TABLE_NAME,NON_UNIQUE,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME "
									+ "FROM INFORMATION_SCHEMA.STATISTICS "
									+ "WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME,SEQ_IN_INDEX",
							getCatalog(dataSource));
					
					// workaround for mysql-jdbc-(bug?)
					// COLUMN_NAME's length is 10, so the names get cut
					r.getMetadata(4).setLength(-1);
					
					VirtualTable vtIndexInfo = new VirtualTable(r,true);
					rs.close();
					indexMap = toMap(vtIndexInfo,"TABLE_NAME");
				}
				
				monitor.beginTask("",tables.length);
				
				int done = 0;
				for(TableInfo table : tables)
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
					catch(Exception e)
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
		catch(SQLException e)
		{
			throw new DBException(dataSource,e);
		}
		
		monitor.done();
		
		return list.toArray(new TableMetaData[list.size()]);
	}
	
	
	protected TableMetaData getTableMetaData(JDBCConnection jdbcConnection, DatabaseMetaData meta,
			int flags, TableInfo table, Map<String, List<VirtualTableRow>> columnMap,
			Map<String, List<VirtualTableRow>> indexMap) throws DBException, SQLException
	{
		String tableName = table.getName();
		
		List<VirtualTableRow> columnRows = columnMap.get(tableName.toUpperCase());
		int columnCount = columnRows.size();
		ColumnMetaData[] columns = new ColumnMetaData[columnCount];
		for(int i = 0; i < columnCount; i++)
		{
			VirtualTableRow dataRow = columnRows.get(i);
			String columnName = (String)dataRow.get("COLUMN_NAME");
			String caption = null;
			int sqlType = ((Number)dataRow.get("DATA_TYPE")).intValue();
			DataType type = DataType.get(sqlType);
			Object lengthObj = dataRow.get("COLUMN_SIZE");
			int length = lengthObj instanceof Number ? ((Number)lengthObj).intValue() : 0;
			Object scaleObj = dataRow.get("DECIMAL_DIGITS");
			int scale = scaleObj instanceof Number ? ((Number)scaleObj).intValue() : 0;
			if(scale < 0)
			{
				scale = 0;
			}
			String defaultStr = (String)dataRow.get("COLUMN_DEF");
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
					catch(NumberFormatException e)
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
					catch(NumberFormatException e)
					{
						e.printStackTrace();
					}
				}
			}
			boolean nullable = ((Number)dataRow.get("NULLABLE")).intValue() == DatabaseMetaData.columnNullable;
			boolean autoIncrement = "Y".equalsIgnoreCase((String)dataRow.get("IS_AUTOINCREMENT"));
			
			columns[i] = new ColumnMetaData(tableName,columnName,caption,type,length,scale,
					defaultValue,nullable,autoIncrement);
		}
		
		Map<IndexInfo, Set<String>> indexColumnMap = new LinkedHashMap();
		int count = UNKNOWN_ROW_COUNT;
		
		if(table.getType() == TableType.TABLE)
		{
			if((flags & INDICES) != 0 && indexMap != null)
			{
				List<VirtualTableRow> indexRows = indexMap.get(tableName.toUpperCase());
				if(indexRows != null && indexRows.size() > 0)
				{
					for(VirtualTableRow pkRow : indexRows)
					{
						String indexName = (String)pkRow.get("INDEX_NAME");
						String columnName = (String)pkRow.get("COLUMN_NAME");
						if(indexName != null && columnName != null)
						{
							boolean unique = ((Number)pkRow.get("NON_UNIQUE")).intValue() == 0;
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
							IndexInfo info = new IndexInfo(indexName,indexType);
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
		
		Index[] indices = new Index[indexColumnMap.size()];
		int i = 0;
		for(IndexInfo indexInfo : indexColumnMap.keySet())
		{
			Set<String> columnList = indexColumnMap.get(indexInfo);
			String[] indexColumns = columnList.toArray(new String[columnList.size()]);
			indices[i++] = new Index(indexInfo.name,indexInfo.type,indexColumns);
		}
		
		return new TableMetaData(table,columns,indices,count);
	}
	
	
	@Override
	public EntityRelationshipModel getEntityRelationshipModel(ProgressMonitor monitor,
			TableInfo... tableInfos) throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		monitor.setTaskName("");
		
		EntityRelationshipModel model = new EntityRelationshipModel();
		
		try
		{
			List<String> tables = new ArrayList();
			for(TableInfo table : tableInfos)
			{
				if(table.getType() == TableType.TABLE)
				{
					tables.add(table.getName());
				}
			}
			
			ConnectionProvider connectionProvider = dataSource.getConnectionProvider();
			Connection connection = connectionProvider.getConnection();
			
			try
			{
				String catalog = getCatalog(dataSource);
				String schema = getSchema(dataSource);
				
				MySQLDatabaseMetaDataFork meta = new MySQLDatabaseMetaDataFork(connection,catalog,
						tableInfos);
				
				ResultSet rs = meta.getExportedKeys(catalog,schema);
				try
				{
					String pkTable = null;
					String fkTable = null;
					List<String> pkColumns = new ArrayList();
					List<String> fkColumns = new ArrayList();
					
					while(rs.next())
					{
						short keySeq = rs.getShort("KEY_SEQ");
						
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
		catch(SQLException e)
		{
			throw new DBException(dataSource,e);
		}
		
		monitor.done();
		
		return model;
	}
	
	
	private Map<String, List<VirtualTableRow>> toMap(VirtualTable vt, String columnName)
	{
		Map<String, List<VirtualTableRow>> columnMap = new HashMap();
		int tableNameColumnIndex = vt.getColumnIndex(columnName);
		for(VirtualTableRow row : vt.rows())
		{
			CollectionUtils.accumulate(columnMap,
					((String)row.get(tableNameColumnIndex)).toUpperCase(),row);
		}
		return columnMap;
	}
	
	
	@Override
	protected void createTable(JDBCConnection jdbcConnection, TableMetaData table)
			throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" (");
		
		ColumnMetaData[] columns = table.getColumns();
		for(int i = 0; i < columns.length; i++)
		{
			if(i > 0)
			{
				sb.append(", ");
			}
			
			ColumnMetaData column = columns[i];
			appendEscapedName(column.getName(),sb);
			sb.append(" ");
			appendColumnDefinition(column,sb,params);
		}
		
		for(Index index : table.getIndices())
		{
			sb.append(", ");
			appendIndexDefinition(index,sb);
		}
		
		sb.append(")");
		
		jdbcConnection.write(sb.toString(),false,params.toArray());
	}
	
	
	@Override
	protected void addColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData columnBefore, ColumnMetaData columnAfter)
			throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
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
	protected void alterColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData existing) throws DBException, SQLException
	{
		List params = new ArrayList();
		
		StringBuilder sb = new StringBuilder();
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
	public boolean equalsType(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		DataType clientType = clientColumn.getType();
		DataType dbType = dbColumn.getType();
		
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
	private Boolean getTypeMatch(ColumnMetaData thisColumn, ColumnMetaData thatColumn)
	{
		DataType thisType = thisColumn.getType();
		DataType thatType = thatColumn.getType();
		
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
	
	
	private boolean equalsTextLengthRange(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		int clientLength = clientColumn.getLength();
		int dbLength = dbColumn.getLength();
		
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
	
	
	private boolean equalsBinaryLengthRange(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		int clientLength = clientColumn.getLength();
		int dbLength = dbColumn.getLength();
		
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
		else if(clientLength <= 4294967295l)
		{
			return dbLength <= 4294967295l;
		}
		else if(clientLength > 4294967295l)
		{
			return dbLength > 4294967295l;
		}
		
		return false;
	}
	

	@SuppressWarnings("incomplete-switch")
	private void appendColumnDefinition(ColumnMetaData column, StringBuilder sb, List params)
	{
		DataType type = column.getType();
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
				int length = column.getLength();
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
				int length = column.getLength();
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
				else if(length <= 4294967295l)
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
			Object defaultValue = column.getDefaultValue();
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
	protected void dropColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column) throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" DROP COLUMN ");
		appendEscapedName(column.getName(),sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	@Override
	protected void createIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		appendEscapedName(table.getTableInfo().getName(),sb);
		sb.append(" ADD ");
		appendIndexDefinition(index,sb);
		
		jdbcConnection.write(sb.toString());
	}
	
	
	private void appendIndexDefinition(Index index, StringBuilder sb)
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
		String[] columns = index.getColumns();
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
	protected void dropIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
		StringBuilder sb = new StringBuilder();
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
	protected void appendEscapedName(String name, StringBuilder sb)
	{
		sb.append("`");
		sb.append(name);
		sb.append("`");
	}
}
