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


import java.util.Hashtable;

import com.xdev.jadoth.sqlengine.SQL;
import com.xdev.jadoth.sqlengine.SQL.DATATYPE;
import com.xdev.jadoth.sqlengine.SQL.INDEXTYPE;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDDLMapper;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;




public class MySQLDDLMapper extends StandardDDLMapper<MySQLDbms>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	 
	/** The Constant DATATYPE_BIT. */
	public static final String							DATATYPE_BIT		= "BIT";
	
	/** The Constant DATATYPE_DATETIME. */
	public static final String							DATATYPE_DATETIME	= "DATETIME";
	
	/** The Constant DATATYPE_TEXT. */
	public static final String							DATATYPE_TEXT		= "TEXT";
	
	/** The Constant DATATYPE_IMAGE. */
	public static final String							DATATYPE_IMAGE		= "IMAGE";
	
	/** The Constant dataTypeStrings. */
	private static final Hashtable<String, DATATYPE>	dataTypeStrings		= createDataTypeStrings();
	
	
	/**
	 * Creates the data type strings.
	 * 
	 * @return the hashtable
	 */
	private static final Hashtable<String, DATATYPE> createDataTypeStrings()
	{
		final Hashtable<String, DATATYPE> c = new Hashtable<>(10);
		
		c.put(DATATYPE_BIT,SQL.DATATYPE.BOOLEAN);
		
		c.put(DATATYPE_DATETIME,SQL.DATATYPE.TIMESTAMP);
		
		c.put(DATATYPE_TEXT,SQL.DATATYPE.LONGVARCHAR);
		c.put(DATATYPE_IMAGE,SQL.DATATYPE.LONGVARBINARY);
		
		c.put(SQL.DATATYPE.FLOAT.name(),SQL.DATATYPE.DOUBLE);
		
		// replicate the whole stuff for lower case, just in case
		for(final String s : c.keySet().toArray(new String[c.size()]))
		{
			c.put(s.toLowerCase(),c.get(s));
		}
		
		return c;
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	/**
	 * Instantiates a new ms sql2005 ddl mapper.
	 * 
	 * @param dbmsAdaptor
	 *            the dbms adaptor
	 */
	protected MySQLDDLMapper(final MySQLDbms dbmsAdaptor)
	{
		super(dbmsAdaptor);
	}
	
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	
	/**
	 * Map data type.
	 * 
	 * @param dataTypeString
	 *            the data type string
	 * @return the dATATYPE
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsDDLMapper.AbstractBody#mapDataType(java.lang.String)
	 */
	@Override
	public DATATYPE mapDataType(final String dataTypeString)
	{
		final String upperCaseDataType = dataTypeString.toUpperCase();
		
		final DATATYPE dbmsType = dataTypeStrings.get(upperCaseDataType);
		if(dbmsType != null)
		{
			return dbmsType;
		}
		
		return super.mapDataType(upperCaseDataType);
	}
	
	
	/**
	 * Map index type.
	 * 
	 * @param indexTypeString
	 *            the index type string
	 * @return the iNDEXTYPE
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsDDLMapper.AbstractBody#mapIndexType(java.lang.String)
	 */
	@Override
	public INDEXTYPE mapIndexType(final String indexTypeString)
	{
		if(indexTypeString == null)
		{
			return SQL.INDEXTYPE.NORMAL;
		}
		
		if(indexTypeString.contains("unique"))
		{
			return SQL.INDEXTYPE.UNIQUE;
		}
		
		return SQL.INDEXTYPE.NORMAL;
	}
	
	
	/**
	 * Lookup ddbms data type mapping.
	 * 
	 * @param type
	 *            the type
	 * @param table
	 *            the table
	 * @return the string
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsDDLMapper.AbstractBody#lookupDdbmsDataTypeMapping(com.xdev.jadoth.sqlengine.SQL.DATATYPE,
	 *      com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity)
	 */
	@Override
	public String lookupDdbmsDataTypeMapping(final DATATYPE type, final SqlTableIdentity table)
	{
		switch(type)
		{
			case BOOLEAN:
				return DATATYPE_BIT;
			case TIMESTAMP:
				return DATATYPE_DATETIME;
			case LONGVARCHAR:
				return DATATYPE_TEXT;
			case DOUBLE:
				return SQL.DATATYPE.FLOAT.name();
			case FLOAT:
				return SQL.DATATYPE.REAL.name();
			default:
				return super.lookupDdbmsDataTypeMapping(type,table);
		}
	}
	
}
