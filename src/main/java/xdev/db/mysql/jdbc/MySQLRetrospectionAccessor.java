/*
 * SqlEngine Database Adapter MySQL 5 - XAPI SqlEngine Database Adapter for MySQL 5
 * Copyright © 2003 XDEV Software (https://xdev.software)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package xdev.db.mysql.jdbc;

import com.xdev.jadoth.sqlengine.dbms.standard.StandardRetrospectionAccessor;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineException;
import com.xdev.jadoth.sqlengine.internal.tables.SqlIndex;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;



public class MySQLRetrospectionAccessor extends StandardRetrospectionAccessor<MySQLDbms>
{
	public MySQLRetrospectionAccessor(final MySQLDbms dbmsadaptor)
	{
		super(dbmsadaptor);
	}
	
	 
	@Override
	public String createSelect_INFORMATION_SCHEMA_COLUMNS(final SqlTableIdentity table)
	{
		throw new RuntimeException("Retrospection not implemented yet!");
	}
	
	
	@Override
	public String createSelect_INFORMATION_SCHEMA_INDICES(final SqlTableIdentity table)
	{
		throw new RuntimeException("Retrospection not implemented yet!");
	}
	
	
	@Override
	public SqlIndex[] loadIndices(final SqlTableIdentity table) throws SQLEngineException
	{
		throw new RuntimeException("Retrospection not implemented yet!");
	}
}
