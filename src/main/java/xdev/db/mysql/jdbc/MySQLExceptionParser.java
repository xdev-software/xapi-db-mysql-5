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

import java.sql.SQLException;

import com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser;
import com.xdev.jadoth.sqlengine.exceptions.SQLEngineException;



public class MySQLExceptionParser implements SQLExceptionParser
{
	
	/**
	 * @param e
	 * @return
	 * @see com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser#parseSQLException(java.sql.SQLException)
	 */
	@Override 
	public SQLEngineException parseSQLException(final SQLException e)
	{
		return new SQLEngineException(e);
	}
	
}
