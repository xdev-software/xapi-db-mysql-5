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

import xdev.db.DBException;
import xdev.db.jdbc.JDBCDataSource;


public class MySQLJDBCDataSource extends JDBCDataSource<MySQLJDBCDataSource, MySQLDbms>
{
	public MySQLJDBCDataSource()
	{
		super(new MySQLDbms());
	} 
	
	
	@Override
	public Parameter[] getDefaultParameters()
	{
		return new Parameter[]{
				HOST.clone(),
				PORT.clone(3306),
				USERNAME.clone("root"),
				PASSWORD.clone(),
				CATALOG.clone(),
				URL_EXTENSION
						.clone("?jdbcCompilantTruncation=false&zeroDateTimeBehavior=convertToNull"),
				IS_SERVER_DATASOURCE.clone(),SERVER_URL.clone(),AUTH_KEY.clone()};
	}
	
	
	@Override
	protected MySQLConnectionInformation getConnectionInformation()
	{
		return new MySQLConnectionInformation(getHost(),getPort(),getUserName(),getPassword()
				.getPlainText(),getCatalog(),getUrlExtension(),getDbmsAdaptor());
	}
	
	
	@Override
	public MySQLJDBCConnection openConnectionImpl() throws DBException
	{
		return new MySQLJDBCConnection(this);
	}
	
	
	@Override
	public MySQLJDBCMetaData getMetaData() throws DBException
	{
		return new MySQLJDBCMetaData(this);
	}
	
	
	@Override
	public boolean canExport()
	{
		return false;
	}
}
