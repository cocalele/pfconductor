package com.netbric.s5.orm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.lang3.SystemUtils;

import com.dieselpoint.norm.Database;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class S5Database extends Database
{
	static
	{
		try
		{
			Class.forName("org.sqlite.JDBC");
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
		}
		// System.setProperty("norm.dataSourceClassName", "org.sqlite.JDBC");
		// System.setProperty("norm.dataSourceClassName",
		// "jdbc:sqlite:C:/data/workspace/s5-iscsi/res/s5_iscsi.db");
		// System.setProperty("norm.serverName", "localhost");
		// System.setProperty("norm.databaseName", "mydb");
		// System.setProperty("norm.user", "root");
		// System.setProperty("norm.password", "rootpassword");

	}

	static S5Database instance = new S5Database();

	public static S5Database getInstance()
	{
		return instance;
	}

	@Override
	protected DataSource getDataSource() throws SQLException
	{

		// Connection connection = null;
		try
		{
			// Class.forName("org.sqlite.JDBC");
			// create a database connection
			// connection =
			// DriverManager.getConnection("jdbc:sqlite:C:/data/workspace/s5-iscsi/res/s5_iscsi.db");
			HikariConfig config = new HikariConfig();
			config.setMaximumPoolSize(100);
			config.setDataSourceClassName(System.getProperty("org.sqlite.JDBC"));
			if (SystemUtils.IS_OS_WINDOWS)
				config.setJdbcUrl("jdbc:sqlite:C:/data/workspace/PureFlash/jconductor/res/s5meta.db");
			else
				config.setJdbcUrl("jdbc:sqlite:/etc/s5/s5_iscsi.db");

			config.setInitializationFailFast(true);
			return new HikariDataSource(config);

		}
		catch (Exception e)
		{
			System.err.println(e.getMessage());
		}
		return null;
	}
}
