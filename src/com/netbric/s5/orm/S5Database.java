package com.netbric.s5.orm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.netbric.s5.conductor.ConfigException;
import org.apache.commons.lang3.SystemUtils;

import com.dieselpoint.norm.Database;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class S5Database extends Database
{
	// MySQL 8.0 以下版本 - JDBC 驱动名及数据库 URL
//	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

	// MySQL 8.0 以上版本 - JDBC 驱动名及数据库 URL
	static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
	static
	{
		try
		{
			Class.forName(JDBC_DRIVER);
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
	private String dbIp;
	private String dbUser;
	private String dbPass;
	private String dbName;

	public void init(com.netbric.s5.conductor.Config cfg) throws ConfigException
	{
		dbIp = cfg.getString("db", "ip", null, true);
		dbUser = cfg.getString("db", "user", null, true);
		dbPass = cfg.getString("db", "pass", null, true);
		dbName = cfg.getString("db", "db_name", null, true);

	}

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
			HikariConfig config = new HikariConfig();
			config.setMaximumPoolSize(100);
//			config.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
			config.setJdbcUrl(String.format("jdbc:mysql://%s:3306/%s?useSSL=false&serverTimezone=UTC", dbIp, dbName));
			config.setUsername(dbUser);
			config.setPassword(dbPass);
			return new HikariDataSource(config);

		}
		catch (Exception e)
		{
			System.err.println(e.getMessage());
		}
		return null;
	}
}
