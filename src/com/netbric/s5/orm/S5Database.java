package com.netbric.s5.orm;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.HashMap;

import javax.sql.DataSource;

import com.dieselpoint.norm.Transaction;
import com.dieselpoint.norm.sqlmakers.MySqlMaker;
import com.netbric.s5.conductor.exception.ConfigException;

import com.dieselpoint.norm.Database;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S5Database extends Database
{
	// MySQL 8.0 以下版本 - JDBC 驱动名及数据库 URL
//	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

	// MySQL 8.0 以上版本 - JDBC 驱动名及数据库 URL
	static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
	static final Logger logger = LoggerFactory.getLogger(S5Database.class);
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
	static {
		instance.setSqlMaker(new MySqlMaker());
	}
	private String dbIp;
	private String dbUser;
	private String dbPass;
	private String dbName;
	private String dbPort;

	public void init(com.netbric.s5.conductor.Config cfg) throws ConfigException
	{
		dbIp = cfg.getString("db", "ip", null, true);
		dbUser = cfg.getString("db", "user", null, true);
		dbPass = cfg.getString("db", "pass", null, true);
		dbName = cfg.getString("db", "db_name", null, true);
		dbPort = cfg.getString("db", "db_port", "3306", false);
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
			String url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&serverTimezone=UTC", dbIp, dbPort, dbName);
			logger.info("meta db:{}", url);
			config.setJdbcUrl(url);
			config.setUsername(dbUser);
			config.setPassword(dbPass);
			config.setMaximumPoolSize(50);
			return new HikariDataSource(config);

		}
		catch (Exception e)
		{
			System.err.println(e.getMessage());
		}
		return null;
	}

	public long queryLongValue(Transaction tx, String sql,  Object... args)throws SQLException {
		HashMap m;
		if(tx == null)
			m = sql(sql, args).first(HashMap.class);
		else
			m = transaction(tx).sql(sql, args).first(HashMap.class);
		for (Object value : m.values()) {
			if(value instanceof  BigDecimal)
				return ((BigDecimal)value).longValue();
			if(value instanceof BigInteger)
				return ((BigInteger)value).longValue();
			if(value instanceof Integer)
				return ((Integer)value).longValue();
			if(value instanceof Long)
				return ((Long)value).longValue();
			logger.error("Unexpected type from DB:{}", value.getClass().getName());
		}
		throw new SQLException(String.format("No valid result returned for sql:%s %s", sql, args));
	}
	public long queryLongValue(String sql,  Object... args) throws SQLException {
		return queryLongValue(null, sql, args);
	}

	public String queryStringValue(String sql,  Object... args) throws SQLException {
		HashMap m = sql(sql, args).first(HashMap.class);
		for (Object value : m.values()) {
			if(value instanceof  String)
				return (String)value;
			logger.error("Unexpected type from DB:{}", value.getClass().getName());
		}
		throw new SQLException(String.format("No valid result returned for sql:%s %s", sql, args));
	}
}
