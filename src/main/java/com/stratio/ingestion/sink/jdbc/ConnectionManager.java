/*------------------------------------------------------------------------------
 * PROJ   : LG U+ IoT Platform 프로젝트
 * NAME   : ConnectionManager.java
 * DESC   : DBCP Connection Pool Manager
 * Author : 윤순혁
 * VER    : 1.0
 * Copyright 2014 LG CNS All rights reserved
 *------------------------------------------------------------------------------
 *                  변         경         사         항                       
 *------------------------------------------------------------------------------
 *    DATE       AUTHOR                      DESCRIPTION                        
 * ----------    ------  --------------------------------------------------------- 
 * 2015. 8. 7.  윤순혁    최초 프로그램 작성                                     
 */ 

package com.stratio.ingestion.sink.jdbc;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * <PRE>
 * DBCP Connection Pool Manager
 * </PRE>
 *
 * @author    윤순혁
 * @version   1.0
 */
public enum ConnectionManager {

	instance;

	private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);
    private static final String CONF_DRIVER = "driver";
    private static final String CONF_CONNECTION_STRING = "connectionString";
    private static final String CONF_USER = "username";
    private static final String CONF_PASSWORD = "password";

    private static final String CONF_DBCP_PREFIX = "dbcp.";

	private BasicDataSource datasource;

	/**
	 * DBCP 초기화
	 * 반드시 Sink 초기에 먼저 수행할 것
	 * 
	 * @param context Flume NG Context
	 */
	public void initialize( Context context ) {
		if ( datasource != null ) {
			try {
				datasource.close();
			} catch ( SQLException e ) { // Do-Nothing
			}
			datasource = null;
		}

		datasource = new BasicDataSource();

		final String driver = context.getString( CONF_DRIVER );
		final String connectionString = context.getString( CONF_CONNECTION_STRING );
		if ( Strings.isNullOrEmpty( driver ) || Strings.isNullOrEmpty( connectionString ) ) {
			throw new JDBCSinkException( "driver and connectionString is required" );
		}
		final String username = context.getString( CONF_USER );
		final String password = context.getString( CONF_PASSWORD );

		datasource.setDriverClassName( driver );
		datasource.setUrl( connectionString );
		datasource.setUsername( username );
		datasource.setPassword( password );

		/*
		 * DBCP Configuration setup
		 * Apache dbcp configuration fields (ex. dbcp.maxWait, dbcp.maxActive).
		 *  
		 * https://commons.apache.org/proper/commons-dbcp/configuration.html
		 */
		Map<String, String> jdbcProperties = context.getSubProperties( CONF_DBCP_PREFIX );
		for ( String key : jdbcProperties.keySet() ) {
			boolean chkSet = false;
			try {
				// dbcp 설정을 가져와서 Datasource 클래스의 setter 와 매칭되는 함수를 찾은 다음 적용한다
				// 예) maxActive --> setMaxActive 함수를 찾아서 설정 값 적용
				for ( Method method : datasource.getClass().getDeclaredMethods() ) {
					if ( method.getParameterTypes().length > 0 && method.getName().equals( "set" + StringUtils.capitalize( key ) ) ) {
						String value = jdbcProperties.get( key );

						if ( method.getParameterTypes()[0].isAssignableFrom( int.class ) ) {
							method.invoke( datasource, Integer.parseInt( value ) );
							chkSet = true;
							break;
						} else if ( method.getParameterTypes()[0].isAssignableFrom( long.class ) ) {
							method.invoke( datasource, Long.parseLong( value ) );
							chkSet = true;
							break;
						} else if ( method.getParameterTypes()[0].isAssignableFrom( boolean.class ) ) {
							method.invoke( datasource, Boolean.valueOf( value ) );
							chkSet = true;
							break;
						} else if ( method.getParameterTypes()[0].isAssignableFrom( String.class ) ) {
							method.invoke( datasource, value );
							chkSet = true;
							break;
						}
					}
				}
			} catch (Exception ex ) {
				log.warn( "DBCP configuration set failed: {} - {}", key, ex.getMessage() );
			}

			if (!chkSet) {
				log.warn( "DBCP configuration field not found: {}", key );
			}
		}

		datasource.setDefaultAutoCommit( false );
		log.info( "DBCP initialize success" );
	}

	/**
	 * @return the datasource
	 */
	public BasicDataSource getDatasource() { // TODO 개선 연구 할 것
		if ( datasource == null ) {
			throw new JDBCSinkException( "ConnectionManager is not initialized" );
		}

		return datasource;
	}

	/**
	 * Get JDBC Connection
	 * 
	 * @return JDBC Connection
	 * @exception SQLException
	 */
	public Connection getConnection() throws SQLException { // TODO 개선 연구 할 것
		if ( datasource == null ) {
			throw new JDBCSinkException( "ConnectionManager is not initialized" );
		}

		return datasource.getConnection();
	}
}