/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.ingestion.sink.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Saves Flume events to any database with a JDBC driver. It can operate either
 * with automatic headers-to-tables mapping or with custom SQL queries.
 *
 * Available configuration parameters are:
 *
 * <p><ul>
 * <li><tt>driver</tt> <em>(string, required)</em>: The driver class (e.g.
 *      <tt>org.h2.Driver</tt>, <tt>org.postgresql.Driver</tt>). <strong>NOTE:</strong>
 *      Stratio JDBC Sink does not include any JDBC driver. You must add a JDBC
 *      driver to your Flume classpath.</li>
 * <li><tt>connectionString</tt> <em>(string, required)</em>: A valid
 *      connection string to a database. Check the documentation for your JDBC driver
 *      for more information.</li>
 * <li><tt>username</tt> <em>(string)</em>: A valid database username.</li>
 * <li><tt>password</tt> <em>(string)</em>: Password.</li>
 * <li><tt>sqlDialect</tt> <em>(string, required)</em>: The SQL dialect of your
 *      database. This should be one of the following: <tt>CUBRID</tt>, <tt>DERBY</tt>,
 *      <tt>FIREBIRD</tt>, <tt>H2</tt>, <tt>HSQLDB</tt>, <tt>MARIADB</tt>, <tt>MYSQL</tt>,
 *      <tt>POSTGRES</tt>, <tt>SQLITE</tt>.</li>
 * <li><tt>table</tt> <em>(string)</em>: A table to store your events.
 *      <em>This is only used for automatic mapping.</em></li>
 * <li><tt>sql</tt> <em>(string)</em>: A custom SQL query to use. If specified,
 *      this query will be used instead of automatic mapping. E.g.
 *      <tt>INSERT INTO tweets (text, num_hashtags, timestamp) VALUES (${body:string}, ${header.numberOfHashtags:integer}, ${header.date:timestamp})</tt>.
 *      Note the variable format: the first part is either <tt>body</tt> or
 *      <tt>header.yourHeaderName</tt> and then the SQL type.</li>
 * <li><tt>batchSize</tt> <em>(integer)</em>: Number of events that will be grouped
 *      in the same query and transaction. Defaults to <tt>20</tt>.</li>
 * </ul></p>
 * ================================================================================================
 * 커스텀: JDBC Connection 생성 방식을 단일 구성에서 Datasource 기반 Connection Pool로 변경
 * https://github.com/Stratio/flume-ingestion/tree/master/stratio-sinks/stratio-jdbc-sink
 */
public class JDBCSink extends AbstractSink implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(JDBCSink.class);

    private static final int DEFAULT_BATCH_SIZE = 20;
    private static final String CONF_SQL_DIALECT = "sqlDialect";
    private static final String CONF_TABLE = "table";
    private static final String CONF_BATCH_SIZE = "batchSize";
    private static final String CONF_SQL = "sql";

    private SQLDialect sqlDialect;
    private SinkCounter sinkCounter;
    private int batchsize;
    private QueryGenerator queryGenerator;

    /**
     * Desc : Constructor of JDBCSink.java class
     */
    public JDBCSink() {
        super();
    }

    @SuppressWarnings( "deprecation" )
	@Override
    public void configure(Context context) {
    	// DBCP 초기화
    	ConnectionManager.instance.initialize( context );

        this.batchsize = context.getInteger(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE);

        this.sqlDialect = SQLDialect.valueOf(context.getString(CONF_SQL_DIALECT).toUpperCase(Locale.ENGLISH));

        final String sql = context.getString(CONF_SQL);
        if (sql == null) {
        	Connection connection = null;
            try {
            	// Table 정보 매핑
            	connection = ConnectionManager.instance.getConnection();
            	final DSLContext create = DSL.using(connection, sqlDialect);
            	this.queryGenerator = new MappingQueryGenerator(create, context.getString(CONF_TABLE));
            } catch (SQLException ex) {
                throw new JDBCSinkException(ex);
            } finally {
            	JDBCUtils.safeClose( connection );
            }
        } else {
            this.queryGenerator = new TemplateQueryGenerator(sqlDialect, sql);
        }

        this.sinkCounter = new SinkCounter(this.getName());
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.BACKOFF;
        Transaction transaction = this.getChannel().getTransaction();
        Connection connection = null;

        try {
        	transaction.begin();

        	connection = ConnectionManager.instance.getConnection();
        	final DSLContext create = DSL.using(connection, sqlDialect);

        	List<Event> eventList = this.takeEventsFromChannel( this.getChannel(), this.batchsize);
            status = Status.READY;
            if (!eventList.isEmpty()) {
                if (eventList.size() == this.batchsize) {
                    this.sinkCounter.incrementBatchCompleteCount();
                } else {
                    this.sinkCounter.incrementBatchUnderflowCount();
                }

                final boolean success = this.queryGenerator.executeQuery(create, eventList);

                if (!success) {
                    throw new JDBCSinkException("Query failed");
                }

                connection.commit();

                this.sinkCounter.addToEventDrainSuccessCount(eventList.size());
            } else {
                this.sinkCounter.incrementBatchEmptyCount();
            }

            transaction.commit();
            status = Status.READY;
        } catch (Throwable t) {
            log.error("Exception during process", t);
            try {
            	connection.rollback();
            } catch (Exception ex) {
                log.error("Exception on rollback", ex);
            } finally {
                transaction.rollback();
                status = Status.BACKOFF;
                this.sinkCounter.incrementConnectionFailedCount();
                if (t instanceof Error) {
                    throw new JDBCSinkException(t);
                }
            }
        } finally {
            transaction.close();
            JDBCUtils.safeClose( connection );
        }
        return status;
    }

    @Override
    public synchronized void start() {
        this.sinkCounter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        this.sinkCounter.stop();
        super.stop();
    }

    private List<Event> takeEventsFromChannel(Channel channel, int eventsToTake) {
        List<Event> events = new ArrayList<Event>();
        for (int i = 0; i < eventsToTake; i++) {
            this.sinkCounter.incrementEventDrainAttemptCount();
            events.add(channel.take());
        }
        events.removeAll(Collections.singleton(null));
        return events;
    }

}
