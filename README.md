Stratio JDBC Sink
=================
Github: https://github.com/Stratio/ingestion/tree/master/stratio-sinks/stratio-jdbc-sink

Stratio JDBC Sink saves Flume events to any database with a JDBC driver.
It can operate either with automatic headers-to-tables mapping or with custom SQL queries.

Customizing
=============
* 본 프로젝트는 Apache Flume 플러그인 중 하나인 'Stratio JDBC Sink'의 커스텀 프로젝트 이다.
* 이력로그 집계룰 위해 이력 로그 전문을 파싱하여 필요한 테이블에 저장할 수 있도록 JDBC Sink 로직을 일부 커스텀 하였다.

* 대상:
   1) com.stratio.ingestion.sink.jdbc.MappingQueryGenerator
   2) com.stratio.ingestion.sink.jdbc.JDBCSink

* 내용:
   1) Event Body 문자열을 파싱하여 및 Column/Value 조합의 Map 형태로 변환한 다음 이력로그 대상 테이블 Insert 구문으로 매핑하여 파싱의 유연함 및 DB 컬럼 확장 등 대응을 용이하게 하였다.
      - Body: "KEY1=VALUE1|KEY2=VALUE2" => {KEY1: VALUE1, KEY2: VALUE2}
      - 즉, Map의 Key는 테이블의 컬럼, Value는 해당 컬럼에 들어갈 값이 된다.
      - Key에 해당하는 컬럼이 테이블에 없을 경우 무시한다.
   2) JDBC Connection 생성 방식을 단일 구성에서 Apache DBCP 기반 Connection Pool로 변경
      - 기능 개선 (장시간 IDLE 상태일때 발생하는 JDBC 에러 방지)

* 제약사항: Configuration `sql` 사용 금지

* 지원 JDBC: MariaDB

Build
=============
개발도구 -> Project 단축메뉴 -> Run As -> Maven Install<br>
-> target 디렉토리에 생성된 'stratio-jdbc-sink-0.4.0.jar' 파일을<br>
Apache Flume의 'lib' 경로에 배포<br>
주의: 반드시 로컬의 'dependency' 디렉토리에 있는 라이브러리(jooq, Mariadb jdbc driver)가 동일 경로에 포함되어 있어야 한다.

Configuration
=============

The available config parameters are:

- `driver` *(string, required)*: The driver class (e.g. `org.h2.Driver`, `org.postgresql.Driver`). **NOTE: Stratio JDBC Sink does not include any JDBC driver. You must add a JDBC driver to your Flume classpath.**

- `connectionString` *(string, required)*: A valid connection string to a database. Check the documentation for your JDBC driver for more information.

- `username` *(string)*: A valid database username.

- `password` *(string)*: Password.

- `sqlDialect` *(string, required)*: The SQL dialect of your database. This should be one of the following: `CUBRID`, `DERBY`, `FIREBIRD`, `H2`, `HSQLDB`, `MARIADB`, `MYSQL`, `POSTGRES`, `SQLITE`. 

- `table` *(string, required)*: A table to store your events. *This is only used for automatic mapping.*

- `sql` *(string, deprecated)*: "프로젝트에서 사용안함" A custom SQL query to use. If specified, this query will be used instead of automatic mapping. E.g. `INSERT INTO tweets (text, num_hashtags, timestamp) VALUES (${body:string}, ${header.numberOfHashtags:integer}, ${header.date:timestamp})`. Note the variable format: the first part is either `body` or `header.yourHeaderName` and then the SQL type.

- `batchSize` *(integer)*: Number of events that will be grouped in the same query and transaction. Defaults to 20.

- `dbcp.~` *(etc)* : "신규 추가된 설정" Apache dbcp configuration fields (ex. dbcp.maxWait, dbcp.maxActive). ref: https://commons.apache.org/proper/commons-dbcp/configuration.html

Automatic mapping
=================

If no custom SQL option is given, the database schema will be analyzed. Then, for each event, each header will be mapped to a table field with the exact same name (case insensitive), if any. Type conversion will be done automatically to match the corresponding SQL type.

Note that event body is NOT mapped automatically.

Sample Flume config
===================

The following file describes an example configuration of an Flume agent that uses a [Spooling directory source](http://flume.apache.org/FlumeUserGuide.html#spooling-directory-source), a [File channel](http://flume.apache.org/FlumeUserGuide.html#file-channel) and Stratio JDBC Sink.

``` 
    # Name the components on this agent
    agent.sources = r1
    agent.sinks = jdbcSink
    agent.channels = c1

    # Describe/configure the source
    agent.sources.r1.type = spoolDir
    agent.sources.r1.spoolDir = /home/flume/data/files/

    # Describe the sink
    agent.sinks.jdbcSink.type = com.stratio.ingestion.sink.jdbc.JDBCSink
    agent.sinks.jdbcSink.connectionString = jdbc:h2:/tmp/jdbcsink_test
    agent.sinks.jdbcSink.table = test
    agent.sinks.jdbcSink.batchSize = 10
    # Additional configuration for dbcp
	agent.sinks.jdbcSink.dbcp.maxWait = 10000
	agent.sinks.jdbcSink.dbcp.dbcp.initialSize = 1
	agent.sinks.jdbcSink.dbcp.dbcp.maxActive = 3

    # Use a channel which buffers events in file
    agent.channels = c1
    agent.channels.c1.type = file
    agent.channels.c1.checkpointDir = /home/user/flume/channel/check/
    agent.channels.c1.dataDirs = /home/user/flume/channel/data/
    # Remember, transactionCapacity must be greater than sink.batchSize.
    agent.channels.c1.transactionCapacity=10000 

    # Bind the source and sink to the channel
    agent.sources.r1.channels = c1
    agent.sinks.jdbcSink.channel = c1
```

