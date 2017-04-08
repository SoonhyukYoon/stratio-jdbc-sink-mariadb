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

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.joda.time.format.DateTimeFormat;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.InsertSetMoreStep;
import org.jooq.InsertSetStep;
import org.jooq.Meta;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

/**
 * <PRE>
 * 커스텀: Event Body 문자열을 파싱하여 및 Column/Value 조합의 Map 형태로 변환한 다음 이력로그 테이블 Insert 구문으로 매핑
 * https://github.com/Stratio/flume-ingestion/tree/master/stratio-sinks/stratio-jdbc-sink
 * </PRE>
 *
 * @author    윤순혁
 * @version   1.0
 * @see       QueryGenerator
 */
class MappingQueryGenerator implements QueryGenerator {

    private static final Logger log = LoggerFactory.getLogger(MappingQueryGenerator.class);

    private Table<?> table;

    /*
     * Custom Variable : Log content splitter
     */
    private static Splitter logSplitter = Splitter.on( "|" );
    private static Splitter itemSplitter = Splitter.on( "=" ).limit( 2 );

	/*
	 * Custome Variable : Timestamp Convert 관련
	 */
	// Timestamp 속성의 컬럼의 경우 문자열 데이터를 DB 저장 가능 타입으로 변환해야 한다....
	private static final String FULL_DATE_FORMAT = "yyyyMMddHHmmssSSS";

	private static final String SEC_TIME_FORMAT = "yyyyMMddHHmmss";

	/**
	 * Desc : Constructor of MappingQueryGenerator.java class
	 * 
	 * @param dslContext
	 * @param tableName
	 */
    public MappingQueryGenerator(DSLContext dslContext, final String tableName) {
        Meta meta = dslContext.meta();

        for (Table<?> table : meta.getTables()) {
            if (table.getName().equalsIgnoreCase(tableName)) {
                this.table = table;
                break;
            }
        }
        if (this.table == null) {
            throw new JDBCSinkException("Table not found: " + tableName);
        }
    }

    public boolean executeQuery(DSLContext dslContext, final List<Event> events) {
        InsertSetStep<?> insert = dslContext.insertInto(this.table);
        int mappedEvents = 0;
        for (Event event : events) {
            Map<Field<?>, Object> fieldValues = new HashMap<Field<?>, Object>();
            /**
             * 커스텀 로직
             * Log 전문에 대한 파싱을 통해 Key/Value Map을 만들고 결과를 Query에 바인딩 한다
             */
            Map<String, String> data = logSplitter.withKeyValueSeparator( itemSplitter ).split( new String( event.getBody(), StandardCharsets.UTF_8 ) );
            for (Map.Entry<String, String> entry : data.entrySet()) {
                Field<?> field = table.field( entry.getKey() );
                if (field == null) {
                    log.trace("Ignoring field: {}", entry.getKey());
                    continue;
                }
                DataType<?> dataType = field.getDataType();
				if ( dataType.getType().isAssignableFrom( Timestamp.class ) && !Strings.isNullOrEmpty( entry.getValue() ) ) {
                	if ( entry.getValue().length() == FULL_DATE_FORMAT.length() ) {
                		fieldValues.put(field, dataType.convert( DateTimeFormat.forPattern( FULL_DATE_FORMAT ).parseMillis( entry.getValue() ) ));	
                	} else if ( entry.getValue().length() == SEC_TIME_FORMAT.length() ) {
                		fieldValues.put(field, dataType.convert( DateTimeFormat.forPattern( SEC_TIME_FORMAT ).parseMillis( entry.getValue() ) ));
                	}
                } else {
                	fieldValues.put(field, dataType.convert(entry.getValue()));	
                }
            }
            if (fieldValues.isEmpty()) {
                log.debug("Ignoring event, no mapped fields.");
            } else {
                mappedEvents++;
                if (insert instanceof InsertSetMoreStep) {
                    insert = ((InsertSetMoreStep<?>) insert).newRecord();
                    insert = (InsertSetStep<?>)insert.set(fieldValues);
                }
            }
        }
        if (insert instanceof InsertSetMoreStep) {
            int result = ((InsertSetMoreStep<?>) insert).execute();
            if (result != mappedEvents) {
                log.warn("Mapped {} events, inserted {}.", mappedEvents, result);
                return false;
            }
        } else {
            log.debug("No insert.");
        }
        return true;
    }

}
