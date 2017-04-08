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

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JDBCSinkTest {

    // 이력 로그의 한줄을여기에 붙여놓고 테스트 합시다.
    private String body = "SEQ_ID=2015080814045832010aca850|LOG_TIME=20150730140458|LOG_TYPE=CSE|SID=|RESULT_CODE=2000|REQ_TIME=20150730140457596|RSP_TIME=20150730140458230|CLIENT_IP=127.0.0.1|DEV_INFO=|OS_INFO=|NW_INFO=|SVC_NAME=|DEV_MODEL=|CARRIER_TYPE=E|SVR_ID=001|RES_TYPE=|ENT_ID=|PDH_RSP_CD=|PRTC_TYPE=|REQ_ID=|OPR_TYPE=|TRG_ADDR=|TRST_TYPE=|RST_CNT_TYPE=|RSP_TYPE=|MGMT_TYPE=|DISCVR=|LONG_POLL=|GRP_REQ_ID=|EQ_TIME=";

    @Test
    public void mappedWithMariaDB() throws Exception {

        Context ctx = new Context();
        ctx.put("driver", "org.mariadb.jdbc.Driver");
        ctx.put("connectionString", "jdbc:mariadb://106.103.234.62:3306/iotp");
        ctx.put("table", "TB_LO_HIST_LOG");
        ctx.put("sqlDialect", "MARIADB");
        ctx.put("batchSize", "1");
        ctx.put("username", "iotp");
        ctx.put("password", "iotp");
        ctx.put("dbcp.maxWait", "10000");
        ctx.put("dbcp.initialSize", "1");
        ctx.put("dbcp.maxActive", "3");
        ctx.put("dbcp.testOnBorrow", "true");
        ctx.put("dbcp.validationQuery", "SELECT 1");
        ctx.put("dbcp.testWhileIdle", "true");
        ctx.put("dbcp.minEvictableIdleTimeMillis", "55000");
        ctx.put("dbcp.timeBetweenEvictionRunsMillis", "34000");      

        // 주의 : sql 설정 절대 금지!!

        JDBCSink jdbcSink = new JDBCSink();

        Configurables.configure(jdbcSink, ctx);

        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");

        Channel channel = new MemoryChannel();
        channel.setName("junitChannel");
        Configurables.configure(channel, channelContext);

        jdbcSink.setChannel(channel);

        channel.start();
        jdbcSink.start();

        Transaction tx = channel.getTransaction();
        tx.begin();

        Map<String, String> headers = new HashMap<String, String>();

        Event event = EventBuilder.withBody(body.getBytes(), headers);
        channel.put(event);

        tx.commit();
        tx.close();

        jdbcSink.process();

        jdbcSink.stop();
        channel.stop();
    }

}
