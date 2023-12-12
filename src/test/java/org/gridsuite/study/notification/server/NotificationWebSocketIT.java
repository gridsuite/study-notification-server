/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.study.notification.server;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.StandardWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Mono;

import java.net.URI;

import static org.gridsuite.study.notification.server.NotificationWebSocketHandler.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Jon Harper <jon.harper at rte-france.com>
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = { NotificationApplication.class })
@DirtiesContext
public class NotificationWebSocketIT {

    @LocalServerPort
    private String port;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    public void echo() {
        WebSocketClient client = new StandardWebSocketClient();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(HEADER_USER_ID, "test");
        client.execute(getUrl("/notify"), httpHeaders, ws -> Mono.empty()).block();
    }

    @Test
    public void metrics() {
        WebSocketClient client = new StandardWebSocketClient();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(HEADER_USER_ID, "test");
        client.execute(getUrl("/notify"), httpHeaders, new WebSocketHandlerTestConnections())
                .doFinally(s -> {
                    testMeter(USERS_METER_NAME, 0);
                    testMeter(CONNECTIONS_METER_NAME, 0);
                })
                .block();
    }

    protected URI getUrl(String path) {
        return URI.create("ws://localhost:" + this.port + path);
    }

    private class WebSocketHandlerTestConnections implements WebSocketHandler {
        @Override
        public Mono<Void> handle(WebSocketSession webSocketSession) {
            return Mono.fromRunnable(() -> {
                testMeter(USERS_METER_NAME, 1);
                testMeter(CONNECTIONS_METER_NAME, 1);
            });
        }
    }

    private void testMeter(String name, int val) {
        Meter meter = meterRegistry.get(name).meter();
        assertNotNull(meter);
        assertEquals(val, Double.valueOf(meter.measure().iterator().next().getValue()).intValue());
    }
}
