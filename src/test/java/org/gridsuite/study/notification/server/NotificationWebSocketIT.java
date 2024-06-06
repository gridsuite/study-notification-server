/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.study.notification.server;

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
import org.springframework.web.reactive.socket.client.StandardWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.gridsuite.study.notification.server.NotificationWebSocketHandler.*;
import static org.junit.Assert.assertEquals;

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

    protected URI getUrl(String path) {
        return URI.create("ws://localhost:" + this.port + path);
    }

    @Test
    public void metricsMapOneUserTwoConnections() {
        WebSocketClient client1 = new StandardWebSocketClient();
        HttpHeaders httpHeaders1 = new HttpHeaders();
        String user = "test";
        httpHeaders1.add(HEADER_USER_ID, user);
        Map<String, Double> exp = Map.of(user, 2d);
        CountDownLatch connectionLatch = new CountDownLatch(2);
        CountDownLatch assertLatch = new CountDownLatch(1);

        Mono<Void> connection1 = client1.execute(getUrl("/notify"), httpHeaders1, ws -> Mono.fromRunnable(() -> handleLatches(connectionLatch, assertLatch)));
        Mono<Void> connection2 = client1.execute(getUrl("/notify"), httpHeaders1, ws -> Mono.fromRunnable(() -> handleLatches(connectionLatch, assertLatch)));

        CompletableFuture<Void> evaluationFuture = evaluateAssert(connectionLatch, exp, assertLatch);
        Mono.zip(connection1, connection2).block();
        evaluationFuture.join(); // Throw assertion errors
    }

    @Test
    public void metricsMapTwoUsers() {
        // First WebSocketClient for connections related to 'test' user
        WebSocketClient client1 = new StandardWebSocketClient();
        HttpHeaders httpHeaders1 = new HttpHeaders();
        String user1 = "test";
        httpHeaders1.add(HEADER_USER_ID, user1);
        String user2 = "test1";
        Map<String, Double> exp = Map.of(user1, 2d, user2, 1d);
        CountDownLatch connectionLatch = new CountDownLatch(3);
        CountDownLatch assertLatch = new CountDownLatch(1);
        Mono<Void> connection1 = client1.execute(getUrl("/notify"), httpHeaders1, ws -> Mono.fromRunnable(() -> handleLatches(connectionLatch, assertLatch)));
        Mono<Void> connection2 = client1.execute(getUrl("/notify"), httpHeaders1, ws -> Mono.fromRunnable(() -> handleLatches(connectionLatch, assertLatch)));

        // Second WebSocketClient for connections related to 'test1' user
        WebSocketClient client2 = new StandardWebSocketClient();
        HttpHeaders httpHeaders2 = new HttpHeaders();
        httpHeaders2.add(HEADER_USER_ID, user2);
        Mono<Void> connection3 = client2.execute(getUrl("/notify"), httpHeaders2, ws -> Mono.fromRunnable(() -> handleLatches(connectionLatch, assertLatch)));

        CompletableFuture<Void> evaluationFuture = evaluateAssert(connectionLatch, exp, assertLatch);
        Mono.zip(connection1, connection2, connection3).block();
        evaluationFuture.join(); // Throw assertion errors
    }

    private void handleLatches(CountDownLatch connectionLatch, CountDownLatch assertLatch) {
        try {
            connectionLatch.countDown();
            assertLatch.await(); // Wait for assertion to be evaluated before closing the connection
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<Void> evaluateAssert(CountDownLatch connectionLatch, Map<String, Double> exp, CountDownLatch assertLatch) {
        return CompletableFuture.runAsync(() -> {
            try {
                connectionLatch.await();  // Wait for connections to be established
                testMeterMap(exp);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                assertLatch.countDown(); // Close connections if there is an assertion error
            }
        });
    }

    private void testMeterMap(Map<String, Double> userMap) {
        for (Map.Entry<String, Double> userEntry : userMap.entrySet()) {
            assertEquals(userEntry.getValue(), meterRegistry.get(USERS_METER_NAME).tag(USER_TAG, userEntry.getKey()).gauge().value(), 0);
        }
    }
}
