/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.study.notification.server;

import org.gridsuite.study.notification.server.config.SecurityConfig;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * @author Radouane KHOUADRI {@literal <redouane.khouadri_externe at rte-france.com>}
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HandshakeSecurityTest {

    @Autowired
    WebTestClient webTestClient;

    @LocalServerPort
    private int port;

    @ParameterizedTest(name = "should reject handshake for {0}")
    @ValueSource(strings = {"/notify", "/quota"})
    void shouldRejectHandshake(String path) {
        webTestClient.get()
                .uri(path)
                .exchange()
                .expectStatus()
                .isUnauthorized();
    }

    @ParameterizedTest(name = "should accept authenticated handshake for {0}")
    @ValueSource(strings = {"/notify", "/quota"})
    void shouldAcceptAuthenticatedHandshake(String path) {
        URI uri = URI.create("ws://localhost:" + port + path);

        HttpHeaders headers = new HttpHeaders();
        headers.set(SecurityConfig.HEADER_USER_ID, "user-1");

        assertThatCode(() ->
                new ReactorNettyWebSocketClient()
                        .execute(uri, headers, WebSocketSession::close)
                        .block()
        ).doesNotThrowAnyException();
    }
}
