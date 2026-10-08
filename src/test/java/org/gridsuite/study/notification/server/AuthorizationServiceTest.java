/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.study.notification.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.gridsuite.study.notification.server.authorization.AuthorizationService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.Authentication;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.Principal;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.gridsuite.study.notification.server.AbstractWebSocketHandler.HEADER_USER_ID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Radouane KHOUADRI {@literal <redouane.khouadri_externe at rte-france.com>}
 */
class AuthorizationServiceTest {

    private HttpServer httpServer;
    private AuthorizationService authorizationService;
    private final AtomicReference<String> requestPath = new AtomicReference<>();
    private final AtomicReference<String> userIdHeader = new AtomicReference<>();

    @BeforeEach
    void setUp() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        int port = httpServer.getAddress().getPort();

        httpServer.createContext("/v1/elements/authorized", this::handleAuthorizationRequest);
        httpServer.start();

        authorizationService = new AuthorizationService(
                WebClient.builder(),
                "http://localhost:" + port,
                "/v1/elements/authorized"
        );
    }

    @AfterEach
    void tearDown() {
        httpServer.stop(0);
    }

    @Test
    void shouldReturnTrueWhenAuthorizationSucceeds() {
        UUID studyUuid = UUID.randomUUID();

        httpServer.removeContext("/v1/elements/authorized");
        httpServer.createContext(
                "/v1/elements/authorized",
                exchange -> respond(exchange, 204)
        );

        Boolean result = authorizationService
                .canReadStudy(websocketSession(studyUuid))
                .block();

        assertThat(result).isTrue();
    }

    @Test
    void shouldSendExpectedRequest() {
        UUID studyUuid = UUID.randomUUID();

        Boolean result = authorizationService
                .canReadStudy(websocketSession(studyUuid, authenticatedUser("user-123")))
                .block();

        assertThat(result).isTrue();
        assertThat(requestPath.get())
                .isEqualTo("/v1/elements/authorized"
                        + "?accessType=READ"
                        + "&ids=" + studyUuid);
        assertThat(userIdHeader.get()).isEqualTo("user-123");
    }

    @Test
    void shouldReturnFalseWhenServerReturnsError() {
        httpServer.removeContext("/v1/elements/authorized");
        httpServer.createContext(
                "/v1/elements/authorized",
                exchange -> respond(exchange, 403)
        );

        Boolean result = authorizationService
                .canReadStudy(websocketSession(UUID.randomUUID()))
                .block();

        assertThat(result).isFalse();
    }

    @Test
    void shouldReturnFalseWhenPrincipalIsMissing() {
        Boolean result = authorizationService
                .canReadStudy(websocketSessionWithoutPrincipal(UUID.randomUUID()))
                .block();

        assertThat(result).isFalse();
    }

    @Test
    void shouldReturnFalseWhenUserIsNotAuthenticated() {
        Authentication authentication = mock(Authentication.class);
        when(authentication.isAuthenticated()).thenReturn(false);

        Boolean result = authorizationService
                .canReadStudy(websocketSession(UUID.randomUUID(), authentication))
                .block();

        assertThat(result).isFalse();
    }

    @Test
    void shouldReturnFalseWhenStudyUuidIsInvalid() {
        Boolean result = authorizationService
                .canReadStudy(websocketSession(
                        "ws://localhost/notifications?studyUuid=invalid"
                ))
                .block();

        assertThat(result).isFalse();
    }

    private void handleAuthorizationRequest(HttpExchange exchange) throws IOException {
        requestPath.set(exchange.getRequestURI().toString());
        userIdHeader.set(exchange.getRequestHeaders().getFirst(HEADER_USER_ID));

        respond(exchange, 204);
    }

    private static void respond(HttpExchange exchange, int statusCode) throws IOException {
        exchange.sendResponseHeaders(statusCode, -1);
        exchange.close();
    }

    private static Authentication authenticatedUser(String userId) {
        Authentication authentication = mock(Authentication.class);

        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn(userId);

        return authentication;
    }

    private static WebSocketSession websocketSession(UUID studyUuid) {
        return websocketSession(studyUuid, authenticatedUser("user-123"));
    }

    private static WebSocketSession websocketSession(UUID studyUuid, Authentication authentication) {
        return websocketSession(
                "ws://localhost/notifications?studyUuid=" + studyUuid,
                Mono.just(authentication)
        );
    }

    private static WebSocketSession websocketSessionWithoutPrincipal(UUID studyUuid) {
        return websocketSession(
                "ws://localhost/notifications?studyUuid=" + studyUuid,
                Mono.empty()
        );
    }

    private static WebSocketSession websocketSession(String uri) {
        return websocketSession(
                uri,
                Mono.just(authenticatedUser("user-123"))
        );
    }

    private static WebSocketSession websocketSession(String uri, Mono<Principal> principal) {
        WebSocketSession session = mock(WebSocketSession.class);
        HandshakeInfo handshakeInfo = mock(HandshakeInfo.class);

        when(session.getHandshakeInfo()).thenReturn(handshakeInfo);
        when(handshakeInfo.getUri()).thenReturn(URI.create(uri));
        when(handshakeInfo.getPrincipal()).thenReturn(principal);

        return session;
    }
}
