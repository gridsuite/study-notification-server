/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.study.notification.server;

import org.gridsuite.study.notification.server.authorization.AuthorizationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.gridsuite.study.notification.server.NotificationWebSocketHandler.FILTER_STUDY_UUID;
import static org.gridsuite.study.notification.server.config.SecurityConfig.HEADER_USER_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Radouane KHOUADRI {@literal <redouane.khouadri_externe at rte-france.com>}
 */
@SpringBootTest(classes = {NotificationApplication.class})
class NotificationWebSocketHandlerPreAuthorizeTest {

    private static final String STUDY_UUID = "study-1";
    public static final String USER_1 = "user-1";
    public static final String USER_2 = "user-2";

    @Autowired
    private NotificationWebSocketHandler handler;
    @MockitoBean
    private AuthorizationService authorizationService;

    private static WebSocketSession mockSession(String userId, String query) {
        WebSocketSession session = mock(WebSocketSession.class);

        HttpHeaders headers = new HttpHeaders();
        headers.set(HEADER_USER_ID, userId);
        HandshakeInfo handshakeInfo = new HandshakeInfo(
                URI.create("ws://localhost/notify" + query), headers, Mono.empty(), null);

        Map<String, Object> attributes = new ConcurrentHashMap<>();

        when(session.getHandshakeInfo()).thenReturn(handshakeInfo);
        when(session.getId()).thenReturn("session-" + userId);
        when(session.getAttributes()).thenReturn(attributes);
        when(session.send(any())).thenReturn(Mono.empty());
        when(session.receive()).thenReturn(Flux.empty());
        return session;
    }

    private static Authentication authenticated(String userId) {
        return new UsernamePasswordAuthenticationToken(userId, null, List.of());
    }

    private Mono<Void> handleAs(Authentication authentication, WebSocketSession session) {
        return handler.handle(session)
                .contextWrite(ReactiveSecurityContextHolder.withAuthentication(authentication));
    }

    @Test
    void shouldAllowWhenUserCanReadStudy() {
        when(authorizationService.canReadStudy(any(), any())).thenReturn(Mono.just(true));
        WebSocketSession session = mockSession(USER_1, "?studyUuid=" + STUDY_UUID);

        assertDoesNotThrow(() -> handleAs(authenticated(USER_1), session).block(Duration.ofSeconds(5)));

        verify(authorizationService).canReadStudy(
                argThat(auth -> USER_1.equals(auth.getName())),
                same(session));

        assertEquals(STUDY_UUID, session.getAttributes().get(FILTER_STUDY_UUID));
        verify(session).send(any());
        verify(session).receive();
    }

    @Test
    void shouldDenyWhenUserCannotReadStudy() {
        when(authorizationService.canReadStudy(any(), any())).thenReturn(Mono.just(false));
        WebSocketSession session = mockSession(USER_2, "?studyUuid=" + STUDY_UUID);

        Mono<Void> result = handleAs(authenticated(USER_2), session);
        Duration duration = Duration.ofSeconds(5);
        assertThrows(AccessDeniedException.class, () -> result.block(duration));

        verify(authorizationService).canReadStudy(
                argThat(auth -> USER_2.equals(auth.getName())),
                same(session));

        assertTrue(session.getAttributes().isEmpty());
        verify(session, never()).send(any());
        verify(session, never()).receive();
    }

    @Test
    void shouldDenyWhenAuthorizationServiceFails() {
        when(authorizationService.canReadStudy(any(), any()))
                .thenReturn(Mono.error(new IllegalStateException("directory-server down")));
        WebSocketSession session = mockSession(USER_1, "?studyUuid=" + STUDY_UUID);

        Mono<Void> result = handleAs(authenticated(USER_1), session);
        Duration duration = Duration.ofSeconds(5);
        assertThrows(RuntimeException.class, () -> result.block(duration));
        verify(session, never()).send(any());
    }

    @Test
    void shouldDenyWithoutSecurityContext() {
        when(authorizationService.canReadStudy(any(), any())).thenReturn(Mono.just(false));
        WebSocketSession session = mockSession(USER_1, "?studyUuid=" + STUDY_UUID);

        Mono<Void> result = handler.handle(session);
        Duration duration = Duration.ofSeconds(5);
        assertThrows(RuntimeException.class, () -> result.block(duration));
        verify(session, never()).send(any());
    }
}
