/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.study.notification.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.gridsuite.study.notification.server.QuotaWebSocketHandler.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Jon Harper <jon.harper at rte-france.com>
 */
class QuotaWebSocketHandlerTest extends AbstractWebSocketHandlerTest<QuotaWebSocketHandler> {

    @Override
    protected QuotaWebSocketHandler createHandler(ObjectMapper objectMapper, MeterRegistry meterRegistry, int heartbeatInterval) {
        return new QuotaWebSocketHandler(objectMapper, meterRegistry, heartbeatInterval);
    }

    @Override
    protected void consumeBrokerFlux(QuotaWebSocketHandler handler, Flux<Message<String>> flux) {
        handler.consumeNotificationQuota().accept(flux);
    }

    private void setUpUriComponentBuilder(String connectedUserId, String filterUserId) {
        UriComponentsBuilder uriComponentBuilder = UriComponentsBuilder.fromUriString("http://localhost:1234/quota");

        setUpHandshakeInfoHeaders(connectedUserId);

        if (filterUserId != null) {
            uriComponentBuilder.queryParam(QUERY_USER_ID, filterUserId);
        }

        when(handshakeinfo.getUri()).thenReturn(uriComponentBuilder.build().toUri());
    }

    private void withFilters(String filterUserId) {
        String connectedUserId = "userId";
        String otherUserId = "userId2";

        Map<String, Object> filterMap = new HashMap<>();
        when(ws.getAttributes()).thenReturn(filterMap);

        setUpUriComponentBuilder(connectedUserId, filterUserId);

        var quotaWebSocketHandler = createHandler(objectMapper, meterRegistry, Integer.MAX_VALUE);
        var atomicRef = new AtomicReference<FluxSink<Message<String>>>();
        var flux = Flux.create(atomicRef::set);
        consumeBrokerFlux(quotaWebSocketHandler, flux);
        var sink = atomicRef.get();
        quotaWebSocketHandler.handle(ws);

        List<GenericMessage<String>> refMessages = Stream.<Map<String, Object>>of(
                Map.of(HEADER_STUDY_UUID, "foo", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "rab"),

                Map.of(HEADER_STUDY_UUID, "public_" + connectedUserId, HEADER_UPDATE_TYPE, "oof", HEADER_USER_ID, connectedUserId),
                Map.of(HEADER_STUDY_UUID, "public_" + otherUserId, HEADER_UPDATE_TYPE, "rab", HEADER_USER_ID, otherUserId),
                Map.of(HEADER_STUDY_UUID, "public_" + otherUserId, HEADER_UPDATE_TYPE, "rab", HEADER_USER_ID, otherUserId, HEADER_ERROR, "error_message"))
                .map(map -> new GenericMessage<>("", map))
                .collect(Collectors.toList());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Flux<WebSocketMessage>> argument = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument.capture());
        List<String> messages = new ArrayList<>();
        argument.getValue().map(WebSocketMessage::getPayloadAsText).subscribe(messages::add);
        refMessages.forEach(sink::next);
        sink.complete();

        List<Map<String, Object>> expected = refMessages.stream()
                .filter(m -> {
                    String userId = (String) m.getHeaders().get(HEADER_USER_ID);
                    return filterUserId == null || filterUserId.equals(userId);
                })
                .map(GenericMessage::getHeaders)
                .map(QuotaWebSocketHandlerTest::toResultHeader)
                .collect(Collectors.toList());

        List<Map<String, Object>> actual = messages.stream().map(t -> {
            try {
                return toResultHeader(((Map<String, Map<String, Object>>) objectMapper.readValue(t, Map.class)).get("headers"));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        assertEquals(expected, actual);
        assertNotEquals(0, actual.size());
    }

    private static Map<String, Object> toResultHeader(Map<String, Object> messageHeader) {
        var resHeader = new HashMap<String, Object>();
        resHeader.put(HEADER_TIMESTAMP, messageHeader.get(HEADER_TIMESTAMP));
        resHeader.put(HEADER_UPDATE_TYPE, messageHeader.get(HEADER_UPDATE_TYPE));

        passHeaderRef(messageHeader, resHeader, HEADER_STUDY_UUID);
        passHeaderRef(messageHeader, resHeader, HEADER_ERROR);
        passHeaderRef(messageHeader, resHeader, HEADER_USER_ID);

        resHeader.remove(HEADER_TIMESTAMP);

        return resHeader;
    }

    private static void passHeaderRef(Map<String, Object> messageHeader, HashMap<String, Object> resHeader, String headerName) {
        if (messageHeader.get(headerName) != null) {
            resHeader.put(headerName, messageHeader.get(headerName));
        }
    }

    @Test
    void testWithoutFilter() {
        withFilters(null);
    }

    @Test
    void testUserIdFilter() {
        withFilters("userId");
    }

    @Test
    void testOtherUserIdFilter() {
        withFilters("userId2");
    }
}
