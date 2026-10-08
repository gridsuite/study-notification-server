/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
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
import org.springframework.http.HttpHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.gridsuite.study.notification.server.NotificationWebSocketHandler.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Jon Harper <jon.harper at rte-france.com>
 */
class NotificationWebSocketHandlerTest extends AbstractWebSocketHandlerTest<NotificationWebSocketHandler> {

    @Override
    protected NotificationWebSocketHandler createHandler(ObjectMapper objectMapper, MeterRegistry meterRegistry, int heartbeatInterval) {
        return new NotificationWebSocketHandler(objectMapper, meterRegistry, heartbeatInterval);
    }

    @Override
    protected void consumeBrokerFlux(NotificationWebSocketHandler handler, Flux<Message<String>> flux) {
        handler.consumeNotification().accept(flux);
    }

    @Override
    protected void setUpHandshakeInfoHeaders(String connectedUserId) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(HEADER_USER_ID, connectedUserId);
        when(handshakeinfo.getHeaders()).thenReturn(httpHeaders);
        when(handshakeinfo.getUri()).thenReturn(java.net.URI.create("http://localhost:1234/?studyUuid=study-123"));
    }

    private void setUpUriComponentBuilder(String connectedUserId, String filterStudyUuid, String filterUpdateType) {
        UriComponentsBuilder uriComponentBuilder = UriComponentsBuilder.fromUriString("http://localhost:1234/notify");
        uriComponentBuilder.queryParam(QUERY_STUDY_UUID, filterStudyUuid);

        setUpHandshakeInfoHeaders(connectedUserId);

        if (filterUpdateType != null) {
            uriComponentBuilder.queryParam(QUERY_UPDATE_TYPE, filterUpdateType);
        }

        when(handshakeinfo.getUri()).thenReturn(uriComponentBuilder.build().toUri());
    }

    private void withFilters(String filterStudyUuid, String filterUpdateType) {
        String connectedUserId = "userId";
        Map<String, Object> filterMap = new HashMap<>();
        when(ws.getAttributes()).thenReturn(filterMap);

        setUpUriComponentBuilder(connectedUserId, filterStudyUuid, filterUpdateType);

        var notificationWebSocketHandler = new NotificationWebSocketHandler(objectMapper, meterRegistry, Integer.MAX_VALUE);
        var atomicRef = new AtomicReference<FluxSink<Message<String>>>();
        var flux = Flux.create(atomicRef::set);
        notificationWebSocketHandler.consumeNotification().accept(flux);
        var sink = atomicRef.get();
        notificationWebSocketHandler.handle(ws);

        List<GenericMessage<String>> refMessages = Stream.<Map<String, Object>>of(
                Map.of(HEADER_STUDY_UUID, "foo", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "baz", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "foo", HEADER_UPDATE_TYPE, "rab"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "rab"),
                Map.of(HEADER_STUDY_UUID, "baz", HEADER_UPDATE_TYPE, "rab"),
                Map.of(HEADER_STUDY_UUID, "foo", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "oof"),
                Map.of(HEADER_STUDY_UUID, "baz", HEADER_UPDATE_TYPE, "oof"),

                Map.of(HEADER_STUDY_UUID, "foo bar/bar", HEADER_UPDATE_TYPE, "foobar"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "studies", HEADER_ERROR, "error_message"),
                Map.of(HEADER_STUDY_UUID, "bar", HEADER_UPDATE_TYPE, "rab", HEADER_SUBSTATIONS_IDS, "s1"),

                Map.of(HEADER_STUDY_UUID, "public_" + connectedUserId, HEADER_UPDATE_TYPE, "oof", HEADER_USER_ID, connectedUserId),

                Map.of(HEADER_STUDY_UUID, "nodes", HEADER_UPDATE_TYPE, "insert", HEADER_PARENT_NODE, UUID.randomUUID().toString(), HEADER_NEW_NODE, UUID.randomUUID().toString(), HEADER_INSERT_MODE,
                        true),
                Map.of(HEADER_STUDY_UUID, "nodes", HEADER_UPDATE_TYPE, "update", HEADER_NODES, List.of(UUID.randomUUID().toString())),
                Map.of(HEADER_STUDY_UUID, "nodes", HEADER_UPDATE_TYPE, "update", HEADER_NODE, UUID.randomUUID().toString()),
                Map.of(HEADER_STUDY_UUID, "nodes", HEADER_UPDATE_TYPE, "delete", HEADER_NODES, List.of(UUID.randomUUID().toString()),
                    HEADER_PARENT_NODE, UUID.randomUUID().toString(), HEADER_REMOVE_CHILDREN, true),

                Map.of(HEADER_STUDY_UUID, "", HEADER_UPDATE_TYPE, "indexation_status_updated", HEADER_INDEXATION_STATUS, "INDEXED"))
                .map(map -> new GenericMessage<>("", map))
                .toList();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Flux<WebSocketMessage>> argument = ArgumentCaptor.forClass(Flux.class);
        List<String> messages = new ArrayList<>();

        if (filterStudyUuid != null) {
            verify(ws).send(argument.capture());
            argument.getValue().map(WebSocketMessage::getPayloadAsText).subscribe(messages::add);
            refMessages.forEach(sink::next);
            sink.complete();
        }

        List<Map<String, Object>> expected = refMessages.stream()
                .filter(m -> {
                    String studyUuid = (String) m.getHeaders().get(HEADER_STUDY_UUID);
                    String updateType = (String) m.getHeaders().get(HEADER_UPDATE_TYPE);
                    return (studyUuid.equals(filterStudyUuid)) && (filterUpdateType == null || updateType.equals(filterUpdateType));
                })
                .map(GenericMessage::getHeaders)
                .map(NotificationWebSocketHandlerTest::toResultHeader)
                .toList();

        List<Map<String, Object>> actual = messages.stream().map(t -> {
            try {
                return toResultHeader(((Map<String, Map<String, Object>>) objectMapper.readValue(t, Map.class)).get("headers"));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).toList();
        assertEquals(expected, actual);
    }

    private static Map<String, Object> toResultHeader(Map<String, Object> messageHeader) {
        var resHeader = new HashMap<String, Object>();
        resHeader.put(HEADER_TIMESTAMP, messageHeader.get(HEADER_TIMESTAMP));
        resHeader.put(HEADER_UPDATE_TYPE, messageHeader.get(HEADER_UPDATE_TYPE));

        passHeaderRef(messageHeader, resHeader, HEADER_STUDY_UUID);
        passHeaderRef(messageHeader, resHeader, HEADER_ERROR);
        passHeaderRef(messageHeader, resHeader, HEADER_SUBSTATIONS_IDS);
        passHeaderRef(messageHeader, resHeader, HEADER_NEW_NODE);
        passHeaderRef(messageHeader, resHeader, HEADER_NODE);
        passHeaderRef(messageHeader, resHeader, HEADER_NODES);
        passHeaderRef(messageHeader, resHeader, HEADER_REMOVE_CHILDREN);
        passHeaderRef(messageHeader, resHeader, HEADER_PARENT_NODE);
        passHeaderRef(messageHeader, resHeader, HEADER_INSERT_MODE);
        passHeaderRef(messageHeader, resHeader, HEADER_INDEXATION_STATUS);

        resHeader.remove(HEADER_TIMESTAMP);

        return resHeader;
    }

    private static void passHeaderRef(Map<String, Object> messageHeader, HashMap<String, Object> resHeader, String headerName) {
        if (messageHeader.get(headerName) != null) {
            resHeader.put(headerName, messageHeader.get(headerName));
        }
    }

    @Test
    void testWithoutFilterInUrl() {
        withFilters(null, null);
    }

    @Test
    void testStudyFilterInUrl() {
        withFilters("bar", null);
    }

    @Test
    void testTypeFilterInUrl() {
        withFilters(null, "rab");
    }

    @Test
    void testStudyAndTypeFilterInUrl() {
        withFilters("bar", "rab");
    }

    @Test
    void testEncodingCharactersInUrl() {
        withFilters("foo bar/bar", "foobar");
    }
}
