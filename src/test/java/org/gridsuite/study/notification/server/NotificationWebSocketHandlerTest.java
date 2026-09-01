/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.study.notification.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.micrometer.core.instrument.MeterRegistry;
import org.gridsuite.study.notification.server.dto.Filters;
import org.gridsuite.study.notification.server.dto.FiltersToAdd;
import org.gridsuite.study.notification.server.dto.FiltersToRemove;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.gridsuite.study.notification.server.NotificationWebSocketHandler.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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

    private void setUpUriComponentBuilder(String connectedUserId) {
        setUpUriComponentBuilder(connectedUserId, null, null);
    }

    private void setUpUriComponentBuilder(String connectedUserId, String filterStudyUuid, String filterUpdateType) {
        UriComponentsBuilder uriComponentBuilder = UriComponentsBuilder.fromUriString("http://localhost:1234/notify");

        setUpHandshakeInfoHeaders(connectedUserId);

        if (filterStudyUuid != null) {
            uriComponentBuilder.queryParam(QUERY_STUDY_UUID, filterStudyUuid);
        }
        if (filterUpdateType != null) {
            uriComponentBuilder.queryParam(QUERY_UPDATE_TYPE, filterUpdateType);
        }

        when(handshakeinfo.getUri()).thenReturn(uriComponentBuilder.build().toUri());
    }

    private void withFilters(String filterStudyUuid, String filterUpdateType, boolean inUrl) {
        String connectedUserId = "userId";
        String otherUserId = "userId2";

        Map<String, Object> filterMap = new HashMap<>();
        when(ws.getAttributes()).thenReturn(filterMap);

        if (inUrl) {
            setUpUriComponentBuilder(connectedUserId, filterStudyUuid, filterUpdateType);
        } else {
            setUpUriComponentBuilder(connectedUserId);
        }

        var notificationWebSocketHandler = new NotificationWebSocketHandler(objectMapper, meterRegistry, Integer.MAX_VALUE);
        var atomicRef = new AtomicReference<FluxSink<Message<String>>>();
        var flux = Flux.create(atomicRef::set);
        notificationWebSocketHandler.consumeNotification().accept(flux);
        var sink = atomicRef.get();
        notificationWebSocketHandler.handle(ws);

        if (!inUrl) {
            if (filterUpdateType != null) {
                filterMap.put(FILTER_UPDATE_TYPE, filterUpdateType);
            }
            if (filterStudyUuid != null) {
                filterMap.put(FILTER_STUDY_UUID, filterStudyUuid);
            }
        }

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
                Map.of(HEADER_STUDY_UUID, "public_" + otherUserId, HEADER_UPDATE_TYPE, "rab", HEADER_USER_ID, otherUserId),
                Map.of(HEADER_STUDY_UUID, "public_" + otherUserId, HEADER_UPDATE_TYPE, "rab", HEADER_USER_ID, otherUserId, HEADER_ERROR, "error_message"),

                Map.of(HEADER_STUDY_UUID, "nodes", HEADER_UPDATE_TYPE, "insert", HEADER_PARENT_NODE, UUID.randomUUID().toString(), HEADER_NEW_NODE, UUID.randomUUID().toString(), HEADER_INSERT_MODE,
                        true),
                Map.of(HEADER_STUDY_UUID, "nodes", HEADER_UPDATE_TYPE, "update", HEADER_NODES, List.of(UUID.randomUUID().toString())),
                Map.of(HEADER_STUDY_UUID, "nodes", HEADER_UPDATE_TYPE, "update", HEADER_NODE, UUID.randomUUID().toString()),
                Map.of(HEADER_STUDY_UUID, "nodes", HEADER_UPDATE_TYPE, "delete", HEADER_NODES, List.of(UUID.randomUUID().toString()),
                    HEADER_PARENT_NODE, UUID.randomUUID().toString(), HEADER_REMOVE_CHILDREN, true),

                Map.of(HEADER_STUDY_UUID, "", HEADER_UPDATE_TYPE, "indexation_status_updated", HEADER_INDEXATION_STATUS, "INDEXED"))
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
                    String studyUuid = (String) m.getHeaders().get(HEADER_STUDY_UUID);
                    String updateType = (String) m.getHeaders().get(HEADER_UPDATE_TYPE);
                    System.out.println((filterStudyUuid == null || filterStudyUuid.equals(studyUuid))
                            && (filterUpdateType == null || filterUpdateType.equals(updateType)));
                    return (filterStudyUuid == null || filterStudyUuid.equals(studyUuid)) && (filterUpdateType == null || filterUpdateType.equals(updateType));
                })
                .map(GenericMessage::getHeaders)
                .map(NotificationWebSocketHandlerTest::toResultHeader)
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
    void testWithoutFilterInBody() {
        withFilters(null, null, false);
    }

    @Test
    void testWithoutFilterInUrl() {
        withFilters(null, null, true);
    }

    @Test
    void testStudyFilterInBody() {
        withFilters("bar", null, false);
    }

    @Test
    void testStudyFilterInUrl() {
        withFilters("bar", null, true);
    }

    @Test
    void testTypeFilterInBody() {
        withFilters(null, "rab", false);
    }

    @Test
    void testTypeFilterInUrl() {
        withFilters(null, "rab", true);
    }

    @Test
    void testStudyAndTypeFilterInBody() {
        withFilters("bar", "rab", false);
    }

    @Test
    void testStudyAndTypeFilterInUrl() {
        withFilters("bar", "rab", true);
    }

    @Test
    void testEncodingCharactersInBody() {
        withFilters("foo bar/bar", "foobar", false);
    }

    @Test
    void testEncodingCharactersInUrl() {
        withFilters("foo bar/bar", "foobar", true);
    }

    @Test
    void testWsReceiveFilters() throws Exception {
        setUpUriComponentBuilder("userId");
        var dataBufferFactory = new DefaultDataBufferFactory();

        var map = new ConcurrentHashMap<String, Object>();
        FiltersToAdd filtersToAdd = new FiltersToAdd("updateTypeFilter", "studyUuid");
        FiltersToRemove filtersToRemove = new FiltersToRemove(false, null);
        Filters filters = new Filters(filtersToAdd, filtersToRemove);
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(filters);
        when(ws2.receive()).thenReturn(Flux.just(new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(json.getBytes()))));
        when(ws2.getAttributes()).thenReturn(map);

        var notificationWebSocketHandler = new NotificationWebSocketHandler(new ObjectMapper(), meterRegistry, 60);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeNotification().accept(flux);
        notificationWebSocketHandler.receive(ws2).subscribe();

        assertEquals("updateTypeFilter", map.get(FILTER_UPDATE_TYPE));
        assertEquals("studyUuid", map.get(FILTER_STUDY_UUID));
    }

    @Test
    void testWsRemoveFilters() throws Exception {
        setUpUriComponentBuilder("userId");
        var dataBufferFactory = new DefaultDataBufferFactory();

        var map = new ConcurrentHashMap<String, Object>();
        map.put(FILTER_UPDATE_TYPE, "updateType");
        map.put(FILTER_STUDY_UUID, "studyUuid");
        FiltersToAdd filtersToAdd = new FiltersToAdd();
        FiltersToRemove filtersToRemove = new FiltersToRemove(true, true);
        Filters filters = new Filters(filtersToAdd, filtersToRemove);
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(filters);
        when(ws2.receive()).thenReturn(Flux.just(new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(json.getBytes()))));
        when(ws2.getAttributes()).thenReturn(map);

        assertEquals("updateType", ws2.getAttributes().get(FILTER_UPDATE_TYPE));
        assertEquals("studyUuid", ws2.getAttributes().get(FILTER_STUDY_UUID));
        var notificationWebSocketHandler = new NotificationWebSocketHandler(new ObjectMapper(), meterRegistry, Integer.MAX_VALUE);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeNotification().accept(flux);
        notificationWebSocketHandler.receive(ws2).subscribe();

        assertNull(ws2.getAttributes().get(FILTER_UPDATE_TYPE));
        assertNull(ws2.getAttributes().get(FILTER_STUDY_UUID));
    }

    @Test
    void testWsReceiveEmptyFilters() throws Exception {
        setUpUriComponentBuilder("userId");
        var dataBufferFactory = new DefaultDataBufferFactory();

        var map = new ConcurrentHashMap<String, Object>();
        Filters filters = new Filters();
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(filters);
        when(ws2.receive()).thenReturn(Flux.just(new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(json.getBytes()))));
        when(ws2.getAttributes()).thenReturn(map);

        var notificationWebSocketHandler = new NotificationWebSocketHandler(new ObjectMapper(), meterRegistry, Integer.MAX_VALUE);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeNotification().accept(flux);
        notificationWebSocketHandler.receive(ws2).subscribe();

        assertNull(map.get(FILTER_UPDATE_TYPE));
        assertNull(map.get(FILTER_STUDY_UUID));
    }

    @Test
    void testWsReceiveUnprocessableFilter() {
        setUpUriComponentBuilder("userId");
        var dataBufferFactory = new DefaultDataBufferFactory();

        var map = new ConcurrentHashMap<String, Object>();
        when(ws2.receive()).thenReturn(Flux.just(new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap("UnprocessableFilter".getBytes()))));
        when(ws2.getAttributes()).thenReturn(map);

        var notificationWebSocketHandler = new NotificationWebSocketHandler(new ObjectMapper(), meterRegistry, 60);
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeNotification().accept(flux);
        notificationWebSocketHandler.receive(ws2).subscribe();

        assertNull(map.get(FILTER_UPDATE_TYPE));
        assertNull(map.get(FILTER_STUDY_UUID));
    }
}
