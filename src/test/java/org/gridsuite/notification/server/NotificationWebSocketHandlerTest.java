/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.notification.server;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

/**
 * @author Jon Harper <jon.harper at rte-france.com>
 */
public class NotificationWebSocketHandlerTest {

    private ObjectMapper objectMapper;
    private WebSocketSession ws;
    private HandshakeInfo handshakeinfo;
    private Flux<Message<String>> flux;

    @Before
    public void setup() {
        objectMapper = new ObjectMapper();
        var dataBufferFactory = new DefaultDataBufferFactory();

        ws = Mockito.mock(WebSocketSession.class);
        handshakeinfo = Mockito.mock(HandshakeInfo.class);

        when(ws.getHandshakeInfo()).thenReturn(handshakeinfo);
        when(ws.receive()).thenReturn(Flux.empty());
        when(ws.send(any())).thenReturn(Mono.empty());
        when(ws.textMessage(any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            String str = (String) args[0];
            return new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(str.getBytes()));
        });
        when(ws.pingMessage(any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Function<DataBufferFactory, DataBuffer> f = (Function<DataBufferFactory, DataBuffer>) args[0];
            return new WebSocketMessage(WebSocketMessage.Type.PING, f.apply(dataBufferFactory));
        });
        when(ws.getId()).thenReturn("testsession");

    }

    private void withFilters(String filterStudyName, String filterUpdateType) {
        var notificationWebSocketHandler = new NotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE);

        UriComponentsBuilder uriComponentBuilder = UriComponentsBuilder.fromUriString("http://localhost:1234/notify");

        if (filterStudyName != null) {
            uriComponentBuilder.queryParam("studyName", filterStudyName);
        }
        if (filterUpdateType != null) {
            uriComponentBuilder.queryParam("updateType", filterUpdateType);
        }

        when(handshakeinfo.getUri()).thenReturn(uriComponentBuilder.build().toUri());

        var atomicRef = new AtomicReference<FluxSink<Message<String>>>();
        var flux = Flux.create(atomicRef::set);
        notificationWebSocketHandler.consumeNotification().accept(flux);
        var sink = atomicRef.get();

        notificationWebSocketHandler.handle(ws);

        List<Map<String, Object>> refMessages = Arrays.asList(
                Map.of("studyName", "foo", "updateType", "oof"),
                Map.of("studyName", "bar", "updateType", "oof"),
                Map.of("studyName", "baz", "updateType", "oof"),
                Map.of("studyName", "foo", "updateType", "rab"),
                Map.of("studyName", "bar", "updateType", "rab"),
                Map.of("studyName", "baz", "updateType", "rab"),
                Map.of("studyName", "foo", "updateType", "oof"),
                Map.of("studyName", "bar", "updateType", "oof"),
                Map.of("studyName", "baz", "updateType", "oof")
        );

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Flux<WebSocketMessage>> argument = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument.capture());
        List<String> messages = new ArrayList<String>();
        argument.getValue().map(WebSocketMessage::getPayloadAsText).log().subscribe(messages::add);
        refMessages.stream().map(headers -> new GenericMessage<String>("", headers)).forEach(sink::next);
        sink.complete();

        List<Map<String, Object>> expected = refMessages.stream().filter(headers -> {
            String name = (String) headers.get("studyName");
            String type = (String) headers.get("updateType");
            return (filterStudyName == null || filterStudyName.equals(name))
                    && (filterUpdateType == null || filterUpdateType.equals(type));
        }).collect(Collectors.toList());
        List<Map<String, Object>> actual = messages.stream().map(t -> {
            try {
                var deserializedHeaders = ((Map<String, Map<String, Object>>) objectMapper.readValue(t, Map.class))
                        .get("headers");
                return Map.of("studyName", deserializedHeaders.get("studyName"), "updateType",
                        deserializedHeaders.get("updateType"));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    @Test
    public void testWithoutFilter() {
        withFilters(null, null);
    }

    @Test
    public void testStudyFilter() {
        withFilters("bar", null);
    }

    @Test
    public void testTypeFilter() {
        withFilters(null, "rab");
    }

    @Test
    public void testStudyAndTypeFilter() {
        withFilters("bar", "rab");
    }

    @Test
    public void testEncodingCharacters() {
        withFilters("foo bar/bar", null);
    }

    @Test
    public void testHeartbeat() {
        var notificationWebSocketHandler = new NotificationWebSocketHandler(null, 1);

        UriComponentsBuilder uriComponentBuilder = UriComponentsBuilder.fromUriString("http://localhost:1234/notify");
        when(handshakeinfo.getUri()).thenReturn(uriComponentBuilder.build().toUri());
        var flux = Flux.<Message<String>>empty();
        notificationWebSocketHandler.consumeNotification().accept(flux);
        notificationWebSocketHandler.handle(ws);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Flux<WebSocketMessage>> argument = ArgumentCaptor.forClass(Flux.class);
        verify(ws).send(argument.capture());
        assertEquals("testsession-0", argument.getValue().blockFirst(Duration.ofSeconds(10)).getPayloadAsText());
    }

}
