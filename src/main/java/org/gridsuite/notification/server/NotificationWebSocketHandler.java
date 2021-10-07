/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.notification.server;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A WebSocketHandler that sends messages from a broker to websockets opened by clients, interleaving with pings to keep connections open.
 * <p>
 * Spring Cloud Stream gets the consumeNotification bean and calls it with the
 * flux from the broker. We call publish and connect to subscribe immediately to the flux
 * and multicast the messages to all connected websockets and to discard the messages when
 * no websockets are connected.
 *
 * @author Jon Harper <jon.harper at rte-france.com>
 */
@Component
public class NotificationWebSocketHandler implements WebSocketHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationWebSocketHandler.class);
    private static final String CATEGORY_BROKER_INPUT = NotificationWebSocketHandler.class.getName() + ".messages.input-broker";
    private static final String CATEGORY_WS_OUTPUT = NotificationWebSocketHandler.class.getName() + ".messages.output-websocket";
    static final String QUERY_STUDY_UUID = "studyUuid";
    static final String QUERY_UPDATE_TYPE = "updateType";
    static final String HEADER_USER_ID = "userId";
    static final String HEADER_STUDY_UUID = "studyUuid";
    static final String HEADER_IS_PUBLIC_STUDY = "isPublicStudy";
    static final String HEADER_STUDY_NAME = "studyName";
    static final String HEADER_UPDATE_TYPE = "updateType";
    static final String HEADER_TIMESTAMP = "timestamp";
    static final String HEADER_ERROR = "error";
    static final String HEADER_SUBSTATIONS_IDS = "substationsIds";
    static final String HEADER_DELETED_EQUIPMENT_ID = "deletedEquipmentId";
    static final String HEADER_DELETED_EQUIPMENT_TYPE = "deletedEquipmentType";
    static final String HEADER_NODES = "nodes";
    static final String HEADER_NODE = "node";
    static final String HEADER_NEW_NODE = "newNode";
    static final String HEADER_REMOVE_CHILDREN = "removeChildren";
    static final String HEADER_INSERT_BEFORE = "insertBefore";

    private ObjectMapper jacksonObjectMapper;

    private int heartbeatInterval;

    public NotificationWebSocketHandler(ObjectMapper jacksonObjectMapper, @Value("${notification.websocket.heartbeat.interval:30}") int heartbeatInterval) {
        this.jacksonObjectMapper = jacksonObjectMapper;
        this.heartbeatInterval = heartbeatInterval;
    }

    Flux<Message<String>> flux;

    @Bean
    public Consumer<Flux<Message<String>>> consumeNotification() {
        return f -> {
            ConnectableFlux<Message<String>> c = f.log(CATEGORY_BROKER_INPUT, Level.FINE).publish();
            this.flux = c;
            c.connect();
            // Force connect 1 fake subscriber to consumme messages as they come.
            // Otherwise, reactorcore buffers some messages (not until the connectable flux had
            // at least one subscriber. Is there a better way ?
            c.subscribe();
        };
    }

    /**
     * map from the broker flux to the filtered flux for one websocket client, extracting only relevant fields.
     */
    private Flux<WebSocketMessage> notificationFlux(WebSocketSession webSocketSession,
                                                    String userId,
                                                    String filterStudyUuid,
                                                    String filterUpdateType) {
        return flux.transform(f -> {
            Flux<Message<String>> res = f;
            if (userId != null) {
                res = res.filter(m -> {
                    var headerIsPublicStudy = m.getHeaders().get(HEADER_IS_PUBLIC_STUDY, Boolean.class);
                    return (m.getHeaders().get(HEADER_ERROR) == null || userId.equals(m.getHeaders().get(HEADER_USER_ID))) &&
                            (headerIsPublicStudy == null || headerIsPublicStudy || userId.equals(m.getHeaders().get(HEADER_USER_ID)));
                });
            }
            if (filterStudyUuid != null) {
                res = res.filter(m -> filterStudyUuid.equals(m.getHeaders().get(HEADER_STUDY_UUID)));
            }
            if (filterUpdateType != null) {
                res = res.filter(m -> filterUpdateType.equals(m.getHeaders().get(HEADER_UPDATE_TYPE)));
            }
            return res;
        }).map(m -> {
            try {
                return jacksonObjectMapper.writeValueAsString(Map.of(
                        "payload", m.getPayload(),
                        "headers", toResultHeader(m.getHeaders())));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).log(CATEGORY_WS_OUTPUT, Level.FINE).map(webSocketSession::textMessage);
    }

    private static Map<String, Object> toResultHeader(Map<String, Object> messageHeader) {
        var resHeader = new HashMap<String, Object>();
        resHeader.put(HEADER_TIMESTAMP, messageHeader.get(HEADER_TIMESTAMP));
        resHeader.put(HEADER_UPDATE_TYPE, messageHeader.get(HEADER_UPDATE_TYPE));

        passHeader(messageHeader, resHeader, HEADER_STUDY_UUID);
        passHeader(messageHeader, resHeader, HEADER_STUDY_NAME);
        passHeader(messageHeader, resHeader, HEADER_ERROR);
        passHeader(messageHeader, resHeader, HEADER_SUBSTATIONS_IDS);
        passHeader(messageHeader, resHeader, HEADER_DELETED_EQUIPMENT_ID);
        passHeader(messageHeader, resHeader, HEADER_DELETED_EQUIPMENT_TYPE);
        passHeader(messageHeader, resHeader, HEADER_NODE);
        passHeader(messageHeader, resHeader, HEADER_INSERT_BEFORE);
        passHeader(messageHeader, resHeader, HEADER_REMOVE_CHILDREN);
        passHeader(messageHeader, resHeader, HEADER_NODES);
        passHeader(messageHeader, resHeader, HEADER_NEW_NODE);

        return resHeader;
    }

    private static void passHeader(Map<String, Object> messageHeader, HashMap<String, Object> resHeader, String headerName) {
        if (messageHeader.get(headerName) != null) {
            resHeader.put(headerName, messageHeader.get(headerName));
        }
    }

    /**
     * A heartbeat flux sending websockets pings
     */
    private Flux<WebSocketMessage> heartbeatFlux(WebSocketSession webSocketSession) {
        return Flux.interval(Duration.ofSeconds(heartbeatInterval)).map(n -> webSocketSession
                .pingMessage(dbf -> dbf.wrap((webSocketSession.getId() + "-" + n).getBytes(StandardCharsets.UTF_8))));
    }

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        var uri = webSocketSession.getHandshakeInfo().getUri();
        String userId = webSocketSession.getHandshakeInfo().getHeaders().getFirst(HEADER_USER_ID);
        MultiValueMap<String, String> parameters = UriComponentsBuilder.fromUri(uri).build(true).getQueryParams();
        String filterStudyUuid = parameters.getFirst(QUERY_STUDY_UUID);
        if (filterStudyUuid != null) {
            try {
                filterStudyUuid = URLDecoder.decode(filterStudyUuid, StandardCharsets.UTF_8.toString());
            } catch (UnsupportedEncodingException e) {
                throw new NotificationServerRuntimeException(e.getMessage());
            }
        }
        String filterUpdateType = parameters.getFirst(QUERY_UPDATE_TYPE);
        LOGGER.debug("New websocket connection for studyName={}, updateType={}", filterStudyUuid, filterUpdateType);
        return webSocketSession
                .send(notificationFlux(webSocketSession, userId, filterStudyUuid, filterUpdateType)
                        .mergeWith(heartbeatFlux(webSocketSession)))
                .and(webSocketSession.receive());
    }
}
