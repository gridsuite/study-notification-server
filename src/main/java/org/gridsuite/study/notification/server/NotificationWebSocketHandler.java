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
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import org.gridsuite.study.notification.server.dto.Filters;
import org.gridsuite.study.notification.server.dto.FiltersToAdd;
import org.gridsuite.study.notification.server.dto.FiltersToRemove;
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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Level;

import static java.util.stream.Collectors.toList;

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
    static final String FILTER_STUDY_UUID = QUERY_STUDY_UUID;
    static final String QUERY_UPDATE_TYPE = "updateType";
    static final String FILTER_UPDATE_TYPE = QUERY_UPDATE_TYPE;
    static final String HEADER_USER_ID = "userId";
    static final String HEADER_STUDY_UUID = "studyUuid";
    static final String HEADER_UPDATE_TYPE = "updateType";
    static final String HEADER_TIMESTAMP = "timestamp";
    static final String HEADER_ERROR = "error";
    static final String HEADER_SUBSTATIONS_IDS = "substationsIds";
    static final String HEADER_NODE = "node";
    static final String HEADER_NODES = "nodes";
    static final String HEADER_PARENT_NODE = "parentNode";
    static final String HEADER_NEW_NODE = "newNode";
    static final String HEADER_MOVED_NODE = "movedNode";
    static final String HEADER_REMOVE_CHILDREN = "removeChildren";
    static final String HEADER_INSERT_MODE = "insertMode";
    static final String HEADER_REFERENCE_NODE_UUID = "referenceNodeUuid";
    static final String HEADER_INDEXATION_STATUS = "indexation_status";
    static final String USERS_METER_NAME = "app.users";
    static final String USER_TAG = "user";

    private final ObjectMapper jacksonObjectMapper;
    public static final String HEADER_PARAMS_NAME = "paramsName";

    private final int heartbeatInterval;

    private final Map<String, Integer> userConnections = new ConcurrentHashMap<>();

    private final MultiGauge multiGauge;

    public NotificationWebSocketHandler(ObjectMapper jacksonObjectMapper, MeterRegistry meterRegistry, @Value("${notification.websocket.heartbeat.interval:30}") int heartbeatInterval) {
        this.jacksonObjectMapper = jacksonObjectMapper;
        this.heartbeatInterval = heartbeatInterval;
        this.multiGauge = MultiGauge.builder(USERS_METER_NAME).description("The current number of connections per user").register(meterRegistry);
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
            // at least one subscriber). Is there a better way ?
            c.subscribe();
        };
    }

    /**
     * map from the broker flux to the filtered flux for one websocket client, extracting only relevant fields.
     */
    private Flux<WebSocketMessage> notificationFlux(WebSocketSession webSocketSession) {
        return flux.filter(message -> {
            String filterStudyUuid = (String) webSocketSession.getAttributes().get(FILTER_STUDY_UUID);
            return filterStudyUuid == null || filterStudyUuid.equals(message.getHeaders().get(HEADER_STUDY_UUID));
        }).filter(message -> {
            String filterUpdateType = (String) webSocketSession.getAttributes().get(FILTER_UPDATE_TYPE);
            return filterUpdateType == null || filterUpdateType.equals(message.getHeaders().get(HEADER_UPDATE_TYPE));
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
        passHeader(messageHeader, resHeader, HEADER_ERROR);
        passHeader(messageHeader, resHeader, HEADER_SUBSTATIONS_IDS);
        passHeader(messageHeader, resHeader, HEADER_PARENT_NODE);
        passHeader(messageHeader, resHeader, HEADER_INSERT_MODE);
        passHeader(messageHeader, resHeader, HEADER_REMOVE_CHILDREN);
        passHeader(messageHeader, resHeader, HEADER_NODE);
        passHeader(messageHeader, resHeader, HEADER_NODES);
        passHeader(messageHeader, resHeader, HEADER_NEW_NODE);
        passHeader(messageHeader, resHeader, HEADER_MOVED_NODE);
        passHeader(messageHeader, resHeader, HEADER_USER_ID); // to filter the display of error messages in the front end
        passHeader(messageHeader, resHeader, HEADER_REFERENCE_NODE_UUID);
        passHeader(messageHeader, resHeader, HEADER_INDEXATION_STATUS);
        passHeader(messageHeader, resHeader, HEADER_PARAMS_NAME);

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

    public Flux<WebSocketMessage> receive(WebSocketSession webSocketSession) {
        return webSocketSession.receive()
                .doOnNext(webSocketMessage -> {
                    try {
                        //if it's not the heartbeat
                        if (webSocketMessage.getType().equals(WebSocketMessage.Type.TEXT)) {
                            String wsPayload = webSocketMessage.getPayloadAsText();
                            LOGGER.debug("Message received : {} by session {}", wsPayload, webSocketSession.getId());
                            Filters receivedFilters = jacksonObjectMapper.readValue(webSocketMessage.getPayloadAsText(), Filters.class);
                            handleReceivedFilters(webSocketSession, receivedFilters);
                        }
                    } catch (JsonProcessingException e) {
                        LOGGER.error(e.toString(), e);
                    }
                });
    }

    private void handleReceivedFilters(WebSocketSession webSocketSession, Filters filters) {
        if (filters.getFiltersToRemove() != null) {
            FiltersToRemove filtersToRemove = filters.getFiltersToRemove();
            if (Boolean.TRUE.equals(filtersToRemove.getRemoveUpdateType())) {
                webSocketSession.getAttributes().remove(FILTER_UPDATE_TYPE);
            }
            if (Boolean.TRUE.equals(filtersToRemove.getRemoveStudyUuid())) {
                webSocketSession.getAttributes().remove(FILTER_STUDY_UUID);
            }
        }
        if (filters.getFiltersToAdd() != null) {
            FiltersToAdd filtersToAdd = filters.getFiltersToAdd();
            //because null is not allowed in ConcurrentHashMap and will cause the websocket to close
            if (filtersToAdd.getUpdateType() != null) {
                webSocketSession.getAttributes().put(FILTER_UPDATE_TYPE, filtersToAdd.getUpdateType());
            }
            if (filtersToAdd.getStudyUuid() != null) {
                webSocketSession.getAttributes().put(FILTER_STUDY_UUID, filtersToAdd.getStudyUuid());
            }
        }
    }

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        var uri = webSocketSession.getHandshakeInfo().getUri();
        MultiValueMap<String, String> parameters = UriComponentsBuilder.fromUri(uri).build(true).getQueryParams();
        String filterStudyUuid = parameters.getFirst(QUERY_STUDY_UUID);
        if (filterStudyUuid != null) {
            try {
                filterStudyUuid = URLDecoder.decode(filterStudyUuid, StandardCharsets.UTF_8.toString());
                webSocketSession.getAttributes().put(FILTER_STUDY_UUID, filterStudyUuid);
            } catch (UnsupportedEncodingException e) {
                throw new NotificationServerRuntimeException(e.getMessage());
            }
        }
        String filterUpdateType = parameters.getFirst(QUERY_UPDATE_TYPE);
        if (filterUpdateType != null) {
            webSocketSession.getAttributes().put(FILTER_UPDATE_TYPE, filterUpdateType);
        }

        return webSocketSession
                .send(notificationFlux(webSocketSession).mergeWith(heartbeatFlux(webSocketSession)))
                .and(receive(webSocketSession))
                .doFirst(() -> updateConnectionMetrics(webSocketSession))
                .doFinally(s -> updateDisconnectionMetrics(webSocketSession));
    }

    private void updateConnectionMetrics(WebSocketSession webSocketSession) {
        var userId = webSocketSession.getHandshakeInfo().getHeaders().getFirst(HEADER_USER_ID);
        LOGGER.info("New websocket connection id={} for user={} studyUuid={}, updateType={}", webSocketSession.getId(), userId,
                webSocketSession.getAttributes().get(FILTER_STUDY_UUID), webSocketSession.getAttributes().get(FILTER_UPDATE_TYPE));
        userConnections.compute(userId, (k, v) -> (v == null) ? 1 : v + 1);
        updateConnectionMetricsRegistry();
    }

    private void updateDisconnectionMetrics(WebSocketSession webSocketSession) {
        var userId = webSocketSession.getHandshakeInfo().getHeaders().getFirst(HEADER_USER_ID);
        LOGGER.info("Websocket disconnection id={} for user={}", webSocketSession.getId(), userId);
        userConnections.computeIfPresent(userId, (k, v) -> v > 1 ? v - 1 : null);
        updateConnectionMetricsRegistry();
    }

    private void updateConnectionMetricsRegistry() {
        multiGauge.register(userConnections.entrySet().stream().map(e -> MultiGauge.Row.of(Tags.of(USER_TAG, e.getKey()), e.getValue()))
                .collect(toList()), true);
    }
}
