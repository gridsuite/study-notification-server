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
import org.gridsuite.study.notification.server.dto.Filters;
import org.gridsuite.study.notification.server.dto.FiltersToAdd;
import org.gridsuite.study.notification.server.dto.FiltersToRemove;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

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
public class NotificationWebSocketHandler extends AbstractWebSocketHandler {

    public NotificationWebSocketHandler(ObjectMapper jacksonObjectMapper, MeterRegistry meterRegistry, @Value("${notification.websocket.heartbeat.interval:30}") int heartbeatInterval) {
        super(jacksonObjectMapper, meterRegistry, heartbeatInterval);
    }

    @Bean
    public Consumer<Flux<Message<String>>> consumeNotification() {
        return this::consumeBrokerFlux;
    }

    @Override
    protected boolean filterMessage(WebSocketSession webSocketSession, Message<String> message) {
        String filterStudyUuid = (String) webSocketSession.getAttributes().get(FILTER_STUDY_UUID);
        if (filterStudyUuid != null && !filterStudyUuid.equals(message.getHeaders().get(HEADER_STUDY_UUID))) {
            return false;
        }
        String filterUpdateType = (String) webSocketSession.getAttributes().get(FILTER_UPDATE_TYPE);
        return filterUpdateType == null || filterUpdateType.equals(message.getHeaders().get(HEADER_UPDATE_TYPE));
    }

    public Flux<WebSocketMessage> receive(WebSocketSession webSocketSession) {
        return webSocketSession.receive()
                .doOnNext(webSocketMessage -> {
                    try {
                        //if it's not the heartbeat
                        if (webSocketMessage.getType().equals(WebSocketMessage.Type.TEXT)) {
                            String wsPayload = webSocketMessage.getPayloadAsText();
                            logger.debug("Message received : {} by session {}", wsPayload, webSocketSession.getId());
                            Filters receivedFilters = jacksonObjectMapper.readValue(webSocketMessage.getPayloadAsText(), Filters.class);
                            handleReceivedFilters(webSocketSession, receivedFilters);
                        }
                    } catch (JsonProcessingException e) {
                        logger.error(e.toString(), e);
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
}
