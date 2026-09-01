/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.study.notification.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
public class QuotaWebSocketHandler extends AbstractWebSocketHandler {

    public QuotaWebSocketHandler(ObjectMapper jacksonObjectMapper, MeterRegistry meterRegistry,
                                 @Value("${notification.websocket.heartbeat.interval:30}") int heartbeatInterval) {
        super(jacksonObjectMapper, meterRegistry, heartbeatInterval);
    }

    @Bean
    public Consumer<Flux<Message<String>>> consumeNotificationQuota() {
        return this::consumeBrokerFlux;
    }

    @Override
    protected boolean filterMessage(WebSocketSession webSocketSession, Message<String> message) {
        String userId = (String) webSocketSession.getAttributes().get(HEADER_USER_ID);
        return userId == null || userId.equals(message.getHeaders().get(HEADER_USER_ID));
    }

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        var uri = webSocketSession.getHandshakeInfo().getUri();
        MultiValueMap<String, String> parameters = UriComponentsBuilder.fromUri(uri).build(true).getQueryParams();
        String filterUserId = parameters.getFirst(QUERY_USER_ID);
        if (filterUserId != null) {
            webSocketSession.getAttributes().put(HEADER_USER_ID, filterUserId);
        }

        return webSocketSession
                .send(notificationFlux(webSocketSession).mergeWith(heartbeatFlux(webSocketSession)))
                .doFirst(() -> updateConnectionMetrics(webSocketSession))
                .doFinally(s -> updateDisconnectionMetrics(webSocketSession));
    }
}
