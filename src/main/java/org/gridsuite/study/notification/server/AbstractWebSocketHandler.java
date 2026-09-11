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
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import static java.util.stream.Collectors.toList;

/**
 * Base class factoring out the common behaviour shared by the notification websocket handlers:
 * connecting the broker flux, converting broker messages to websocket messages, sending heartbeat
 * pings and tracking per-user connection metrics.
 *
 * @author Jon Harper <jon.harper at rte-france.com>
 */
public abstract class AbstractWebSocketHandler implements WebSocketHandler {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final String categoryBrokerInput = getClass().getName() + ".messages.input-broker";
    private final String categoryWsOutput = getClass().getName() + ".messages.output-websocket";

    static final String HEADER_USER_ID = "userId";
    static final String HEADER_TIMESTAMP = "timestamp";
    static final String HEADER_ERROR = "error";

    static final String USERS_METER_NAME = "app.users";
    static final String USER_TAG = "user";

    protected final ObjectMapper jacksonObjectMapper;

    protected final int heartbeatInterval;

    private final Map<String, Integer> userConnections = new ConcurrentHashMap<>();

    private final MultiGauge multiGauge;

    protected Flux<Message<String>> flux;

    protected AbstractWebSocketHandler(ObjectMapper jacksonObjectMapper, MeterRegistry meterRegistry, int heartbeatInterval) {
        this.jacksonObjectMapper = jacksonObjectMapper;
        this.heartbeatInterval = heartbeatInterval;
        this.multiGauge = MultiGauge.builder(USERS_METER_NAME).description("The current number of connections per user").register(meterRegistry);
    }

    /**
     * Publish and connect to the broker flux so that messages are multicast to all connected
     * websockets and discarded when no websockets are connected.
     */
    protected void consumeBrokerFlux(Flux<Message<String>> f) {
        ConnectableFlux<Message<String>> c = f.log(categoryBrokerInput, Level.FINE).publish();
        this.flux = c;
        c.connect();
        // Force connect 1 fake subscriber to consumme messages as they come.
        // Otherwise, reactorcore buffers some messages (not until the connectable flux had
        // at least one subscriber). Is there a better way ?
        c.subscribe();
    }

    /**
     * map from the broker flux to the filtered flux for one websocket client, extracting only relevant fields.
     */
    protected Flux<WebSocketMessage> notificationFlux(WebSocketSession webSocketSession) {
        return flux.filter(message -> filterMessage(webSocketSession, message)).map(m -> {
            try {
                return jacksonObjectMapper.writeValueAsString(Map.of(
                        "payload", m.getPayload(),
                        "headers", toResultHeader(m.getHeaders())));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).log(categoryWsOutput, Level.FINE).map(webSocketSession::textMessage);
    }

    /**
     * Decide whether a broker message should be forwarded to the given websocket session.
     */
    protected abstract boolean filterMessage(WebSocketSession webSocketSession, Message<String> message);

    /**
     * Additional message headers, specific to the concrete handler, that should be passed through
     * to the websocket client on top of the common ones handled by {@link #toResultHeader}.
     */
    protected abstract List<String> getAdditionalPassthroughHeaders();

    private Map<String, Object> toResultHeader(Map<String, Object> messageHeader) {
        var resHeader = new HashMap<String, Object>();
        resHeader.put(HEADER_TIMESTAMP, messageHeader.get(HEADER_TIMESTAMP));

        passHeader(messageHeader, resHeader, HEADER_ERROR);
        passHeader(messageHeader, resHeader, HEADER_USER_ID); // to filter the display of error messages in the front end

        for (String headerName : getAdditionalPassthroughHeaders()) {
            passHeader(messageHeader, resHeader, headerName);
        }

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
    protected Flux<WebSocketMessage> heartbeatFlux(WebSocketSession webSocketSession) {
        return Flux.interval(Duration.ofSeconds(heartbeatInterval)).map(n -> webSocketSession
                .pingMessage(dbf -> dbf.wrap((webSocketSession.getId() + "-" + n).getBytes(java.nio.charset.StandardCharsets.UTF_8))));
    }

    protected synchronized void updateConnectionMetrics(WebSocketSession webSocketSession) {
        var userId = webSocketSession.getHandshakeInfo().getHeaders().getFirst(HEADER_USER_ID);
        logConnection(webSocketSession, userId);
        userConnections.compute(userId, (k, v) -> (v == null) ? 1 : v + 1);
        updateConnectionMetricsRegistry();
    }

    protected abstract void logConnection(WebSocketSession webSocketSession, String userId);

    protected synchronized void updateDisconnectionMetrics(WebSocketSession webSocketSession) {
        var userId = webSocketSession.getHandshakeInfo().getHeaders().getFirst(HEADER_USER_ID);
        logger.info("Websocket disconnection id={} for user={}", webSocketSession.getId(), userId);
        userConnections.computeIfPresent(userId, (k, v) -> v > 1 ? v - 1 : null);
        updateConnectionMetricsRegistry();
    }

    private void updateConnectionMetricsRegistry() {
        multiGauge.register(userConnections.entrySet().stream().map(e -> MultiGauge.Row.of(Tags.of(USER_TAG, e.getKey()), e.getValue()))
                .collect(toList()), true);
    }
}
