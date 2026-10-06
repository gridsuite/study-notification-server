/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.study.notification.server.authorization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

import static org.gridsuite.study.notification.server.config.SecurityConfig.HEADER_USER_ID;

/**
 * @author Radouane KHOUADRI {@literal <redouane.khouadri_externe at rte-france.com>}
 */
@Service("authorizationService")
public class AuthorizationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationService.class);

    private static final String PARAM_IDS = "ids";
    private static final String PARAM_ACCESS_TYPE = "accessType";
    private static final String QUERY_STUDY_UUID = "studyUuid";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final WebClient webClient;
    private final String elementsAuthorizedPath;

    public AuthorizationService(
            WebClient.Builder webClientBuilder,
            @Value("${gridsuite.services.directory-server.base-uri:http://directory-server/}") String directoryServerBaseUri,
            @Value("${gridsuite.services.directory-server.authorized-elements-path:/v1/elements/authorized}")
            String elementsAuthorizedPath) {
        this.webClient = webClientBuilder.baseUrl(directoryServerBaseUri).build();
        this.elementsAuthorizedPath = elementsAuthorizedPath;
    }

    public Mono<Boolean> canReadStudy(Authentication authentication, WebSocketSession session) {
        if (authentication == null || !authentication.isAuthenticated()) {
            return Mono.just(false);
        }

        UUID studyUuid = extractStudyUuid(session);
        if (studyUuid == null) {
            return Mono.just(false);
        }

        String userId = authentication.getName();

        String path = UriComponentsBuilder.fromPath(elementsAuthorizedPath)
                .queryParam(PARAM_ACCESS_TYPE, "READ")
                .queryParam(PARAM_IDS, studyUuid)
                .toUriString();

        return webClient.get()
                .uri(path)
                .header(HEADER_USER_ID, userId)
                .retrieve()
                .toBodilessEntity()
                .map(response -> response.getStatusCode().is2xxSuccessful())
                .timeout(TIMEOUT)
                .doOnNext(granted -> LOGGER.debug("Read access for user {} on study {}: {}", userId, studyUuid, granted))
                .onErrorResume(e -> {
                    LOGGER.warn("Authorization check failed for user {} on study {}. Access denied.", userId, studyUuid, e);
                    return Mono.just(false);
                })
                .defaultIfEmpty(false);
    }

    private static UUID extractStudyUuid(WebSocketSession session) {
        String raw = UriComponentsBuilder.fromUri(session.getHandshakeInfo().getUri())
                .build()
                .getQueryParams()
                .getFirst(QUERY_STUDY_UUID);
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw.trim());
        } catch (IllegalArgumentException _) {
            return null;
        }
    }
}
