/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.study.notification.server.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.ReactiveAuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.method.configuration.EnableReactiveMethodSecurity;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.SecurityWebFiltersOrder;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.authentication.AuthenticationWebFilter;
import org.springframework.security.web.server.authentication.ServerAuthenticationConverter;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;

import static org.gridsuite.study.notification.server.AbstractWebSocketHandler.HEADER_USER_ID;

/**
 * @author Radouane KHOUADRI {@literal <redouane.khouadri_externe at rte-france.com>}
 */
@Configuration
@EnableWebFluxSecurity
@EnableReactiveMethodSecurity(proxyTargetClass = true) // proxyTargetClass=true is required: WebSocketConfiguration injects the concrete handler classes
public class SecurityConfig {

    public static final String HEADER_ROLES = "roles";

    @Bean
    SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return http
                .addFilterAt(headerAuthenticationFilter(), SecurityWebFiltersOrder.AUTHENTICATION)
                .authorizeExchange(exchanges -> exchanges.anyExchange().permitAll())
                .build();
    }

    private AuthenticationWebFilter headerAuthenticationFilter() {
        ServerAuthenticationConverter converter = exchange -> {
            String userId = exchange.getRequest().getHeaders().getFirst(HEADER_USER_ID);
            if (userId == null || userId.isEmpty()) {
                return Mono.empty();
            }
            Authentication authentication =
                    new UsernamePasswordAuthenticationToken(userId, null, extractRoles(exchange.getRequest().getHeaders()));
            return Mono.just(authentication);
        };

        ReactiveAuthenticationManager authenticationManager = Mono::just;

        AuthenticationWebFilter filter = new AuthenticationWebFilter(authenticationManager);
        filter.setServerAuthenticationConverter(converter);
        return filter;
    }

    private static List<SimpleGrantedAuthority> extractRoles(HttpHeaders headers) {
        String rawRoles = headers.getFirst(HEADER_ROLES);
        if (rawRoles == null) {
            return List.of();
        }
        return Arrays.stream(rawRoles.split("\\|"))
                .map(String::trim)
                .filter(role -> !role.isEmpty())
                .map(SimpleGrantedAuthority::new)
                .toList();
    }
}
