/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.study.notification.server;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.util.ClassUtils;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Radouane KHOUADRI {@literal <redouane.khouadri_externe at rte-france.com>}
 */
class WebSocketHandlerSecurityTest {
    private static final String BASE_PACKAGE = "org.gridsuite.study.notification.server";

    static Stream<Class<?>> webSocketHandlers() {
        var scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AssignableTypeFilter(WebSocketHandler.class));

        return scanner.findCandidateComponents(BASE_PACKAGE).stream()
                .map(beanDefinition -> ClassUtils.resolveClassName(
                        Objects.requireNonNull(beanDefinition.getBeanClassName()),
                        WebSocketHandlerSecurityTest.class.getClassLoader()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("webSocketHandlers")
    void handleMethodMustBeAnnotatedWithPreAuthorize(Class<?> handlerClass) throws NoSuchMethodException {
        Method handle = handlerClass.getMethod("handle", WebSocketSession.class);

        boolean secured = AnnotatedElementUtils.hasAnnotation(handle, PreAuthorize.class)
                || AnnotatedElementUtils.hasAnnotation(handle.getDeclaringClass(), PreAuthorize.class);

        assertTrue(secured, () -> "Missing @PreAuthorize on "
                + handle.getDeclaringClass().getName() + "#handle(WebSocketSession)");
    }
}
