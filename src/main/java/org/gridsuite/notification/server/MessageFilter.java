/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.notification.server;

import java.util.function.Predicate;

import org.springframework.messaging.Message;

import static org.gridsuite.notification.server.NotificationWebSocketHandler.*;

/**
 * @author Slimane Amar <slimane.amar at rte-france.com>
 */
final class MessageFilter implements Predicate<Message<String>> {

    private final String userId;
    private final String filterStudyUuid;
    private final String filterUpdateType;

    public MessageFilter(String userId, String filterStudyUuid, String filterUpdateType) {
        this.userId = userId;
        this.filterStudyUuid = filterStudyUuid;
        this.filterUpdateType = filterUpdateType;
    }

    @Override
    public boolean test(Message<String> message) {
        var ok = true;
        if (userId != null) {
            var headerIsPublicStudy = message.getHeaders().get(HEADER_IS_PUBLIC_STUDY, Boolean.class);
            ok &= headerIsPublicStudy == null
                    || headerIsPublicStudy
                    || userId.equals(message.getHeaders().get(HEADER_USER_ID));
        }

        if (filterStudyUuid != null) {
            ok &= filterStudyUuid.equals(message.getHeaders().get(HEADER_STUDY_UUID));
        }

        if (filterUpdateType != null) {
            ok &= filterUpdateType.equals(message.getHeaders().get(HEADER_UPDATE_TYPE));
        }

        return ok;
    }
}

