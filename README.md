# Study Notification Server

[![Actions Status](https://github.com/gridsuite/study-notification-server/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/gridsuite/study-notification-server/actions)
[![Coverage Status](https://sonarcloud.io/api/project_badges/measure?project=org.gridsuite%3Astudy-notification-server&metric=coverage)](https://sonarcloud.io/component_measures?id=org.gridsuite%3Astudy-notification-server&metric=coverage)
[![MPL-2.0 License](https://img.shields.io/badge/license-MPL_2.0-blue.svg)](https://www.mozilla.org/en-US/MPL/2.0/)

## Description

Study Notification Server is the GridStudy notification service. It consumes study-related update messages (and user quota updates) from RabbitMQ and exposes them to GridStudy clients through two WebSocket endpoints.

## Functional Scope

- Consume study update messages from the `study.update` RabbitMQ destination and broadcast them on the `/notify` WebSocket endpoint.
- Consume user quota update messages from the `quota.update` RabbitMQ destination and broadcast them on the `/quota` WebSocket endpoint.
- Support client-side filtering on `/notify` by `studyUuid` and `updateType`, provided either as query parameters at connection time or dynamically updated afterwards through messages sent by the client on the WebSocket itself.
- Filter `/quota` messages so each client only receives quota updates for its own user (matched against the `userId` header, from the query parameter or the handshake header).
- Forward the update payload and a set of selected message headers needed by the frontend (e.g. `studyUuid`, `updateType`, `node`, `resultUuid`, `computationType`, etc. for `/notify`; `quotaType` for `/quota`).
- Send periodic WebSocket ping frames to keep client connections alive.
- Expose the current number of WebSocket connections per user as a Micrometer metric.

## Technical Stack

- Spring Boot (WebFlux, Actuator, Cloud Stream)
- RabbitMQ via Spring Cloud Stream
- WebSocket
- Micrometer / Prometheus

## Development Scripts

Build Docker image

```shell
mvn install -DskipTests -Dpowsybl.docker.install
```

## WebSocket API

The service exposes two WebSocket endpoints:

```text
/notify
/quota
```

### `/notify`

Broadcasts study update messages. Supports optional filtering by `studyUuid` and `updateType` query parameters:

```text
/notify?studyUuid=<uuid>&updateType=<type>
```

Each outbound text message is a JSON object with the consumed message payload and a filtered header set (only headers present in the original message are included):

```json
{
  "payload": "...",
  "headers": {
    "updateType": "...",
    "studyUuid": "...",
    "substationsIds": "...",
    "node": "...",
    "nodes": "...",
    "rootNetworkUuid": "...",
    "rootNetworksUuids": "...",
    "parentNode": "...",
    "newNode": "...",
    "movedNode": "...",
    "removeChildren": "...",
    "insertMode": "...",
    "referenceNodeUuid": "...",
    "indexation_status": "...",
    "computationType": "...",
    "computationSubtype": "...",
    "resultUuid": "...",
    "exportUuid": "...",
    "exportToGridExplore": "...",
    "fileName": "...",
    "workspaceUuid": "...",
    "panelId": "...",
    "clientId": "..."
  }
}
```

### `/quota`

Broadcasts user quota update messages, filtered to the connected user. 

Each outbound text message is a JSON object with the consumed message payload and the `quotaType` header:

```json
{
  "payload": "...",
  "headers": {
    "quotaType": "..."
  }
}
```
