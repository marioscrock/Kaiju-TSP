# Kaiju-TSP

A one-component TSP (Traces Stream Processor) for [Jaeger](https://github.com/jaegertracing/jaeger) end-to-end tracing system (`v1.5`). The detailed description can be found in the thesis elaborate [Towards observability with (RDF) trace stream processing](https://www.politesi.polimi.it/handle/10589/144741).

This repository contains the Kaiju source code together with a `docker-compose` file to launch a coupled Jaeger-Kaiju deployment.

Kaiju-TSP communications:
- port `2042`accepts incoming spans via TChannel from customized `jaeger-agent` (source code in folder `jaeger`)
- port `9876`accepts incoming metrics or logs via socket (JSON) (format defined as POJO in `eventsocket.Metric.java`and `eventsocket.Event.java`)
- port `9278` exposes the API defined 
- on `<ip>:4567/streams/jsonTraces` exposes the RDF stream through web socket

#### Kaiju-TSP API

API exposed on port `9278`.
- `POST /api/query?query=<query>` Execute the fire and forget query sent.
- `POST /api/statement?statement=<statement>&msg=<msg>` Install the given statement with a `eps.listener.CEPListener` registered with the given message. Returns the statement code.
- `POST /api/remove?statement=<stmt_code>` Remove the statement with the given code.
- `GET /api/traces/all` Return all `traceId`s related to spans currently retained by the Esper Engine.
- `GET /api/traces?service=<service>` Return all spans for a given `serviceName` currently retained by the Esper Engine.
- `GET /api/traces/:id` Return all spans for a given `traceId` currently retained by the Esper Engine.
- `GET /api/logs/:key` Return a tuple `(spanId, operationName, logs)` for all logs with a field with the given `key` currently retained by the Esper Engine.
- `GET /api/dependencies/:traceId` Return a set of tuples `(serviceFrom, serviceTo, numInteractions)` representing interactions between services in a given trace.

All requests return {@code status 400} and an error message if Kaiju reports an error while managing the request.
