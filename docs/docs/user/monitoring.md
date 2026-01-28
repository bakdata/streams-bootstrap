# Monitoring

The framework provides features for monitoring your applications.

- **JMX Metrics Export**: Applications built with `streams-bootstrap` can expose JMX (Java Management Extensions)
  metrics, which provide insights into the performance and health of the Java application and the Kafka clients.
- **Prometheus Integration**: The Helm charts are configured to work with Prometheus, a popular open-source monitoring
  and alerting toolkit. This allows you to scrape the JMX metrics and visualize them in dashboards (e.g., using
  Grafana).

## Monitoring and Observability

The Helm charts provide integrated monitoring and observability for Kafka applications using a combination of
JMX, Prometheus, Kubernetes probes, and Services. Monitoring can be tailored from lightweight setups for development
to full production stacks with Prometheus Operator.

### Monitoring Mechanisms

| Mechanism               | Use Case                            | Key Values               |
|-------------------------|-------------------------------------|--------------------------|
| JMX remote access       | Direct debugging and inspection     | `jmx.enabled`            |
| Prometheus JMX exporter | Production metrics collection       | `prometheus.jmx.enabled` |
| Liveness probes         | Container health checks             | `livenessProbe`          |
| Readiness probes        | Traffic readiness / rollout control | `readinessProbe`         |

### JMX Configuration

JMX (Java Management Extensions) provides direct access to application metrics and management operations, typically
used for development and debugging.

Enable JMX in `values.yaml`:

```yaml
jmx:
  enabled: true
  port: 5555
  host: localhost
```

Parameters:

- `jmx.enabled`: Enable JMX port for remote access (default: `false`).
- `jmx.port`: JMX port number (default: `5555`).
- `jmx.host`: Host binding for the RMI server (default: `localhost`).

When enabled, the chart configures the JVM with flags similar to:

```text
-Dcom.sun.management.jmxremote
-Dcom.sun.management.jmxremote.port=5555
-Dcom.sun.management.jmxremote.local.only=false
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.ssl=false
-Djava.rmi.server.hostname=localhost
```

Accessing JMX metrics from a local client:

```bash
kubectl port-forward <pod-name> 5555:5555
jconsole localhost:5555
```

### Prometheus JMX Exporter

For production monitoring, the Prometheus JMX Exporter runs as a sidecar container that scrapes JMX metrics from the
application and exposes them in Prometheus format.

Enable the exporter in `values.yaml`:

```yaml
prometheus:
  jmx:
    enabled: true
    image: bitnami/jmx-exporter
    imageTag: 1.1.0
    imagePullPolicy: Always
    port: 5556
    metricRules:
      - pattern: ".*"
    resources:
      requests:
        cpu: 10m
        memory: 100Mi
      limits:
        cpu: 100m
        memory: 100Mi
```

Key parameters:

- `prometheus.jmx.enabled`: Deploy JMX exporter sidecar (default: `false`).
- `prometheus.jmx.image`: Container image for the exporter (default: `bitnami/jmx-exporter`).
- `prometheus.jmx.imageTag`: Exporter image tag (default: `1.1.0`).
- `prometheus.jmx.port`: HTTP port for metrics endpoint (default: `5556`).
- `prometheus.jmx.metricRules`: JMX metric selection and mapping rules.
- `prometheus.jmx.resources`: Resource requests/limits for the exporter container.

#### Metric Rules

The `metricRules` section configures which JMX beans are exposed and how they are mapped to Prometheus metrics. The
default configuration uses `pattern: ".*"` to export all metrics, but production setups should restrict this to
relevant Kafka Streams/producer/consumer metrics.

Example rule set:

```yaml
prometheus:
  jmx:
    metricRules:
      - pattern: "kafka.streams<type=(.+), client-id=(.+), (.+)=(.+)><>(.+):"
        name: kafka_streams_$1_$5
        labels:
          client_id: "$2"
          $3: "$4"
      - pattern: "kafka.producer<type=(.+), client-id=(.+)><>(.+):"
        name: kafka_producer_$1_$3
        labels:
          client_id: "$2"
```

A ConfigMap containing the JMX exporter configuration is generated automatically by the chart and mounted into the
sidecar container.

### Prometheus Integration

#### Pod annotations

For Prometheus instances that use pod annotations for discovery:

```yaml
podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/path: "/metrics"
  prometheus.io/port: "5556"
```

This enables scraping the JMX exporter endpoint exposed on `prometheus.jmx.port`.

### PodMonitor

For more advanced Prometheus Operator setups, a `PodMonitor` custom resource can be deployed.

The `streams-bootstrap` repository provides a reference `PodMonitor`
configuration: [monitoring/pod_monitor.yaml](https://github.com/bakdata/streams-bootstrap/blob/1ff01c2f/monitoring/pod_monitor.yaml)

### Health Checks

Kubernetes uses liveness and readiness probes to determine when a pod is healthy and when it is ready to receive
traffic.

**Liveness probes** restart containers that become unhealthy:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
```

**Readiness probes** gate traffic until the application is ready:

```yaml
readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 5
  timeoutSeconds: 3
  successThreshold: 1
  failureThreshold: 3
```

All standard Kubernetes probe types are supported (`httpGet`, `tcpSocket`, `exec`, and `grpc` on recent Kubernetes
versions). Probes are configured under the corresponding `livenessProbe` and `readinessProbe` sections in values.

### Service and Port Configuration

Ports and Services control how HTTP APIs and metrics endpoints are exposed:

```yaml
service:
  enabled: true
  type: ClusterIP

ports:
  - containerPort: 8080
    name: http
    protocol: TCP
    servicePort: 80
  - containerPort: 5556
    name: metrics
    protocol: TCP
    servicePort: 5556
```

Port mapping reference:

- `jmx.port` → JMX remote port (default `5555`).
- `prometheus.jmx.port` → JMX exporter metrics port (default `5556`).
- Additional entries in `ports[]` → application-specific ports (e.g. HTTP APIs, custom metrics endpoints).

### Monitoring Configuration Examples

**Full monitoring stack** (JMX exporter, probes, Service, annotations):

```yaml
prometheus:
  jmx:
    enabled: true
    port: 5556
    metricRules:
      - pattern: "kafka.streams<type=stream-metrics, client-id=(.+)><>(.+):"
        name: kafka_streams_$2
        labels:
          client_id: "$1"

readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10

livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 60
  periodSeconds: 30

service:
  enabled: true
  type: ClusterIP

ports:
  - containerPort: 8080
    name: http
    servicePort: 80
  - containerPort: 5556
    name: metrics
    servicePort: 5556

podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/path: "/metrics"
  prometheus.io/port: "5556"
```

**Development/debug configuration with JMX only**:

```yaml
jmx:
  enabled: true
  port: 5555
  host: localhost

prometheus:
  jmx:
    enabled: false

livenessProbe:
  tcpSocket:
    port: 5555
  initialDelaySeconds: 30
  periodSeconds: 30
```

**Minimal production configuration** with annotations and resource limits:

```yaml
prometheus:
  jmx:
    enabled: true
    resources:
      requests:
        cpu: 10m
        memory: 100Mi
      limits:
        cpu: 100m
        memory: 100Mi

podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "5556"
```

### Application-Specific Considerations

Kafka Streams applications expose metrics under several JMX domains, including `kafka.streams`, `kafka.producer`, and
`kafka.consumer`. Commonly monitored metrics include:

- `kafka.streams.state`: Overall application state (running, rebalancing, error).
- `kafka.streams.commit-latency-avg`: Average commit latency.
- `kafka.consumer.records-lag-max`: Maximum records lag per partition.
- `kafka.producer.record-send-rate`: Producer throughput.

Producer and consumer applications (via `producer-app` and `consumer-app` charts) use the same `prometheus.jmx`
structure
but may differ in availability patterns (for example, Jobs vs Deployments).

### Troubleshooting

**Common issues**:

- No metrics endpoint:
    - Ensure `prometheus.jmx.enabled: true`.
- Connection refused on JMX port:
    - Ensure `jmx.enabled: true` and the port is exposed.
- Empty metrics response:
    - Review `metricRules` patterns; overly restrictive rules may filter out all metrics.
- High exporter CPU usage:
    - Avoid `pattern: ".*"` in production; use targeted patterns instead.
- Pod not ready:
    - Validate liveness/readiness probe configuration and the corresponding application endpoints.

**Verifying metrics export**:

```bash
kubectl port-forward <pod-name> 5556:5556
curl http://localhost:5556/metrics
```

**Debugging JMX connection**:

```bash
kubectl port-forward <pod-name> 5555:5555
jconsole localhost:5555
```

If connection fails, verify that JMX is enabled, the port is mapped in `ports`, and the JVM has been started with the
correct JMX system properties.
