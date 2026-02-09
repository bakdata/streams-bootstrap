# Deployment to Kubernetes

`streams-bootstrap` provides support for deploying applications to Kubernetes using Helm
charts. The charts cover Kafka Streams, producer, consumer, and producer-consumer applications and offer standardized
solutions for autoscaling, monitoring, and state persistence.

---

## Core capabilities

- **Autoscaling** – KEDA-based horizontal scaling driven by Kafka consumer lag
- **Monitoring** – JMX metrics export with Prometheus integration
- **Persistence** – Persistent volumes for Kafka Streams state stores

---

## Helm charts

A set of Helm charts is shipped, tailored to different application types:

| Chart name             | Purpose                                       | Kubernetes workload types      |
|------------------------|-----------------------------------------------|--------------------------------|
| `streams-app`          | Deploy Kafka Streams applications             | `Deployment`, `StatefulSet`    |
| `producer-app`         | Deploy Kafka Producer applications            | `Deployment`, `Job`, `CronJob` |
| `consumer-app`         | Deploy Kafka Consumer applications            | `Deployment`, `StatefulSet`    |
| `consumerproducer-app` | Deploy batch / consumer–producer applications | `Deployment`, `StatefulSet`    |
| `*-cleanup-job`        | Clean Kafka resources before deployment       | `Job` (Helm hooks)             |

---

## Chart repository and installation

The Helm charts are published as a Helm repository:

```bash
helm repo add streams-bootstrap https://bakdata.github.io/streams-bootstrap/
helm repo update
```

A Streams application can then be installed with:

```bash
helm install my-app bakdata-common/streams-app --values my-values.yaml
```

---

## Deployment patterns

### Streams, consumer and consumer–producer applications

Streams, consumer and consumer–producer applications support both stateless and stateful deployment modes:

- **Deployment**
    - Used for stateless applications or when state is stored externally
    - Enabled when `statefulSet: false` or `persistence.enabled: false`

- **StatefulSet**
    - Used for stateful Kafka Streams applications with local state stores
    - Enabled when `statefulSet: true`
    - Required when `persistence.enabled: true`
    - Each pod receives a dedicated `PersistentVolumeClaim` for RocksDB state

---

### Producer applications

Producer applications support multiple execution modes depending on workload characteristics:

- **Deployment**
  - Used for long-running or continuous producers
  - Enabled when `deployment: true`
  - Supports horizontal scaling via `replicaCount`

- **Job**
  - Used for one-time runs or backfills
  - Default when `deployment: false` and no `schedule` is provided
  - Supports `restartPolicy`, `backoffLimit`, and `ttlSecondsAfterFinished`

- **CronJob**
  - Used for scheduled, periodic execution
  - Enabled when a cron expression is provided via `schedule`
  - Supports `suspend`, `successfulJobsHistoryLimit`, and `failedJobsHistoryLimit`


---

### Cleanup jobs

Cleanup charts are executed as Helm hook Jobs:

- Run as `pre-install` or `pre-upgrade` hooks
- Remove:
    - Kafka topics
    - Consumer groups
    - Schema Registry subjects

This ensures a clean starting point for reprocessing or redeployment scenarios.

---

## Configuration structure
TODO

---

## Environment variable mapping

Helm values are translated into environment variables using a configurable prefix:

```yaml
configurationEnvPrefix: "APP"

commandLine:
  MY_PARAM: "value"
kafka:
  inputTopics: [ "input" ]
  outputTopic: "output"
```

This results in:

- `APP_MY_PARAM=value`
- `APP_INPUT_TOPICS=input`
- `APP_OUTPUT_TOPIC=output`

Kafka client configuration uses the `KAFKA_` prefix:

```yaml
kafka:
  config:
    max.poll.records: 500
```

Becomes:

- `KAFKA_MAX_POLL_RECORDS=500`

---

## Autoscaling

Autoscaling is implemented using Kubernetes Event-Driven Autoscaling (KEDA). When enabled, KEDA monitors Kafka consumer
lag and adjusts the number of replicas accordingly.

Autoscaling is disabled by default.

### Enabling autoscaling

```yaml
autoscaling:
  enabled: true
  lagThreshold: "1000"
  minReplicas: 0
  maxReplicas: 5
```

When enabled, the chart creates a KEDA `ScaledObject` and omits a fixed `replicaCount` from the workload specification.

### Scaling behavior

KEDA computes the desired number of replicas as:

```
desiredReplicas = ceil(totalLag / lagThreshold)
```

subject to `minReplicas` and `maxReplicas`.

### Integration with persistence

When persistence is enabled for Streams applications, autoscaling targets a `StatefulSet`. Each replica receives its own
`PersistentVolumeClaim`.

> **Note:** Scale-down operations remove pods and their PVCs. Backup and recovery strategies should be considered.

---

## Monitoring

Monitoring is based on JMX metrics and Prometheus scraping:

- `jmx.enabled: true` enables Kafka client and Streams metrics
- `prometheus.jmx.enabled: true` adds a Prometheus JMX exporter sidecar
- Metrics are exposed on a dedicated `/metrics` endpoint

Collected metrics include consumer lag, processing rates, and RocksDB statistics.

---

## Persistence

Persistence is configured via the `persistence.*` section (Streams applications only):

```yaml
persistence:
  enabled: true
  size: 1Gi
  storageClassName: standard
```

When enabled together with `statefulSet: true`, each pod receives a dedicated volume for local state storage. This
enables:

- Faster restarts due to warm state
- Improved recovery semantics for stateful topologies

If persistence is disabled, applications behave as stateless deployments and rely on Kafka changelogs for state
reconstruction.
