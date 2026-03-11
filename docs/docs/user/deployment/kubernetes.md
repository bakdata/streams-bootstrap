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
| `*-cleanup-job`        | Clean Kafka resources before deployment       | `Job`                          |

---

## Chart repository and installation

The Helm charts are published as a Helm repository:

```bash
helm repo add streams-bootstrap https://bakdata.github.io/streams-bootstrap/
helm repo update
```

A Streams application can then be installed with:

```bash
helm install my-app streams-bootstrap/streams-app --values my-values.yaml
```

---

## Deployment patterns

### Streams, consumer and consumer–producer applications

Streams, consumer and consumer–producer applications support both stateless and stateful deployment modes:

- **Deployment**
    - Used for stateless applications or when state is small and can be restored easily
    - Enabled when `statefulSet: false`

- **StatefulSet**
    - Used for stateful Kafka Streams applications with local state stores
    - Enabled when `statefulSet: true`
    - Required when `persistence.enabled: true`
    - If persistence is enabled each pod receives a dedicated `PersistentVolumeClaim` for RocksDB state

---

### Producer applications

Producer applications support multiple execution modes depending on workload characteristics:

- **Deployment**
    - Used for continuous producers
    - Enabled when `deployment: true`
    - Supports horizontal scaling via `replicaCount`

- **Job**
    - Used for one-time runs or backfills
    - Default when `deployment: false` and no `schedule` is provided
    - Supports `restartPolicy`, `backoffLimit`, and `ttlSecondsAfterFinished`

- **CronJob**
    - Used for scheduled, periodic execution
    - Enabled when a cron expression is provided via `schedule`
    - Supports everything a job supports and `suspend`, `successfulJobsHistoryLimit`, and `failedJobsHistoryLimit`

---

## Configuration structure

TODO

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

For details on the scaling behavior, please refer to
the [official KEDA documentation for the Kafka scaler](https://keda.sh/docs/scalers/apache-kafka/).

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

Persistence is configured via the `persistence.*` section (for Streams, Consumer and Consumer-Producer applications):

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
