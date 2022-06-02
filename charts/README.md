# Kafka Streams Bakdata Helm Repository

Collection of commonly used charts associated with bakdata Kafka streaming applications.

## Install

```
helm repo add bakdata-common https://raw.githubusercontent.com/bakdata/streams-bootstrap/<branch_name>/charts/
helm install bakdata-common/<chart_to_install>
```

## Development

You can add new charts in a separate folder or update existing ones. To update the helm repository please run:

```
cd <your-chart-dir>
helm package .
cd ..
helm repo index .
```
