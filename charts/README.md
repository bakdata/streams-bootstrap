# Kafka Streams Bakdata Helm Repository

Collection of commonly used charts associated with bakdata kafka streaming applications.

## Install

```
helm repo add bakdata-common https://raw.githubusercontent.com/bakdata/common-kafka-streams/<branch_name>/charts/
helm install bakdata-common/<chart_to_install>
```

## Development

You can add new charts in a separate folder or update existing ones. To update the helm repository please run:

```
helm repo index . --merge <your-chart>
```
