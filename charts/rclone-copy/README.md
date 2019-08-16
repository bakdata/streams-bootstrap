# rclone Helm for Kubernetes

## Usage:
- Create `rclone.conf` (either via `rclone configure` or from `rclone.conf.template`)
- Adapt `values.yaml` to contain your values.
- Install helm with: `helm install . -f values.yaml -n "mySource-mySoruceType-myDestType"` in this directory.
