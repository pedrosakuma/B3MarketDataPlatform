# b3-marketdata

Helm chart for the `b3-marketdata` UMDF consumer + WebSocket fan-out service
(Layer-1 component chart, per the [deploy topology RFC](https://github.com/pedrosakuma/B3Deploy/issues/1)).

`b3deploy` deploys this as `chart@version` + env-specific values; it is not
meant to be a general-purpose UMDF chart — the network topology assumptions
(unicast UDP from a `matching` workload, WS consumers such as `trading-host`)
are specific to the b3-sim reference deployment.

## Install

```console
helm install marketdata oci://ghcr.io/pedrosakuma/charts/b3-marketdata --version <chart-version>
```

## Key values

| Key | Description | Default |
| --- | --- | --- |
| `image.repository` | Container image | `ghcr.io/pedrosakuma/b3-marketdata` |
| `image.tag` | Image tag override | `.Chart.AppVersion` |
| `image.digest` | Pin by digest (`sha256:...`), takes precedence over `tag` | `""` |
| `wsPort` | WebSocket/health port | `8080` |
| `udpPorts.*` | UMDF unicast UDP ingest ports | `30084/30085/30184/31084` |
| `resources` | Pod resource requests/limits | `cpu: 75m`, `memory: 256Mi/512Mi` |
| `colocation.enabled` | Pod affinity to co-locate with `matching` | `false` |
| `networkPolicy.enabled` | Emit the marketdata-ingress NetworkPolicy | `true` |
| `transportConfig` | Raw override for `transport.json`; leave empty to use the built-in EQT layout wired to `udpPorts` | `""` |

See [`values.yaml`](./values.yaml) for the full surface.

## Topology notes

- **No LoadBalancer in the UDP path.** Azure VNets have no IP multicast, so
  matching sends unicast UDP straight to the pod IP via a headless Service.
- **`publishNotReadyAddresses: true` is load-bearing.** matching resolves the
  Service DNS name at startup and aborts if it misses, so the A record must
  exist before marketdata reports Ready.
- **NetworkPolicy** admits only `matching` (the 4 UDP ports) and
  `trading-host` (`:8080` WS). Adjust `networkPolicy.matchingPodSelector` /
  `tradingHostPodSelector` if those workloads carry different labels in your
  cluster.

## Versioning contract

Chart `version` is SemVer and released in lockstep with the `b3-marketdata`
image; `appVersion` tracks the last image version validated against this
chart. CI (`.github/workflows/helm-chart.yml`) only packages and pushes the
chart when `Chart.yaml`'s `version` changes on `main`.
