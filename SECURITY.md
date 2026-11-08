# Security policy

TaskForge is an educational, production-like portfolio project and is not yet
production ready.

v0.7 has no authentication, authorization, TLS termination, tenant isolation,
or rate limiting. The Compose NATS server has no authentication either; events
carry no payloads or keys, but job metadata is still visible to anyone who can
reach NATS. Dead-letter redrive and schedule management are likewise unauthenticated. Run it only on loopback or a trusted development network; do
not expose it directly to the public internet.

Telemetry endpoints (metrics listeners, Prometheus, Grafana, Jaeger) are
unauthenticated and bound to loopback in Compose. Spans and metrics carry IDs,
kinds, queues, and error codes, never payloads, results, or keys. A client
`traceparent` header is never trusted as a parent span; it is recorded as a
link, so clients cannot force sampling or join other traces.

Do not report suspected vulnerabilities in public issues. Contact the repository
owner privately through the contact method listed on the owner's GitHub profile.

Never commit database credentials, private keys, job payloads, access tokens, or
production logs. The values in `.env.example` and `compose.yaml` are local
development defaults only.
