# Security policy

TaskForge is an educational, production-like portfolio project and is not yet
production ready.

v0.1 has no authentication, authorization, TLS termination, tenant isolation,
or rate limiting. Run it only on loopback or a trusted development network; do
not expose it directly to the public internet.

Do not report suspected vulnerabilities in public issues. Contact the repository
owner privately through the contact method listed on the owner's GitHub profile.

Never commit database credentials, private keys, job payloads, access tokens, or
production logs. The values in `.env.example` and `compose.yaml` are local
development defaults only.
