# Security policy

tinyTiles processes externally supplied PBF, MBTiles, artifact and HTTP tile
data. Please report suspected vulnerabilities privately through the GitHub
repository's security-advisory mechanism once the repository is published.
Until then, do not include exploit details in public issues.

Security-sensitive areas include:

- artifact completion/checksum/index validation;
- path handling for persistent outputs and native cache directories;
- resource limits during import, PBF generation and HTTP synchronization;
- browser CORS and revisioned-cache behaviour;
- malformed, oversized or checksum-mismatched tile payloads.

The reference demo server deliberately has no authentication, authorization or
rate limiting. It is for local demos and integration tests, not direct public
deployment.
