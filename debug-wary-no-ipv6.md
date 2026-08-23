# Debug Session: Wary IPv6 Startup Failure

Status: [OPEN]
Session: wary-no-ipv6

## Symptom
Wary fails to start and reports no IPv6 address.

## Hypotheses
1. Wary startup arguments or configuration do not enable IPv6.
2. The operating system or network stack has no usable IPv6 address.
3. The service binds only to an IPv4 address or IPv4-only socket.
4. The `.env` IPv6 setting is empty or malformed.
5. The startup script fails during IPv6 initialization.

## Evidence
Pending runtime reproduction and logs.

## Changes
No business logic changed. Instrumentation has not yet been added.
