# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Conduit, please report it responsibly:

1. **Do NOT** open a public GitHub issue
2. Email security concerns to the maintainers
3. Include as much detail as possible: steps to reproduce, impact, affected versions

We will acknowledge receipt within 48 hours and provide a timeline for a fix.

## Security Design

Conduit follows a defense-in-depth approach:

- **Parameterized queries only** — Zero string interpolation in SQL
- **Read-only by default** — Write operations require explicit opt-in
- **DSN sanitization** — Passwords are never logged or displayed
- **PII detection** — Automatic masking of sensitive columns
- **Filter expression parser** — Rejects SQL injection patterns
- **RBAC** — Role-based access control with per-table permissions
- **Audit logging** — Every query is logged with user, timestamp, and duration

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| Latest  | Yes                |
| < 1.0   | Best effort        |
