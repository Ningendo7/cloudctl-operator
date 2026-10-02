# Security Policy

## Reporting a Vulnerability

If you believe you've found a security vulnerability in cloudctl-operator,
please report it privately rather than opening a public issue.

Use GitHub's [private vulnerability reporting](https://github.com/Ningendo7/cloudctl-operator/security/advisories/new)
feature (**Security** tab → **Report a vulnerability**) on this repository.
This opens a private advisory visible only to you and the maintainer, so
the issue can be discussed and fixed before any public disclosure.

If that feature isn't available on this repository for any reason, open a
regular issue asking for a private contact method instead of including
any vulnerability details in it.

## What to Include

- The affected version/commit.
- Steps to reproduce, or a minimal CR/configuration that triggers it.
- The actual impact you're concerned about (e.g. unauthorized AWS resource
  access, privilege escalation via `sharedWith`, a way to bypass the
  ownership/adoption safety checks) - this project's
  [threat model](docs/threat-model.md) documents what's already considered
  in scope, which may help you describe the gap precisely.

## Supported Versions

This project has not yet made a tagged release. Until it does, only the
latest commit on `main` is supported.

## Response

This is a single-maintainer project. There's no guaranteed response time,
but reports are taken seriously and will be acknowledged as soon as
possible.
