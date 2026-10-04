# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/),
and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Two-way sync between two PostgreSQL 17 databases over logical replication: `install`, `init`, `run`, `status`, `watch`, `compare`, `repair`.
- Conflict rules (later `updated_at` wins, delete wins, unique policy `stop` or `merge`).
- Network simulation and a soak kit for long tests.
