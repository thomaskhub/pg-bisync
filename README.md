# pg-bisync

Two-way (active-active) sync between two PostgreSQL databases on **plain, unpatched Postgres**:
row triggers write every change to an outbox table, a small worker copies the changes to the other
node, conflicts are resolved by fixed rules. A single small Go binary.

Status: under construction (not ready for production).

## Rules

- Last change wins per row (`updatedAt`); identical timestamps → a configured node wins.
- Deletes win over updates (tombstones).
- At-least-once delivery with idempotent apply; late-committing transactions are not lost.
- Foreign keys and cascades stay active when changes are applied; rows that cannot be applied are recorded and alerted.

## Requirements

- PostgreSQL ≥ 17 on both nodes, every synced table with a single-column primary key and an `updatedAt` column.

## License

MIT
