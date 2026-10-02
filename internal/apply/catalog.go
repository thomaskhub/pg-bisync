package apply

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// catalog caches what the applier needs to know about the local tables.
type catalog struct {
	types   map[string]map[string]string // table → column → SQL type (format_type)
	pk      map[string]string            // table → primary key column
	fks     map[string]foreignKey        // constraint name → definition (single-column FKs)
	uniques map[string]uniqueKey         // constraint name → definition (non-PK unique constraints)
}

type foreignKey struct {
	Name, Table, Column, RefTable, RefColumn string
	Deferrable                               bool
}

type uniqueKey struct {
	Table   string
	Columns []string
}

func loadCatalog(ctx context.Context, conn *pgx.Conn, schema string) (*catalog, error) {
	c := &catalog{types: map[string]map[string]string{}, pk: map[string]string{}, fks: map[string]foreignKey{}, uniques: map[string]uniqueKey{}}
	rows, err := conn.Query(ctx, `
		SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped`, schema)
	if err != nil {
		return nil, fmt.Errorf("load columns: %w", err)
	}
	var tbl, col, typ string
	_, err = pgx.ForEachRow(rows, []any{&tbl, &col, &typ}, func() error {
		if c.types[tbl] == nil {
			c.types[tbl] = map[string]string{}
		}
		c.types[tbl][col] = typ
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load columns: %w", err)
	}
	rows, err = conn.Query(ctx, `
		SELECT k.conname, k.contype::text, k.condeferrable, c.relname, coalesce(r.relname, ''),
		       array(SELECT a.attname FROM unnest(k.conkey) WITH ORDINALITY u(n, o)
		             JOIN pg_attribute a ON a.attrelid = k.conrelid AND a.attnum = u.n ORDER BY u.o),
		       array(SELECT a.attname FROM unnest(coalesce(k.confkey, '{}')) WITH ORDINALITY u(n, o)
		             JOIN pg_attribute a ON a.attrelid = k.confrelid AND a.attnum = u.n ORDER BY u.o)
		FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_class r ON r.oid = k.confrelid
		WHERE n.nspname = $1 AND k.contype IN ('p', 'f', 'u')`, schema)
	if err != nil {
		return nil, fmt.Errorf("load constraints: %w", err)
	}
	var name, kind, ref string
	var deferrable bool
	var cols, refCols []string
	_, err = pgx.ForEachRow(rows, []any{&name, &kind, &deferrable, &tbl, &ref, &cols, &refCols}, func() error {
		switch {
		case kind == "p" && len(cols) == 1:
			c.pk[tbl] = cols[0]
		case kind == "f" && len(cols) == 1 && len(refCols) == 1:
			c.fks[name] = foreignKey{Name: name, Table: tbl, Column: cols[0], RefTable: ref, RefColumn: refCols[0], Deferrable: deferrable}
		case kind == "u":
			c.uniques[name] = uniqueKey{Table: tbl, Columns: append([]string(nil), cols...)}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load constraints: %w", err)
	}
	return c, nil
}
