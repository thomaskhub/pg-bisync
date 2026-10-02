// Package compare finds rows that differ between the two nodes (per table, localised by hash
// buckets of the primary key) and repairs a row by re-saving the winning version.
package compare

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/thomkin/pg-bisync/internal/config"
	"github.com/thomkin/pg-bisync/internal/schema"
)

const (
	buckets     = 64
	maxListed   = 100
	bucketQuery = `SELECT abs(hashtext(%[2]s::text)) %% %[3]d, count(*), md5(string_agg(x::text, '|' ORDER BY %[2]s::text))
		FROM %[1]s x GROUP BY 1`
	rowQuery = `SELECT %[2]s::text, md5(x::text) FROM %[1]s x WHERE abs(hashtext(%[2]s::text)) %% %[3]d = ANY($1)`
)

// TableDiff lists the differences of one table.
type TableDiff struct {
	LocalRows int64    `json:"local_rows"`
	PeerRows  int64    `json:"peer_rows"`
	Differing []string `json:"differing_pks,omitempty"` // at most 100
	More      bool     `json:"more,omitempty"`
}

// Result maps table → differences (only tables that differ).
type Result map[string]TableDiff

// Run compares every synced table of both nodes.
func Run(ctx context.Context, cfg config.Config) (Result, error) {
	local, peer, err := connectBoth(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer local.Close(context.Background())
	defer peer.Close(context.Background())
	tables, err := schema.Tables(ctx, local, cfg)
	if err != nil {
		return nil, err
	}
	res := Result{}
	for _, t := range tables {
		d, differ, err := table(ctx, local, peer, cfg.Schema, t)
		if err != nil {
			return nil, err
		}
		if differ {
			res[t.Name] = d
		}
	}
	return res, nil
}

func table(ctx context.Context, local, peer *pgx.Conn, schemaName string, t schema.Table) (TableDiff, bool, error) {
	qt := pgx.Identifier{schemaName, t.Name}.Sanitize()
	pk := pgx.Identifier{t.PK}.Sanitize()
	type bucket struct {
		n   int64
		sum string
	}
	read := func(c *pgx.Conn) (map[int]bucket, int64, error) {
		rows, err := c.Query(ctx, fmt.Sprintf(bucketQuery, qt, pk, buckets))
		if err != nil {
			return nil, 0, fmt.Errorf("checksum %s: %w", t.Name, err)
		}
		out := map[int]bucket{}
		var total int64
		var b int
		var bk bucket
		_, err = pgx.ForEachRow(rows, []any{&b, &bk.n, &bk.sum}, func() error {
			out[b] = bk
			total += bk.n
			return nil
		})
		return out, total, err
	}
	lb, ln, err := read(local)
	if err != nil {
		return TableDiff{}, false, err
	}
	pb, pn, err := read(peer)
	if err != nil {
		return TableDiff{}, false, err
	}
	var bad []int32
	for b := 0; b < buckets; b++ {
		if lb[b] != pb[b] {
			bad = append(bad, int32(b))
		}
	}
	d := TableDiff{LocalRows: ln, PeerRows: pn}
	if len(bad) == 0 {
		return d, false, nil
	}
	rowSums := func(c *pgx.Conn) (map[string]string, error) {
		rows, err := c.Query(ctx, fmt.Sprintf(rowQuery, qt, pk, buckets), bad)
		if err != nil {
			return nil, fmt.Errorf("rows of %s: %w", t.Name, err)
		}
		out := map[string]string{}
		var id, sum string
		_, err = pgx.ForEachRow(rows, []any{&id, &sum}, func() error { out[id] = sum; return nil })
		return out, err
	}
	lr, err := rowSums(local)
	if err != nil {
		return d, true, err
	}
	pr, err := rowSums(peer)
	if err != nil {
		return d, true, err
	}
	for id, s := range lr {
		if pr[id] != s {
			d.Differing = append(d.Differing, id)
		}
	}
	for id := range pr {
		if _, ok := lr[id]; !ok {
			d.Differing = append(d.Differing, id)
		}
	}
	sort.Strings(d.Differing)
	if len(d.Differing) > maxListed {
		d.Differing, d.More = d.Differing[:maxListed], true
	}
	return d, true, nil
}

// Repair makes the winner's version of one row win on both nodes: an existing row is re-saved on
// the winner (new timestamp, normal sync carries it over); a row missing on the winner is deleted
// on the other node (delete wins).
func Repair(ctx context.Context, cfg config.Config, tableName, id, winner string) (string, error) {
	if winner != cfg.Node && winner != cfg.Peer {
		return "", fmt.Errorf("winner %q must be %s or %s", winner, cfg.Node, cfg.Peer)
	}
	local, peer, err := connectBoth(ctx, cfg)
	if err != nil {
		return "", err
	}
	defer local.Close(context.Background())
	defer peer.Close(context.Background())
	tables, err := schema.Tables(ctx, local, cfg)
	if err != nil {
		return "", err
	}
	var t *schema.Table
	for i := range tables {
		if tables[i].Name == tableName {
			t = &tables[i]
		}
	}
	if t == nil {
		return "", fmt.Errorf("table %s is not synced", tableName)
	}
	win, lose := local, peer
	if winner == cfg.Peer {
		win, lose = peer, local
	}
	qt := pgx.Identifier{cfg.Schema, t.Name}.Sanitize()
	pk := pgx.Identifier{t.PK}.Sanitize()
	// the stamp trigger sets a new timestamp on any update of the row
	tag, err := win.Exec(ctx, fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s::text = $1", qt, pk, pk, pk), id)
	if err != nil {
		return "", fmt.Errorf("re-save on %s: %w", winner, err)
	}
	if tag.RowsAffected() == 1 {
		return "re-saved on " + winner, nil
	}
	if _, err := lose.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s::text = $1", qt, pk), id); err != nil {
		return "", fmt.Errorf("delete on the other node: %w", err)
	}
	return "deleted (missing on " + winner + ")", nil
}

func connectBoth(ctx context.Context, cfg config.Config) (*pgx.Conn, *pgx.Conn, error) {
	local, err := pgx.Connect(ctx, cfg.LocalDSN)
	if err != nil {
		return nil, nil, fmt.Errorf("connect local: %w", err)
	}
	peer, err := pgx.Connect(ctx, cfg.PeerDSN)
	if err != nil {
		_ = local.Close(ctx)
		return nil, nil, fmt.Errorf("connect peer: %w", err)
	}
	return local, peer, nil
}
