package test

import (
	"context"
	"testing"
	"time"

	"github.com/thomkin/pg-bisync/internal/schema"
	"github.com/thomkin/pg-bisync/internal/stream"
)

const typesSchema = `
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE typed (
  id uuid PRIMARY KEY,
  n numeric(20, 6), f float8, i8 bigint, b boolean, d date, ts timestamp, tstz timestamptz, iv interval,
  j json, jb jsonb, ia int[], ta text[], by bytea, ip inet, e mood, t text,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);`

// TestTypes: every common type survives the text round trip (insert, update, NULLs, odd text).
func TestTypes(t *testing.T) {
	c := newCluster(t)
	for _, n := range both(c) {
		n.Exec(t, typesSchema)
		if _, err := schema.Install(context.Background(), n.Pool, c.cfg[n.Name]); err != nil {
			t.Fatal(err)
		}
	}
	c.start()
	c.waitSync(t, 30*time.Second)
	c.in.Exec(t, `INSERT INTO typed VALUES
	  ('6f1c2a7e-0000-4000-8000-000000000001', 12345678901234.123456, 1.5e-300, -9223372036854775808, true,
	   '2028-02-29'::date, '2026-10-02 12:34:56.789', '2026-10-02 12:34:56.789+05:30', '1 year 2 mons 3 days 04:05:06.7',
	   '{"a": [1, 2, {"b": null}]}', '{"x": "ü😀\nquote\"", "n": 1.0}', '{1,NULL,3}', '{"a,b","c\"d",NULL,""}',
	   '\x00ff10', '2001:db8::1/64', 'happy', E'line1\nline2\ttab \\ backslash ''single'' "double" ü😀'),
	  ('6f1c2a7e-0000-4000-8000-000000000002', NULL, 'NaN', NULL, NULL, NULL, NULL, '-infinity', NULL,
	   NULL, NULL, '{}', NULL, '', NULL, NULL, '')`)
	c.waitSync(t, 30*time.Second)
	if d := c.in.Checksum(t, "typed")["typed"]; d != c.eu.Checksum(t, "typed")["typed"] {
		t.Fatal("typed rows differ after insert")
	}
	c.in.Exec(t, `UPDATE typed SET jb = jb || '{"y": [true]}', ta = ta || '{e}', f = 'Infinity', by = '\xdeadbeef' WHERE e = 'happy'`)
	c.eu.Exec(t, `UPDATE typed SET n = -0.000001, iv = '-1 day', ip = '10.0.0.1' WHERE n IS NULL`)
	c.waitSync(t, 30*time.Second)
	if c.in.Checksum(t, "typed")["typed"] != c.eu.Checksum(t, "typed")["typed"] {
		t.Fatal("typed rows differ after updates")
	}
}

// TestKeyText: the key of a change comes from the old key tuple for deletes and from the new
// row otherwise (unit test, no database).
func TestKeyText(t *testing.T) {
	rel := &stream.Relation{Name: "t", Columns: []stream.Column{{Name: "id", Key: true}, {Name: "v"}}}
	text := func(s string) stream.Value { return stream.Value{Kind: stream.ValueText, Text: s} }
	tests := []struct {
		name    string
		c       stream.Change
		want    string
		wantErr bool
	}{
		{"K1_insert", stream.Change{Op: stream.Insert, Rel: rel, New: []stream.Value{text("a"), text("x")}}, "a", false},
		{"K2_update_no_key_change", stream.Change{Op: stream.Update, Rel: rel, New: []stream.Value{text("b"), text("x")}}, "b", false},
		{"K3_delete", stream.Change{Op: stream.Delete, Rel: rel, Key: []stream.Value{text("c"), {Kind: stream.ValueNull}}}, "c", false},
		{"K4_null_key", stream.Change{Op: stream.Insert, Rel: rel, New: []stream.Value{{Kind: stream.ValueNull}, text("x")}}, "", true},
		{"K5_no_key_column", stream.Change{Op: stream.Insert, Rel: &stream.Relation{Columns: []stream.Column{{Name: "v"}}}, New: []stream.Value{text("x")}}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.KeyText()
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Errorf("KeyText() = %q, %v; want %q, err %v", got, err, tt.want, tt.wantErr)
			}
		})
	}
}
