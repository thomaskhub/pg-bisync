package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		env     map[string]string
		wantErr bool
		check   func(t *testing.T, c Config)
	}{
		{
			name: "C1_defaults",
			json: `{"node":"in","peer":"eu","local_dsn":"a","peer_dsn":"b"}`,
			check: func(t *testing.T, c Config) {
				if c.UpdatedColumn != "updated_at" || c.Publication != "bisync" || c.Schema != "public" {
					t.Errorf("defaults not set: %+v", c)
				}
				if c.TieBreakNode != "eu" || !c.PeerWinsTie() {
					t.Errorf("tie break = %q, want eu (alphabetically first)", c.TieBreakNode)
				}
				if c.SlotName() != "bisync_in" || c.OriginName() != "bisync_from_eu" {
					t.Errorf("names: slot %s origin %s", c.SlotName(), c.OriginName())
				}
			},
		},
		{
			name: "C2_env_overrides_dsn",
			json: `{"node":"in","peer":"eu","local_dsn":"file","peer_dsn":"file"}`,
			env:  map[string]string{"PGBISYNC_LOCAL_DSN": "envlocal", "PGBISYNC_PEER_DSN": "envpeer"},
			check: func(t *testing.T, c Config) {
				if c.LocalDSN != "envlocal" || c.PeerDSN != "envpeer" {
					t.Errorf("env not applied: %s %s", c.LocalDSN, c.PeerDSN)
				}
			},
		},
		{name: "C3_same_node", json: `{"node":"in","peer":"in","local_dsn":"a","peer_dsn":"b"}`, wantErr: true},
		{name: "C4_bad_name", json: `{"node":"In-1","peer":"eu","local_dsn":"a","peer_dsn":"b"}`, wantErr: true},
		{name: "C5_missing_dsn", json: `{"node":"in","peer":"eu"}`, wantErr: true},
		{name: "C6_bad_tie", json: `{"node":"in","peer":"eu","local_dsn":"a","peer_dsn":"b","tie_break_node":"us"}`, wantErr: true},
		{name: "C7_bad_policy", json: `{"node":"in","peer":"eu","local_dsn":"a","peer_dsn":"b","unique_policy":{"t":"x"}}`, wantErr: true},
		{name: "C8_bad_json", json: `{`, wantErr: true},
		{
			name: "C9_policy",
			json: `{"node":"in","peer":"eu","local_dsn":"a","peer_dsn":"b","unique_policy":{"asset":"merge"}}`,
			check: func(t *testing.T, c Config) {
				if c.UniquePolicyFor("asset") != UniqueMerge || c.UniquePolicyFor("other") != UniqueStop {
					t.Errorf("policy lookup wrong")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			p := filepath.Join(t.TempDir(), "c.json")
			if err := os.WriteFile(p, []byte(tt.json), 0o600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(p)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.check != nil && err == nil {
				tt.check(t, c)
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "none.json")); err == nil {
		t.Fatal("want error for missing file")
	}
}
