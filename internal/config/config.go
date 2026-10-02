// Package config loads the pg-bisync configuration of one node.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"time"
)

// Unique-key policies for conflicts on unique constraints other than the primary key.
const (
	UniqueStop  = "stop"  // stop applying and alert
	UniqueMerge = "merge" // the row with the later change wins, the other one is deleted on both nodes
)

const (
	defaultUpdatedColumn  = "updated_at"
	defaultPublication    = "bisync"
	defaultSchema         = "public"
	defaultStatusInterval = 10 * time.Second
	defaultRetryMax       = time.Minute
)

// Config is the configuration of one node.
type Config struct {
	Node     string `json:"node"`      // this node, e.g. "in"
	Peer     string `json:"peer"`      // the other node, e.g. "eu"
	LocalDSN string `json:"local_dsn"` // connection to the local database (password via env)
	PeerDSN  string `json:"peer_dsn"`  // connection to the peer database (password via env)

	Schema        string   `json:"schema"`         // schema of the synced tables (default public)
	Tables        []string `json:"tables"`         // synced tables; empty = all tables of Schema with a single-column PK
	UpdatedColumn string   `json:"updated_column"` // last-change column (timestamptz), default updated_at
	TieBreakNode  string   `json:"tie_break_node"` // wins on identical timestamps
	Publication   string   `json:"publication"`    // default bisync

	UniquePolicy map[string]string `json:"unique_policy"` // table → stop | merge (default stop)

	StatusInterval time.Duration `json:"-"`
	RetryMax       time.Duration `json:"-"`
}

var nodeName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,30}$`)

// Load reads a JSON config file and applies environment overrides:
// PGBISYNC_LOCAL_DSN, PGBISYNC_PEER_DSN replace the DSNs (to keep passwords out of the file).
func Load(path string) (Config, error) {
	var c Config
	b, err := os.ReadFile(path)
	if err != nil {
		return c, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("parse config %s: %w", path, err)
	}
	if v := os.Getenv("PGBISYNC_LOCAL_DSN"); v != "" {
		c.LocalDSN = v
	}
	if v := os.Getenv("PGBISYNC_PEER_DSN"); v != "" {
		c.PeerDSN = v
	}
	c.SetDefaults()
	return c, c.Validate()
}

// SetDefaults fills empty optional fields.
func (c *Config) SetDefaults() {
	if c.Schema == "" {
		c.Schema = defaultSchema
	}
	if c.UpdatedColumn == "" {
		c.UpdatedColumn = defaultUpdatedColumn
	}
	if c.Publication == "" {
		c.Publication = defaultPublication
	}
	if c.TieBreakNode == "" {
		c.TieBreakNode = min(c.Node, c.Peer)
	}
	if c.StatusInterval == 0 {
		c.StatusInterval = defaultStatusInterval
	}
	if c.RetryMax == 0 {
		c.RetryMax = defaultRetryMax
	}
}

// Validate checks required fields and allowed values.
func (c Config) Validate() error {
	var errs []error
	if !nodeName.MatchString(c.Node) || !nodeName.MatchString(c.Peer) {
		errs = append(errs, errors.New("node and peer must match [a-z][a-z0-9_]{0,30}"))
	}
	if c.Node == c.Peer {
		errs = append(errs, errors.New("node and peer must differ"))
	}
	if c.LocalDSN == "" || c.PeerDSN == "" {
		errs = append(errs, errors.New("local_dsn and peer_dsn are required"))
	}
	if c.TieBreakNode != c.Node && c.TieBreakNode != c.Peer {
		errs = append(errs, fmt.Errorf("tie_break_node %q must be node or peer", c.TieBreakNode))
	}
	for t, p := range c.UniquePolicy {
		if p != UniqueStop && p != UniqueMerge {
			errs = append(errs, fmt.Errorf("unique_policy[%s] = %q: want stop or merge", t, p))
		}
	}
	return errors.Join(errs...)
}

// SlotName is the replication slot on the peer that this node reads from.
func (c Config) SlotName() string { return "bisync_" + c.Node }

// OriginName is the local replication origin for changes applied from the peer.
func (c Config) OriginName() string { return "bisync_from_" + c.Peer }

// PeerWinsTie reports whether the peer's change wins on identical timestamps.
func (c Config) PeerWinsTie() bool { return c.TieBreakNode == c.Peer }

// UniquePolicyFor returns the unique-key policy of a table.
func (c Config) UniquePolicyFor(table string) string {
	if p, ok := c.UniquePolicy[table]; ok {
		return p
	}
	return UniqueStop
}
