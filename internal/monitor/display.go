package monitor

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/we-be/tritium/pkg/storage"
)

const banner = `
████████╗██████╗ ██╗████████╗██╗██╗   ██╗███╗   ███╗
╚══██╔══╝██╔══██╗██║╚══██╔══╝██║██║   ██║████╗ ████║
   ██║   ██████╔╝██║   ██║   ██║██║   ██║██╔████╔██║
   ██║   ██╔══██╗██║   ██║   ██║██║   ██║██║╚██╔╝██║
   ██║   ██║  ██║██║   ██║   ██║╚██████╔╝██║ ╚═╝ ██║
   ╚═╝   ╚═╝  ╚═╝╚═╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝     ╚═╝
`

const rule = "  ──────────────────────────────────────────────────"

// Render clears the terminal and draws one snapshot.
func Render(w io.Writer, snap Snapshot, summary bool, now time.Time, interval time.Duration) {
	fmt.Fprint(w, "\033[H\033[2J")
	fmt.Fprintf(w, "%s%s%s", BrightMagenta, banner, Reset)
	fmt.Fprintf(w, "%s%s%sCluster Monitor%s\n", BgBlue, BrightWhite, Bold, Reset)
	fmt.Fprintf(w, "\n%s%s%sStatus at %s%s\n", Dim, Italic, White, now.Format("15:04:05"), Reset)

	switch {
	case snap.Err != nil:
		fmt.Fprintf(w, "\n%s%s✗ %v%s\n", Bold, Red, snap.Err, Reset)
	case summary:
		renderSummary(w, snap, now)
	default:
		renderDetailed(w, snap, now)
	}
	fmt.Fprintf(w, "\n%s%s%sPress Ctrl+C to exit • Refreshing every %s%s\n", Dim, Italic, White, interval, Reset)
}

func renderSummary(w io.Writer, snap Snapshot, now time.Time) {
	fmt.Fprintf(w, "\n%s%s%sCluster Summary%s\n\n%s\n", BgBlue, BrightWhite, Bold, Reset, rule)
	for _, n := range snap.Nodes {
		color, symbol := stateStyle(n.State)
		storeColor, storeSymbol := BrightGreen, "✓"
		if !snap.Stores[n.ID].Healthy() {
			storeColor, storeSymbol = BrightYellow, "!"
		}
		fmt.Fprintf(w, "  %s%s%s Node %s [%s%s%s Store] %s%s · %s replicas%s\n",
			color, symbol, Reset, n.Addr, storeColor, storeSymbol, Reset, Dim, n.Version, replicas(n), Reset)
	}
	fmt.Fprintln(w, rule)
	renderEvents(w, snap.Events, 3)
}

func renderDetailed(w io.Writer, snap Snapshot, now time.Time) {
	fmt.Fprintf(w, "\n%s%s%sCluster Status%s\n", BgBlue, BrightWhite, Bold, Reset)
	for _, n := range snap.Nodes {
		color, symbol := stateStyle(n.State)
		fmt.Fprintf(w, "\n%s%s%s Node %s%s\n%s\n", Bold, color, symbol, n.ID, Reset, rule)
		field(w, "Address", BrightYellow, n.Addr)
		field(w, "Version", BrightCyan, n.Version)
		field(w, "Seeds", BrightCyan, seeds(n))
		field(w, "Replicas", BrightMagenta, replicas(n))
		field(w, "Connections", BrightMagenta, fmt.Sprint(n.Stats.ActiveConnections))
		field(w, "Transferred", BrightGreen, bytesString(n.Stats.BytesTransferred))
		field(w, "Writes", BrightGreen, fmt.Sprint(n.Stats.Writes))
		field(w, "Keys", BrightCyan, fmt.Sprint(n.Stats.Keys))
		field(w, "Memory", BrightCyan, bytesString(n.Stats.Memory))
		field(w, "Last Seen", BrightBlue, ago(now.Sub(n.LastSeen)))
		fmt.Fprintln(w, rule)
		renderStore(w, snap.Stores[n.ID])
		fmt.Fprintln(w, rule)
	}
	renderEvents(w, snap.Events, 8)
}

// renderEvents shows the fleet's most recent events, newest first: what
// happened across every node, read from whichever one answered.
func renderEvents(w io.Writer, events []storage.Event, max int) {
	fmt.Fprintf(w, "\n%s%s%sRecent Events%s\n%s\n", BgBlue, BrightWhite, Bold, Reset, rule)
	if len(events) == 0 {
		fmt.Fprintf(w, "  %snone in the last hour%s\n", Dim, Reset)
		fmt.Fprintln(w, rule)
		return
	}
	start := len(events) - max
	if start < 0 {
		start = 0
	}
	for i := len(events) - 1; i >= start; i-- {
		e := events[i]
		line := fmt.Sprintf("  %s%s%s %s%-8s%s node=%s", Dim, time.UnixMilli(e.At).Format("15:04:05"), Reset,
			BrightCyan, e.Event, Reset, strings.TrimPrefix(e.Node, "node-"))
		if e.Peer != "" {
			line += " peer=" + e.Peer
		}
		if e.Keys > 0 {
			line += fmt.Sprintf(" keys=%d", e.Keys)
		}
		if e.Took > 0 {
			line += " took=" + (time.Duration(e.Took) * time.Millisecond).String()
		}
		fmt.Fprintln(w, line)
	}
	fmt.Fprintln(w, rule)
}

// renderStore is one node's store as the node reports it.
func renderStore(w io.Writer, s Store) {
	switch {
	case s.Info == nil:
		fmt.Fprintf(w, "  %s%s✗ Store (%s): node did not answer%s\n", Bold, Red, s.Addr, Reset)
		return
	case !s.Healthy():
		fmt.Fprintf(w, "  %s%s✗ Store (%s): %s%s\n", Bold, Red, s.Addr, s.Info["store_error"], Reset)
		return
	}
	fmt.Fprintf(w, "  %s✓%s Store (%s)\n", BrightGreen, Reset, s.Addr)
	var parts []string
	if v := s.Info["store_version"]; v != "" {
		parts = append(parts, "v"+v)
	}
	if k := s.Info["store_keys"]; k != "" {
		parts = append(parts, k+" keys")
	}
	if used, err := strconv.ParseInt(s.Info["store_used_memory"], 10, 64); err == nil {
		mem := bytesString(used)
		if max, err := strconv.ParseInt(s.Info["store_maxmemory"], 10, 64); err == nil && max > 0 {
			mem += " of " + bytesString(max)
		}
		parts = append(parts, mem)
	}
	if up, err := strconv.ParseInt(s.Info["store_uptime_in_seconds"], 10, 64); err == nil {
		parts = append(parts, "up "+strings.TrimSuffix(ago(time.Duration(up)*time.Second), " ago"))
	}
	if len(parts) > 0 {
		fmt.Fprintf(w, "    %s%s%s\n", Dim, strings.Join(parts, " · "), Reset)
	}
}

func field(w io.Writer, name, color, value string) {
	fmt.Fprintf(w, "  %s%s%s:%s %s%s%s\n", Dim, White, name, Reset, color, value, Reset)
}

func stateStyle(s storage.NodeState) (color, symbol string) {
	switch s {
	case storage.NodeStateHealthy:
		return BrightGreen, "✓"
	case storage.NodeStateDegraded:
		return BrightYellow, "!"
	default:
		return BrightRed, "✗"
	}
}

func seeds(n storage.NodeInfo) string {
	if len(n.Seeds) == 0 {
		return "none (seeded the cluster)"
	}
	return strings.Join(n.Seeds, ", ")
}

// replicas is "2" or, when some stopped answering, "2 (1 held)".
func replicas(n storage.NodeInfo) string {
	if n.Stats.Held > 0 {
		return fmt.Sprintf("%d (%d held)", n.Stats.Replicas, n.Stats.Held)
	}
	return fmt.Sprint(n.Stats.Replicas)
}

func ago(d time.Duration) string {
	switch {
	case d < time.Second:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	}
	return fmt.Sprintf("%dh ago", int(d.Hours()))
}

func bytesString(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for n/div >= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
