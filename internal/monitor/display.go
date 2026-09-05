package monitor

import (
	"fmt"
	"io"
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
		if !storeHealthy(snap.Stores[n.StoreAddr]) {
			storeColor, storeSymbol = BrightYellow, "!"
		}
		fmt.Fprintf(w, "  %s%s%s Node %s [%s%s%s Store] %s\n",
			color, symbol, Reset, n.Addr, storeColor, storeSymbol, Reset, role(n))
	}
	fmt.Fprintln(w, rule)
}

func renderDetailed(w io.Writer, snap Snapshot, now time.Time) {
	fmt.Fprintf(w, "\n%s%s%sCluster Status%s\n", BgBlue, BrightWhite, Bold, Reset)
	for _, n := range snap.Nodes {
		color, symbol := stateStyle(n.State)
		fmt.Fprintf(w, "\n%s%s%s Node %s%s\n%s\n", Bold, color, symbol, n.ID, Reset, rule)
		field(w, "Role", BrightCyan, role(n))
		field(w, "Address", BrightYellow, n.Addr)
		field(w, "Store", BrightYellow, n.StoreAddr)
		field(w, "Connections", BrightMagenta, fmt.Sprint(n.Stats.ActiveConnections))
		field(w, "Transferred", BrightGreen, bytesString(n.Stats.BytesTransferred))
		field(w, "Last Seen", BrightBlue, ago(now.Sub(n.LastSeen)))
		if s, ok := snap.Stores[n.StoreAddr]; ok {
			fmt.Fprintln(w, rule)
			renderStore(w, s, "Primary")
			for i, r := range s.Replicas {
				renderStore(w, r, fmt.Sprintf("Replica %d", i+1))
			}
		}
		fmt.Fprintln(w, rule)
	}
}

func renderStore(w io.Writer, s Store, name string) {
	if s.Info == nil {
		fmt.Fprintf(w, "  %s%s✗ %s (%s) unreachable%s\n", Bold, Red, name, s.Addr, Reset)
		return
	}
	color, symbol := BrightGreen, "✓"
	if !s.Healthy() {
		color, symbol = BrightRed, "✗"
	}
	fmt.Fprintf(w, "  %s%s%s %s (%s)\n", color, symbol, Reset, name, s.Addr)
	if s.Role() == "master" {
		fmt.Fprintf(w, "    %s%sRole:%s Primary, replicas: %s%s%s\n", Dim, White, Reset, BrightYellow, s.Info["connected_slaves"], Reset)
	} else {
		fmt.Fprintf(w, "    %s%sRole:%s Replica, link: %s%s%s\n", Dim, White, Reset, color, s.Info["master_link_status"], Reset)
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

func storeHealthy(s Store) bool {
	if !s.Healthy() {
		return false
	}
	for _, r := range s.Replicas {
		if !r.Healthy() {
			return false
		}
	}
	return true
}

func role(n storage.NodeInfo) string {
	if n.IsLeader {
		return "Seed"
	}
	return "Member"
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
