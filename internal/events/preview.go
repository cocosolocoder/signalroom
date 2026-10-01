package events

import (
	"encoding/binary"
	"errors"
	"sort"
	"strings"
	"time"
)

// Preview caps. A single preview may not split its range into more than
// maxPreviewWindows windows, and the total number of group/window cells is
// capped at maxPreviewGroupWindowCells so a request cannot grow without bound
// as the label cardinality rises.
const (
	maxPreviewWindows          = 10000
	maxPreviewGroupWindowCells = 100000
	maxPreviewGroupBy          = 4
)

// Window-end states a group can be in.
const (
	previewStateNormal   = "normal"
	previewStateAlerting = "alerting"
)

// Parameter problems a preview request can hit. The HTTP layer maps each to
// its own 400 message.
var (
	// ErrPreviewWindow reports a non-positive window or a range that is not
	// strictly increasing.
	ErrPreviewWindow = errors.New("preview window must be positive and since must be before until")
	// ErrPreviewThreshold reports a count threshold outside 1..1000000.
	ErrPreviewThreshold = errors.New("preview threshold must be between 1 and 1000000")
	// ErrPreviewStreak reports a trigger or recovery streak outside 1..100.
	ErrPreviewStreak = errors.New("preview trigger and recovery windows must each be between 1 and 100")
	// ErrPreviewGroupBy reports more than maxPreviewGroupBy label names.
	ErrPreviewGroupBy = errors.New("preview may group by at most 4 label names")
	// ErrPreviewTooManyWindows reports a range needing more than
	// maxPreviewWindows windows.
	ErrPreviewTooManyWindows = errors.New("preview range requires too many windows")
	// ErrPreviewTooManyGroups reports a group/window cell count over the cap.
	ErrPreviewTooManyGroups = errors.New("preview requires too many group-window cells")
)

// AlertPreviewQuery selects the half-open [Since, Until) range, the window
// size in seconds, the count threshold, the consecutive-window streaks needed
// to trigger and to recover, and the optional service, severity, label
// filters plus up to four label names to group by.
type AlertPreviewQuery struct {
	Service        string
	Severity       string
	Labels         map[string]string
	Since          time.Time
	Until          time.Time
	Window         time.Duration
	Threshold      int
	TriggerWindows int
	RecoverWindows int
	GroupBy        []string
}

// AlertWindow is one half-open [Since, Until) slice with its event count and
// the state of its group at the window's end. The final window ends exactly
// at the range end even when it is shorter than a full window.
type AlertWindow struct {
	Since time.Time
	Until time.Time
	Count int
	State string
}

// Alert is one triggered incident. RecoveredAt is nil when the group is still
// alerting at the end of the preview range; such an alert stays open and is
// never closed automatically.
type Alert struct {
	Service     string
	Labels      map[string]*string
	TriggeredAt time.Time
	RecoveredAt *time.Time
}

// AlertGroup is one service/label combination that matched at least one event
// inside the range. Labels carries exactly the GroupBy names in request
// order, with a nil value for a label the group's events lack.
type AlertGroup struct {
	Service string
	Labels  map[string]*string
	Windows []AlertWindow
	Alerts  []Alert
}

// AlertPreview is the full result: every matched group with its per-window
// counts and states, plus every alert it produced.
type AlertPreview struct {
	Groups []AlertGroup
	Alerts []Alert
}

// previewGroupKey identifies one service/group-by combination. A nil element
// in Values means the event carried no label with that group-by name.
type previewGroupKey struct {
	service string
	values  []*string
}

// encode renders the key as a length-prefixed record so it can be used as a
// map key despite holding a slice: service, then for each group-by value a
// nil marker or a present marker followed by the length-prefixed value.
func (k previewGroupKey) encode() string {
	var b strings.Builder
	var length [8]byte
	writeString := func(s string) {
		binary.BigEndian.PutUint64(length[:], uint64(len(s)))
		b.Write(length[:])
		b.WriteString(s)
	}
	writeString(k.service)
	for _, value := range k.values {
		if value == nil {
			b.WriteByte(0)
			continue
		}
		b.WriteByte(1)
		writeString(*value)
	}
	return b.String()
}

// PreviewAlerts counts filtered events per window for every service (and
// optionally label) combination present in the range, then walks each group
// through its trigger/recovery state machine. The whole result is computed
// from one read of the timeline, so a batch committed concurrently is either
// fully present or fully absent.
func (t *Timeline) PreviewAlerts(query AlertPreviewQuery) (AlertPreview, error) {
	if err := validatePreviewQuery(query); err != nil {
		return AlertPreview{}, err
	}

	windowCount := previewWindowCount(query.Since, query.Until, query.Window)
	if windowCount > maxPreviewWindows {
		return AlertPreview{}, ErrPreviewTooManyWindows
	}

	service := strings.TrimSpace(query.Service)
	severity := strings.ToLower(strings.TrimSpace(query.Severity))

	t.mu.RLock()
	counts := t.previewCounts(query, service, severity, windowCount)
	t.mu.RUnlock()

	if len(counts)*windowCount > maxPreviewGroupWindowCells {
		return AlertPreview{}, ErrPreviewTooManyGroups
	}

	entries := make([]previewGroupEntry, 0, len(counts))
	for _, entry := range counts {
		entries = append(entries, *entry)
	}
	sortPreviewEntries(entries)

	groups := make([]AlertGroup, 0, len(entries))
	alerts := make([]Alert, 0)
	for _, entry := range entries {
		windows := buildPreviewWindows(query.Since, query.Until, query.Window, entry.counts)
		groupAlerts := evaluatePreview(windows, query.Threshold, query.TriggerWindows, query.RecoverWindows)
		labels := make(map[string]*string, len(query.GroupBy))
		for i, name := range query.GroupBy {
			labels[name] = entry.key.values[i]
		}
		for i := range groupAlerts {
			groupAlerts[i].Service = entry.key.service
			groupAlerts[i].Labels = labels
		}
		groups = append(groups, AlertGroup{
			Service: entry.key.service,
			Labels:  labels,
			Windows: windows,
			Alerts:  groupAlerts,
		})
		alerts = append(alerts, groupAlerts...)
	}
	sortPreviewAlerts(alerts)
	return AlertPreview{Groups: groups, Alerts: alerts}, nil
}

func validatePreviewQuery(query AlertPreviewQuery) error {
	if query.Window <= 0 || !query.Since.Before(query.Until) {
		return ErrPreviewWindow
	}
	if query.Threshold < 1 || query.Threshold > 1000000 {
		return ErrPreviewThreshold
	}
	if query.TriggerWindows < 1 || query.TriggerWindows > 100 ||
		query.RecoverWindows < 1 || query.RecoverWindows > 100 {
		return ErrPreviewStreak
	}
	if len(query.GroupBy) > maxPreviewGroupBy {
		return ErrPreviewGroupBy
	}
	return nil
}

// previewWindowCount returns the number of whole windows plus one short final
// window when the range length is not divisible by the window size.
func previewWindowCount(since, until time.Time, window time.Duration) int {
	length := until.Sub(since)
	count := length / window
	if length%window != 0 {
		count++
	}
	return int(count)
}

// previewGroupEntry pairs a group key with its per-window counts.
type previewGroupEntry struct {
	key    previewGroupKey
	counts []int
}

// previewCounts buckets every in-range, filter-matching event by group and
// window. Only groups with at least one matching event are created. The
// caller holds the timeline lock.
func (t *Timeline) previewCounts(query AlertPreviewQuery, service, severity string, windowCount int) map[string]*previewGroupEntry {
	counts := make(map[string]*previewGroupEntry)
	for _, event := range t.events {
		if service != "" && event.Service != service {
			continue
		}
		if severity != "" && event.Severity != severity {
			continue
		}
		if !labelsMatch(event.Labels, query.Labels) {
			continue
		}
		index, ok := bucketIndex(event.At, query.Since, query.Until, query.Window)
		if !ok {
			continue
		}
		key := previewGroupKey{service: event.Service}
		if len(query.GroupBy) > 0 {
			key.values = make([]*string, len(query.GroupBy))
			for i, name := range query.GroupBy {
				if value, ok := event.Labels[name]; ok {
					v := value
					key.values[i] = &v
				}
			}
		}
		entry := counts[key.encode()]
		if entry == nil {
			entry = &previewGroupEntry{key: key, counts: make([]int, windowCount)}
			counts[key.encode()] = entry
		}
		entry.counts[index]++
	}
	return counts
}

// buildPreviewWindows lays windows end to end from the range start, shortening
// the final one to the range end, and pairs them with the group's counts.
func buildPreviewWindows(since, until time.Time, window time.Duration, counts []int) []AlertWindow {
	windows := make([]AlertWindow, len(counts))
	start := since
	for i := range counts {
		end := start.Add(window)
		if end.After(until) {
			end = until
		}
		windows[i] = AlertWindow{Since: start, Until: end, Count: counts[i], State: previewStateNormal}
		start = end
	}
	return windows
}

// evaluatePreview walks a group through the state machine. The group starts
// normal with no history from before the range:
//
//   - normal: consecutive windows at or above the threshold reaching
//     triggerWindows open an alert at the end of the trigger window; a window
//     below the threshold restarts the streak.
//   - alerting: the alert stays open while counts stay high; consecutive
//     windows below the threshold reaching recoverWindows recover at the end
//     of that window; a window at or above the threshold restarts the
//     recovery streak.
//
// A group still alerting at the range end keeps a nil recovery time and is
// never closed automatically.
func evaluatePreview(windows []AlertWindow, threshold, triggerWindows, recoverWindows int) []Alert {
	alerts := make([]Alert, 0)
	alerting := false
	highStreak := 0
	lowStreak := 0
	var current *Alert

	for i := range windows {
		if windows[i].Count >= threshold {
			highStreak++
			lowStreak = 0
		} else {
			lowStreak++
			highStreak = 0
		}

		if alerting {
			if lowStreak >= recoverWindows {
				alerting = false
				lowStreak = 0
				windows[i].State = previewStateNormal
				recovered := windows[i].Until
				current.RecoveredAt = &recovered
				current = nil
			} else {
				windows[i].State = previewStateAlerting
			}
			continue
		}

		if highStreak >= triggerWindows {
			alerting = true
			highStreak = 0
			windows[i].State = previewStateAlerting
			alerts = append(alerts, Alert{TriggeredAt: windows[i].Until})
			current = &alerts[len(alerts)-1]
		} else {
			windows[i].State = previewStateNormal
		}
	}
	return alerts
}

// sortPreviewEntries orders groups by service name, then by the group-by
// values in request order, with a missing value sorting before any string.
func sortPreviewEntries(entries []previewGroupEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return comparePreviewGroupKeys(entries[i].key, entries[j].key)
	})
}

func comparePreviewGroupKeys(a, b previewGroupKey) bool {
	if a.service != b.service {
		return a.service < b.service
	}
	for i := range a.values {
		av, bv := a.values[i], b.values[i]
		if av == nil {
			if bv != nil {
				return true
			}
			continue
		}
		if bv == nil {
			return false
		}
		if *av != *bv {
			return *av < *bv
		}
	}
	return false
}

// sortPreviewAlerts orders alerts by trigger instant. Equal instants keep the
// group order the groups were built in (service, then label values).
func sortPreviewAlerts(alerts []Alert) {
	sort.SliceStable(alerts, func(i, j int) bool {
		return alerts[i].TriggeredAt.Before(alerts[j].TriggeredAt)
	})
}
