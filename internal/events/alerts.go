package events

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Alert preview limits. The range is split from Since into windows of Window
// seconds; a request needing more than maxAlertWindows windows, or whose
// realized groups would require more than maxAlertGroupWindows window slots
// in total, is rejected rather than truncated.
const (
	// MaxAlertGroupLabels bounds how many label names one preview may group by.
	MaxAlertGroupLabels = 4

	maxAlertWindows      = 10000
	maxAlertGroupWindows = 100000

	minAlertWindowSeconds = 1
	maxAlertWindowSeconds = 86400
	minAlertThreshold     = 1
	maxAlertThreshold     = 1000000
	minAlertConsecutive   = 1
	maxAlertConsecutive   = 100
)

// Alert preview validation failures, mapped to 400 responses by the HTTP
// layer.
var (
	// ErrAlertRange reports a range whose start is not strictly before its end.
	ErrAlertRange = errors.New("alert preview range must start before it ends")
	// ErrAlertWindow reports a window outside 1..86400 seconds.
	ErrAlertWindow = errors.New("alert preview window must be an integer from 1 to 86400 seconds")
	// ErrAlertThreshold reports a threshold outside 1..1000000.
	ErrAlertThreshold = errors.New("alert preview threshold must be an integer from 1 to 1000000")
	// ErrAlertConsecutive reports a trigger/recovery run outside 1..100.
	ErrAlertConsecutive = errors.New("alert preview consecutive window counts must be integers from 1 to 100")
	// ErrAlertGroupLabels reports more than four group labels.
	ErrAlertGroupLabels = errors.New("alert preview may group by at most four labels")
	// ErrAlertTooManyWindows reports a range needing more than 10000 windows.
	ErrAlertTooManyWindows = errors.New("alert preview range requires more than 10000 windows")
	// ErrAlertTooManyGroups reports groups*windows exceeding 100000.
	ErrAlertTooManyGroups = errors.New("alert preview produces more than 100000 group windows")
)

// AlertPreviewQuery selects a half-open range [Since, Until) split into
// windows of Window seconds and evaluated independently per service, or per
// service plus the label values named in GroupLabels. Service, Severity,
// Labels, and GroupLabels must already be normalized the way Query uses.
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
	GroupLabels    []string
}

// AlertWindow is one window in a group: the half-open [Start, End) bounds,
// the events counted inside, and the state at the window end. Status is
// AlertStatusNormal or AlertStatusAlerting.
type AlertWindow struct {
	Start  time.Time
	End    time.Time
	Count  int
	Status string
}

// AlertRecord is one open/close cycle. OpenedAt is the end of the window in
// which the trigger run completed; RecoveredAt is the end of the window in
// which the recovery run completed, or nil when the alert is still open at
// the range end.
type AlertRecord struct {
	OpenedAt    time.Time
	RecoveredAt *time.Time
}

// AlertGroup is the evaluated timeline of one service (and label-value
// combination when group labels are given). LabelValues aligns with the
// result's sorted group label names; a nil entry means the group's events
// carried no such label.
type AlertGroup struct {
	Service     string
	LabelValues []*string
	Windows     []AlertWindow
	Alerts      []AlertRecord
}

// AlertPreviewResult is the full preview. GroupLabelNames lists the grouping
// labels in lexicographic order so groups' LabelValues are self-describing;
// it is nil when grouping is by service alone.
type AlertPreviewResult struct {
	GroupLabelNames []string
	Groups          []AlertGroup
}

// Preview statuses. Every group starts a preview in normal state; nothing
// before Since is inherited.
const (
	AlertStatusNormal   = "normal"
	AlertStatusAlerting = "alerting"
)

// alertGroupKey identifies one evaluated series. Values align with the
// lexicographically sorted group label names; nil entries are missing
// labels.
type alertGroupKey struct {
	service string
	values  []*string
}

// groupKey encodes a key unambiguously with length-prefixed string fields so
// values containing the separator cannot collide, and a distinct prefix for
// missing values.
func groupKeyString(service string, values []*string) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(len(service)))
	b.WriteByte(':')
	b.WriteString(service)
	for _, v := range values {
		if v == nil {
			b.WriteString("n:")
			continue
		}
		b.WriteByte('s')
		b.WriteString(strconv.Itoa(len(*v)))
		b.WriteByte(':')
		b.WriteString(*v)
	}
	return b.String()
}

// PreviewAlerts scans the timeline once and replays the trigger/recovery
// rules over the committed snapshot, so a batch ingested during the call is
// either counted in full or not at all, and retries of identical events add
// nothing.
func (t *Timeline) PreviewAlerts(query AlertPreviewQuery) (AlertPreviewResult, error) {
	names, windowCount, err := validateAlertQuery(query)
	if err != nil {
		return AlertPreviewResult{}, err
	}

	filter := NewFilter(query.Service, query.Severity, query.Labels)

	// Sparse counts collected under one read lock: encoded group key ->
	// window index -> count. keys remembers the decoded identity of each
	// encoded key. Dense per-group windows are only built after the size
	// check, so a request that would exceed the group-window cap costs only
	// the events it touches.
	counts := make(map[string]map[int]int)
	keys := make(map[string]alertGroupKey)

	t.mu.RLock()
	for _, event := range t.events {
		if !filter.Matches(event) {
			continue
		}
		index, ok := bucketIndex(event.At, query.Since, query.Until, query.Window)
		if !ok {
			continue
		}
		key := alertGroupKey{service: event.Service}
		if len(names) > 0 {
			key.values = make([]*string, len(names))
			for i, name := range names {
				if value, has := event.Labels[name]; has {
					value := value
					key.values[i] = &value
				}
			}
		}
		encoded := groupKeyString(key.service, key.values)
		groupCounts := counts[encoded]
		if groupCounts == nil {
			groupCounts = make(map[int]int)
			counts[encoded] = groupCounts
			keys[encoded] = key
		}
		groupCounts[index]++
	}
	t.mu.RUnlock()

	if len(counts)*windowCount > maxAlertGroupWindows {
		return AlertPreviewResult{}, ErrAlertTooManyGroups
	}

	ordered := make([]alertGroupKey, 0, len(keys))
	for _, key := range keys {
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return groupKeyLess(ordered[i], ordered[j])
	})

	groups := make([]AlertGroup, 0, len(ordered))
	for _, key := range ordered {
		groups = append(groups, evaluateAlertGroup(query, windowCount, key, counts[groupKeyString(key.service, key.values)]))
	}

	return AlertPreviewResult{GroupLabelNames: names, Groups: groups}, nil
}

// validateAlertQuery enforces every range and parameter bound and returns
// the sorted group label names together with the number of windows.
func validateAlertQuery(query AlertPreviewQuery) ([]string, int, error) {
	if !query.Until.After(query.Since) {
		return nil, 0, ErrAlertRange
	}
	windowSeconds := int64(query.Window / time.Second)
	if query.Window <= 0 || query.Window%time.Second != 0 ||
		windowSeconds < minAlertWindowSeconds || windowSeconds > maxAlertWindowSeconds {
		return nil, 0, ErrAlertWindow
	}
	if query.Threshold < minAlertThreshold || query.Threshold > maxAlertThreshold {
		return nil, 0, ErrAlertThreshold
	}
	if query.TriggerWindows < minAlertConsecutive || query.TriggerWindows > maxAlertConsecutive ||
		query.RecoverWindows < minAlertConsecutive || query.RecoverWindows > maxAlertConsecutive {
		return nil, 0, ErrAlertConsecutive
	}
	if len(query.GroupLabels) > MaxAlertGroupLabels {
		return nil, 0, ErrAlertGroupLabels
	}
	names := append([]string(nil), query.GroupLabels...)
	sort.Strings(names)

	length := query.Until.Sub(query.Since)
	count := length / query.Window
	if length%query.Window != 0 {
		count++
	}
	if count > maxAlertWindows {
		return nil, 0, ErrAlertTooManyWindows
	}
	return names, int(count), nil
}

// groupKeyLess orders keys by service, then label values in name order, with
// a missing (nil) value sorting before any string.
func groupKeyLess(a, b alertGroupKey) bool {
	if a.service != b.service {
		return a.service < b.service
	}
	for i := range a.values {
		x, y := a.values[i], b.values[i]
		switch {
		case x == nil && y != nil:
			return true
		case x != nil && y == nil:
			return false
		case x == nil && y == nil:
			continue
		case *x != *y:
			return *x < *y
		}
	}
	return false
}

// evaluateAlertGroup expands one group's sparse counts into the complete,
// zero-filled window set and walks the trigger/recovery state machine.
func evaluateAlertGroup(query AlertPreviewQuery, windowCount int, key alertGroupKey, sparse map[int]int) AlertGroup {
	windows := make([]AlertWindow, windowCount)
	alerts := make([]AlertRecord, 0)

	alerting := false
	triggerRun := 0
	recoverRun := 0

	for i := 0; i < windowCount; i++ {
		start := query.Since.Add(query.Window * time.Duration(i))
		end := start.Add(query.Window)
		if end.After(query.Until) {
			end = query.Until
		}
		count := sparse[i]

		if !alerting {
			if count >= query.Threshold {
				triggerRun++
				if triggerRun >= query.TriggerWindows {
					alerting = true
					triggerRun = 0
					alerts = append(alerts, AlertRecord{OpenedAt: end})
				}
			} else {
				triggerRun = 0
			}
		} else {
			if count < query.Threshold {
				recoverRun++
				if recoverRun >= query.RecoverWindows {
					alerting = false
					recoverRun = 0
					recoveredAt := end
					alerts[len(alerts)-1].RecoveredAt = &recoveredAt
				}
			} else {
				recoverRun = 0
			}
		}

		status := AlertStatusNormal
		if alerting {
			status = AlertStatusAlerting
		}
		windows[i] = AlertWindow{Start: start, End: end, Count: count, Status: status}
	}

	return AlertGroup{
		Service:     key.service,
		LabelValues: key.values,
		Windows:     windows,
		Alerts:      alerts,
	}
}
