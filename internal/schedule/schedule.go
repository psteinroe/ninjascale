package schedule

import (
	"fmt"
	"time"
)

// Schedule defines time-based bounds overrides.
type Schedule struct {
	Timezone string
	Entries  []Entry
}

// Entry defines a schedule entry.
type Entry struct {
	Days     []time.Weekday
	Start    string // "HH:MM"
	End      string // "HH:MM"
	MinCount int
	MaxCount int
}

// GetActiveBounds returns the min/max bounds for the given time.
// Returns matched=false if no entry matches.
func (s *Schedule) GetActiveBounds(t time.Time) (min, max int, matched bool) {
	loc, err := time.LoadLocation(s.Timezone)
	if err != nil {
		loc = time.UTC
	}

	localTime := t.In(loc)
	weekday := localTime.Weekday()
	currentMinutes := localTime.Hour()*60 + localTime.Minute()

	for _, entry := range s.Entries {
		if !containsDay(entry.Days, weekday) {
			continue
		}

		startMinutes := parseTimeMinutes(entry.Start)
		endMinutes := parseTimeMinutes(entry.End)

		// Handle overnight spans (e.g., 22:00 - 06:00)
		if endMinutes < startMinutes {
			// Overnight: matches if current >= start OR current <= end
			if currentMinutes >= startMinutes || currentMinutes <= endMinutes {
				return entry.MinCount, entry.MaxCount, true
			}
		} else {
			// Normal: matches if start <= current <= end
			if currentMinutes >= startMinutes && currentMinutes <= endMinutes {
				return entry.MinCount, entry.MaxCount, true
			}
		}
	}

	return 0, 0, false
}

func containsDay(days []time.Weekday, day time.Weekday) bool {
	if len(days) == 0 {
		return true // Empty means all days
	}
	for _, d := range days {
		if d == day {
			return true
		}
	}
	return false
}

func parseTimeMinutes(t string) int {
	var h, m int
	_, _ = fmt.Sscanf(t, "%d:%d", &h, &m)
	return h*60 + m
}
