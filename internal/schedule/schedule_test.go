package schedule

import (
	"testing"
	"time"
)

func TestSchedule_GetActiveBounds(t *testing.T) {
	sched := &Schedule{
		Timezone: "UTC",
		Entries: []Entry{
			{
				Days:     []time.Weekday{time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday},
				Start:    "08:00",
				End:      "18:00",
				MinCount: 3,
				MaxCount: 20,
			},
			{
				Days:     []time.Weekday{time.Saturday, time.Sunday},
				Start:    "00:00",
				End:      "23:59",
				MinCount: 1,
				MaxCount: 5,
			},
		},
	}

	tests := []struct {
		name        string
		time        time.Time
		wantMin     int
		wantMax     int
		wantMatched bool
	}{
		{
			name:        "weekday business hours",
			time:        time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC), // Monday 10:00
			wantMin:     3,
			wantMax:     20,
			wantMatched: true,
		},
		{
			name:        "weekday before hours",
			time:        time.Date(2024, 1, 15, 6, 0, 0, 0, time.UTC), // Monday 06:00
			wantMatched: false,
		},
		{
			name:        "weekday after hours",
			time:        time.Date(2024, 1, 15, 20, 0, 0, 0, time.UTC), // Monday 20:00
			wantMatched: false,
		},
		{
			name:        "weekend",
			time:        time.Date(2024, 1, 13, 14, 0, 0, 0, time.UTC), // Saturday 14:00
			wantMin:     1,
			wantMax:     5,
			wantMatched: true,
		},
		{
			name:        "edge: exactly at start",
			time:        time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC), // Monday 08:00
			wantMin:     3,
			wantMax:     20,
			wantMatched: true,
		},
		{
			name:        "edge: exactly at end",
			time:        time.Date(2024, 1, 15, 18, 0, 0, 0, time.UTC), // Monday 18:00
			wantMin:     3,
			wantMax:     20,
			wantMatched: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			min, max, matched := sched.GetActiveBounds(tt.time)
			if matched != tt.wantMatched {
				t.Errorf("matched = %v, want %v", matched, tt.wantMatched)
			}
			if matched {
				if min != tt.wantMin {
					t.Errorf("min = %d, want %d", min, tt.wantMin)
				}
				if max != tt.wantMax {
					t.Errorf("max = %d, want %d", max, tt.wantMax)
				}
			}
		})
	}
}

func TestSchedule_Timezone(t *testing.T) {
	sched := &Schedule{
		Timezone: "Europe/Berlin",
		Entries: []Entry{
			{
				Days:     []time.Weekday{time.Monday},
				Start:    "09:00",
				End:      "17:00",
				MinCount: 5,
				MaxCount: 10,
			},
		},
	}

	// 09:00 Berlin = 08:00 UTC (in winter)
	utcTime := time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC) // Monday

	min, max, matched := sched.GetActiveBounds(utcTime)
	if !matched {
		t.Fatal("expected match")
	}
	if min != 5 {
		t.Errorf("min = %d, want 5", min)
	}
	if max != 10 {
		t.Errorf("max = %d, want 10", max)
	}
}

func TestSchedule_OvernightSpan(t *testing.T) {
	sched := &Schedule{
		Timezone: "UTC",
		Entries: []Entry{
			{
				Days:     []time.Weekday{time.Monday, time.Tuesday},
				Start:    "22:00",
				End:      "06:00",
				MinCount: 0,
				MaxCount: 2,
			},
		},
	}

	tests := []struct {
		name        string
		time        time.Time
		wantMatched bool
	}{
		{
			name:        "before midnight",
			time:        time.Date(2024, 1, 15, 23, 0, 0, 0, time.UTC), // Monday 23:00
			wantMatched: true,
		},
		{
			name:        "after midnight",
			time:        time.Date(2024, 1, 16, 3, 0, 0, 0, time.UTC), // Tuesday 03:00
			wantMatched: true,
		},
		{
			name:        "outside window",
			time:        time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC), // Monday 12:00
			wantMatched: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, matched := sched.GetActiveBounds(tt.time)
			if matched != tt.wantMatched {
				t.Errorf("matched = %v, want %v", matched, tt.wantMatched)
			}
		})
	}
}
