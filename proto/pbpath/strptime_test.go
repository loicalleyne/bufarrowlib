package pbpath

import (
	"testing"
	"time"
)

func TestParseStrptime(t *testing.T) {
	// ── DuckDB format specifiers ──────────────────────────────────────

	t.Run("basic_date", func(t *testing.T) {
		got, err := ParseStrptime("%Y-%m-%d", "2024-03-15")
		if err != nil {
			t.Fatal(err)
		}
		want := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("date_time", func(t *testing.T) {
		got, err := ParseStrptime("%Y-%m-%d %H:%M:%S", "2024-12-25 23:59:59")
		if err != nil {
			t.Fatal(err)
		}
		want := time.Date(2024, 12, 25, 23, 59, 59, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("two_digit_year", func(t *testing.T) {
		got, err := ParseStrptime("%y-%m-%d", "24-01-01")
		if err != nil {
			t.Fatal(err)
		}
		if got.Year() != 2024 {
			t.Fatalf("expected year 2024, got %d", got.Year())
		}
	})

	t.Run("microseconds", func(t *testing.T) {
		got, err := ParseStrptime("%Y-%m-%d %H:%M:%S.%f", "2024-01-01 12:00:00.123456")
		if err != nil {
			t.Fatal(err)
		}
		want := time.Date(2024, 1, 1, 12, 0, 0, 123456000, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("microseconds_short", func(t *testing.T) {
		got, err := ParseStrptime("%Y-%m-%d %H:%M:%S.%f", "2024-01-01 12:00:00.12")
		if err != nil {
			t.Fatal(err)
		}
		// "12" → right-padded to "120000" → 120000 µs
		want := time.Date(2024, 1, 1, 12, 0, 0, 120000000, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("12h_am", func(t *testing.T) {
		got, err := ParseStrptime("%I:%M %p", "09:30 AM")
		if err != nil {
			t.Fatal(err)
		}
		if got.Hour() != 9 || got.Minute() != 30 {
			t.Fatalf("expected 09:30, got %02d:%02d", got.Hour(), got.Minute())
		}
	})

	t.Run("12h_pm", func(t *testing.T) {
		got, err := ParseStrptime("%I:%M %p", "03:45 PM")
		if err != nil {
			t.Fatal(err)
		}
		if got.Hour() != 15 || got.Minute() != 45 {
			t.Fatalf("expected 15:45, got %02d:%02d", got.Hour(), got.Minute())
		}
	})

	t.Run("12h_noon", func(t *testing.T) {
		got, err := ParseStrptime("%I:%M %p", "12:00 PM")
		if err != nil {
			t.Fatal(err)
		}
		if got.Hour() != 12 {
			t.Fatalf("expected 12, got %d", got.Hour())
		}
	})

	t.Run("12h_midnight", func(t *testing.T) {
		got, err := ParseStrptime("%I:%M %p", "12:00 AM")
		if err != nil {
			t.Fatal(err)
		}
		if got.Hour() != 0 {
			t.Fatalf("expected 0 (midnight), got %d", got.Hour())
		}
	})

	t.Run("utc_offset_z", func(t *testing.T) {
		got, err := ParseStrptime("%Y-%m-%dT%H:%M:%S%z", "2024-01-01T12:00:00Z")
		if err != nil {
			t.Fatal(err)
		}
		if got.Location().String() != "UTC" {
			t.Fatalf("expected UTC, got %v", got.Location())
		}
	})

	t.Run("utc_offset_plus", func(t *testing.T) {
		got, err := ParseStrptime("%Y-%m-%dT%H:%M:%S%z", "2024-01-01T12:00:00+0530")
		if err != nil {
			t.Fatal(err)
		}
		_, offset := got.Zone()
		if offset != 5*3600+30*60 {
			t.Fatalf("expected offset 19800, got %d", offset)
		}
	})

	t.Run("utc_offset_colon", func(t *testing.T) {
		got, err := ParseStrptime("%Y-%m-%dT%H:%M:%S%z", "2024-01-01T12:00:00+05:30")
		if err != nil {
			t.Fatal(err)
		}
		_, offset := got.Zone()
		if offset != 5*3600+30*60 {
			t.Fatalf("expected offset 19800, got %d", offset)
		}
	})

	t.Run("utc_offset_minus", func(t *testing.T) {
		got, err := ParseStrptime("%Y-%m-%dT%H:%M:%S%z", "2024-01-01T12:00:00-0800")
		if err != nil {
			t.Fatal(err)
		}
		_, offset := got.Zone()
		if offset != -8*3600 {
			t.Fatalf("expected offset -28800, got %d", offset)
		}
	})

	t.Run("timezone_name", func(t *testing.T) {
		// %Z is consumed but best-effort; result depends on system TZ database
		_, err := ParseStrptime("%Y-%m-%d %Z", "2024-01-01 UTC")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("day_of_year", func(t *testing.T) {
		got, err := ParseStrptime("%Y-%j", "2024-075")
		if err != nil {
			t.Fatal(err)
		}
		// Day 75 of 2024 (leap year) = March 15
		if got.Month() != 3 || got.Day() != 15 {
			t.Fatalf("expected March 15, got %v %d", got.Month(), got.Day())
		}
	})

	t.Run("abbreviated_weekday", func(t *testing.T) {
		// %a is consumed and ignored
		got, err := ParseStrptime("%a, %d %b %Y", "Fri, 15 Mar 2024")
		if err != nil {
			t.Fatal(err)
		}
		want := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("full_weekday", func(t *testing.T) {
		got, err := ParseStrptime("%A, %d %B %Y", "Friday, 15 March 2024")
		if err != nil {
			t.Fatal(err)
		}
		want := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("month_name_abbreviated", func(t *testing.T) {
		got, err := ParseStrptime("%d-%b-%Y", "25-Dec-2024")
		if err != nil {
			t.Fatal(err)
		}
		if got.Month() != 12 || got.Day() != 25 {
			t.Fatalf("expected Dec 25, got %v %d", got.Month(), got.Day())
		}
	})

	t.Run("month_name_full", func(t *testing.T) {
		got, err := ParseStrptime("%d %B %Y", "01 January 2024")
		if err != nil {
			t.Fatal(err)
		}
		if got.Month() != 1 {
			t.Fatalf("expected January, got %v", got.Month())
		}
	})

	t.Run("literal_percent", func(t *testing.T) {
		got, err := ParseStrptime("100%% done %Y", "100% done 2024")
		if err != nil {
			t.Fatal(err)
		}
		if got.Year() != 2024 {
			t.Fatalf("expected year 2024, got %d", got.Year())
		}
	})

	t.Run("whitespace_n", func(t *testing.T) {
		got, err := ParseStrptime("%Y%n%m%n%d", "2024 03 15")
		if err != nil {
			t.Fatal(err)
		}
		want := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("no_pad_modifier", func(t *testing.T) {
		got, err := ParseStrptime("%Y-%-m-%-d", "2024-3-5")
		if err != nil {
			t.Fatal(err)
		}
		if got.Month() != 3 || got.Day() != 5 {
			t.Fatalf("expected March 5, got %v %d", got.Month(), got.Day())
		}
	})

	// ── Go time.Parse format (auto-detect) ───────────────────────────

	t.Run("go_layout_rfc3339", func(t *testing.T) {
		got, err := ParseStrptime("2006-01-02T15:04:05Z07:00", "2024-03-15T10:30:00+00:00")
		if err != nil {
			t.Fatal(err)
		}
		want := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	// ── Error paths ──────────────────────────────────────────────────

	t.Run("error_bad_digits", func(t *testing.T) {
		_, err := ParseStrptime("%Y-%m-%d", "abcd-01-01")
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("error_month_out_of_range", func(t *testing.T) {
		_, err := ParseStrptime("%Y-%m-%d", "2024-13-01")
		if err == nil {
			t.Fatal("expected error for month 13")
		}
	})

	t.Run("error_day_out_of_range", func(t *testing.T) {
		_, err := ParseStrptime("%Y-%m-%d", "2024-01-32")
		if err == nil {
			t.Fatal("expected error for day 32")
		}
	})

	t.Run("error_hour_out_of_range", func(t *testing.T) {
		_, err := ParseStrptime("%H", "25")
		if err == nil {
			t.Fatal("expected error for hour 25")
		}
	})

	t.Run("error_extra_input", func(t *testing.T) {
		_, err := ParseStrptime("%Y", "2024extra")
		if err == nil {
			t.Fatal("expected error for extra input")
		}
	})

	t.Run("error_trailing_percent", func(t *testing.T) {
		_, err := ParseStrptime("%Y-%", "2024-")
		if err == nil {
			t.Fatal("expected error for trailing %")
		}
	})

	t.Run("error_unsupported_specifier", func(t *testing.T) {
		_, err := ParseStrptime("%Q", "x")
		if err == nil {
			t.Fatal("expected error for unsupported %Q")
		}
	})

	t.Run("error_literal_mismatch", func(t *testing.T) {
		_, err := ParseStrptime("%Y/%m/%d", "2024-01-01")
		if err == nil {
			t.Fatal("expected error for literal mismatch")
		}
	})

	t.Run("error_unexpected_eof", func(t *testing.T) {
		_, err := ParseStrptime("%Y-%m-%d", "2024-01")
		if err == nil {
			t.Fatal("expected error for short input")
		}
	})

	t.Run("error_bad_ampm", func(t *testing.T) {
		_, err := ParseStrptime("%I %p", "09 XX")
		if err == nil {
			t.Fatal("expected error for bad AM/PM")
		}
	})

	t.Run("error_bad_month_name", func(t *testing.T) {
		_, err := ParseStrptime("%d-%b-%Y", "01-Xyz-2024")
		if err == nil {
			t.Fatal("expected error for bad month name")
		}
	})
}

func TestReadDigits(t *testing.T) {
	tests := []struct {
		name      string
		s         string
		pos       int
		minD      int
		maxD      int
		wantN     int
		wantAdv   int
		wantError bool
	}{
		{"four_digits", "2024", 0, 4, 4, 2024, 4, false},
		{"two_of_four", "2024", 0, 2, 2, 20, 2, false},
		{"min_one", "3x", 0, 1, 2, 3, 1, false},
		{"min_two_fail", "3x", 0, 2, 2, 0, 0, true},
		{"empty", "", 0, 1, 2, 0, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n, adv, err := readDigits(tc.s, tc.pos, tc.minD, tc.maxD)
			if tc.wantError {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if n != tc.wantN {
				t.Fatalf("expected n=%d, got %d", tc.wantN, n)
			}
			if adv != tc.wantAdv {
				t.Fatalf("expected adv=%d, got %d", tc.wantAdv, adv)
			}
		})
	}
}

func TestParseMonthName(t *testing.T) {
	tests := []struct {
		name string
		want time.Month
		ok   bool
	}{
		{"Jan", time.January, true},
		{"jan", time.January, true},
		{"january", time.January, true},
		{"FEBRUARY", time.February, true},
		{"Feb", time.February, true},
		{"December", time.December, true},
		{"dec", time.December, true},
		{"xyz", 0, false},
		{"", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := parseMonthName(tc.name)
			if ok != tc.ok {
				t.Fatalf("expected ok=%v, got ok=%v", tc.ok, ok)
			}
			if ok && m != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, m)
			}
		})
	}
}
