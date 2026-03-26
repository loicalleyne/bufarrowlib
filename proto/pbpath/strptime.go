package pbpath

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseStrptime parses a date/time string using the given format.
//
// Format auto-detection:
//   - If the format contains a '%' character, it is interpreted as a DuckDB
//     strptime format (e.g. "%Y-%m-%d %H:%M:%S").
//   - Otherwise it is treated as a Go [time.Parse] layout (e.g.
//     "2006-01-02 15:04:05").
//
// Supported DuckDB format specifiers:
//
//	%Y  — 4-digit year          %y  — 2-digit year (00–99 → 2000–2099)
//	%m  — month (01–12)         %-m — month without leading zero
//	%d  — day (01–31)           %-d — day without leading zero
//	%H  — hour 24h (00–23)     %-H — hour without leading zero
//	%I  — hour 12h (01–12)     %-I — hour 12h without leading zero
//	%M  — minute (00–59)       %-M — minute without leading zero
//	%S  — second (00–59)       %-S — second without leading zero
//	%f  — microseconds (up to 6 digits, zero-padded right)
//	%p  — AM/PM
//	%z  — UTC offset (+HHMM / -HHMM / Z)
//	%Z  — timezone name (parsed but forced to UTC for portability)
//	%j  — day of year (001–366)
//	%a  — abbreviated weekday name (Mon, Tue, …) — consumed but ignored
//	%A  — full weekday name (Monday, Tuesday, …) — consumed but ignored
//	%b  — abbreviated month name (Jan, Feb, …)
//	%B  — full month name (January, February, …)
//	%%  — literal '%'
//	%n  — any whitespace (at least one character)
//	%t  — any whitespace (at least one character; same as %n)
//
// The parser is intentionally simple and lenient: it does not validate
// day-of-week consistency, and %Z is treated as informational (the result
// is always in UTC unless %z provides an offset).
func ParseStrptime(format, value string) (time.Time, error) {
	if strings.ContainsRune(format, '%') {
		return parseDuckDB(format, value)
	}
	return time.Parse(format, value)
}

// parseDuckDB implements the manual DuckDB format scanner.
func parseDuckDB(format, value string) (time.Time, error) {
	// Accumulator fields.
	year := -1
	month := time.Month(0)
	day := -1
	hour := -1
	minute := 0
	second := 0
	microsecond := 0
	ampm := 0 // 0=unset, 1=AM, 2=PM
	hour12 := false
	yday := -1 // day of year

	var loc *time.Location // set by %z

	fi := 0 // format index
	vi := 0 // value index

	for fi < len(format) {
		if format[fi] != '%' {
			// Literal character — must match.
			if vi >= len(value) {
				return time.Time{}, fmt.Errorf("strptime: unexpected end of input at position %d", vi)
			}
			if format[fi] != value[vi] {
				return time.Time{}, fmt.Errorf("strptime: expected literal %q at position %d, got %q", format[fi], vi, value[vi])
			}
			fi++
			vi++
			continue
		}

		// '%' specifier.
		fi++ // skip '%'
		if fi >= len(format) {
			return time.Time{}, fmt.Errorf("strptime: trailing '%%' in format")
		}

		// Check for '-' modifier (suppress leading zero).
		noPad := false
		if format[fi] == '-' {
			noPad = true
			fi++
			if fi >= len(format) {
				return time.Time{}, fmt.Errorf("strptime: trailing '%%-' in format")
			}
		}

		spec := format[fi]
		fi++

		switch spec {
		case 'Y': // 4-digit year
			n, adv, err := readDigits(value, vi, 4, 4)
			if err != nil {
				return time.Time{}, fmt.Errorf("strptime %%Y: %w", err)
			}
			year = n
			vi += adv

		case 'y': // 2-digit year → 2000+
			n, adv, err := readDigits(value, vi, 2, 2)
			if err != nil {
				return time.Time{}, fmt.Errorf("strptime %%y: %w", err)
			}
			year = 2000 + n
			vi += adv

		case 'm': // month 01–12
			minD := 2
			if noPad {
				minD = 1
			}
			n, adv, err := readDigits(value, vi, minD, 2)
			if err != nil {
				return time.Time{}, fmt.Errorf("strptime %%m: %w", err)
			}
			if n < 1 || n > 12 {
				return time.Time{}, fmt.Errorf("strptime %%m: month %d out of range", n)
			}
			month = time.Month(n)
			vi += adv

		case 'd': // day 01–31
			minD := 2
			if noPad {
				minD = 1
			}
			n, adv, err := readDigits(value, vi, minD, 2)
			if err != nil {
				return time.Time{}, fmt.Errorf("strptime %%d: %w", err)
			}
			if n < 1 || n > 31 {
				return time.Time{}, fmt.Errorf("strptime %%d: day %d out of range", n)
			}
			day = n
			vi += adv

		case 'H': // hour 00–23
			minD := 2
			if noPad {
				minD = 1
			}
			n, adv, err := readDigits(value, vi, minD, 2)
			if err != nil {
				return time.Time{}, fmt.Errorf("strptime %%H: %w", err)
			}
			if n > 23 {
				return time.Time{}, fmt.Errorf("strptime %%H: hour %d out of range", n)
			}
			hour = n
			vi += adv

		case 'I': // hour 01–12 (12h)
			minD := 2
			if noPad {
				minD = 1
			}
			n, adv, err := readDigits(value, vi, minD, 2)
			if err != nil {
				return time.Time{}, fmt.Errorf("strptime %%I: %w", err)
			}
			if n < 1 || n > 12 {
				return time.Time{}, fmt.Errorf("strptime %%I: hour %d out of range", n)
			}
			hour = n
			hour12 = true
			vi += adv

		case 'M': // minute 00–59
			minD := 2
			if noPad {
				minD = 1
			}
			n, adv, err := readDigits(value, vi, minD, 2)
			if err != nil {
				return time.Time{}, fmt.Errorf("strptime %%M: %w", err)
			}
			if n > 59 {
				return time.Time{}, fmt.Errorf("strptime %%M: minute %d out of range", n)
			}
			minute = n
			vi += adv

		case 'S': // second 00–59
			minD := 2
			if noPad {
				minD = 1
			}
			n, adv, err := readDigits(value, vi, minD, 2)
			if err != nil {
				return time.Time{}, fmt.Errorf("strptime %%S: %w", err)
			}
			if n > 59 {
				return time.Time{}, fmt.Errorf("strptime %%S: second %d out of range", n)
			}
			second = n
			vi += adv

		case 'f': // microseconds (up to 6 digits, right-padded with zeros)
			start := vi
			for vi < len(value) && vi-start < 6 && value[vi] >= '0' && value[vi] <= '9' {
				vi++
			}
			digits := value[start:vi]
			if len(digits) == 0 {
				return time.Time{}, fmt.Errorf("strptime %%f: expected digits at position %d", start)
			}
			// Right-pad to 6 digits.
			for len(digits) < 6 {
				digits += "0"
			}
			n, _ := strconv.Atoi(digits)
			microsecond = n

		case 'p': // AM/PM
			upper := strings.ToUpper(value[vi:])
			if strings.HasPrefix(upper, "AM") {
				ampm = 1
				vi += 2
			} else if strings.HasPrefix(upper, "PM") {
				ampm = 2
				vi += 2
			} else {
				return time.Time{}, fmt.Errorf("strptime %%p: expected AM/PM at position %d", vi)
			}

		case 'z': // UTC offset +HHMM / -HHMM / Z
			if vi >= len(value) {
				return time.Time{}, fmt.Errorf("strptime %%z: unexpected end of input")
			}
			if value[vi] == 'Z' || value[vi] == 'z' {
				loc = time.UTC
				vi++
			} else if value[vi] == '+' || value[vi] == '-' {
				sign := 1
				if value[vi] == '-' {
					sign = -1
				}
				vi++
				// Read HHMM or HH:MM
				hh, adv, err := readDigits(value, vi, 2, 2)
				if err != nil {
					return time.Time{}, fmt.Errorf("strptime %%z: %w", err)
				}
				vi += adv
				// Optional colon separator
				if vi < len(value) && value[vi] == ':' {
					vi++
				}
				mm := 0
				if vi < len(value) && value[vi] >= '0' && value[vi] <= '9' {
					mm, adv, err = readDigits(value, vi, 2, 2)
					if err != nil {
						return time.Time{}, fmt.Errorf("strptime %%z: %w", err)
					}
					vi += adv
				}
				offset := sign * (hh*3600 + mm*60)
				loc = time.FixedZone("", offset)
			} else {
				return time.Time{}, fmt.Errorf("strptime %%z: expected offset at position %d", vi)
			}

		case 'Z': // timezone name — consume alphabetic characters, ignore
			start := vi
			for vi < len(value) && ((value[vi] >= 'A' && value[vi] <= 'Z') || (value[vi] >= 'a' && value[vi] <= 'z') || value[vi] == '/' || value[vi] == '_') {
				vi++
			}
			if vi == start {
				return time.Time{}, fmt.Errorf("strptime %%Z: expected timezone name at position %d", vi)
			}
			// Best-effort: try to load the timezone. If it works, use it.
			tzName := value[start:vi]
			if tz, err := time.LoadLocation(tzName); err == nil {
				loc = tz
			}
			// Otherwise silently default to UTC.

		case 'j': // day of year 001–366
			n, adv, err := readDigits(value, vi, 3, 3)
			if err != nil {
				return time.Time{}, fmt.Errorf("strptime %%j: %w", err)
			}
			if n < 1 || n > 366 {
				return time.Time{}, fmt.Errorf("strptime %%j: day of year %d out of range", n)
			}
			yday = n
			vi += adv

		case 'a': // abbreviated weekday — consume and ignore
			_, adv := readAlpha(value, vi)
			if adv < 2 {
				return time.Time{}, fmt.Errorf("strptime %%a: expected weekday name at position %d", vi)
			}
			vi += adv

		case 'A': // full weekday — consume and ignore
			_, adv := readAlpha(value, vi)
			if adv < 3 {
				return time.Time{}, fmt.Errorf("strptime %%A: expected weekday name at position %d", vi)
			}
			vi += adv

		case 'b': // abbreviated month name
			name, adv := readAlpha(value, vi)
			if adv < 3 {
				return time.Time{}, fmt.Errorf("strptime %%b: expected month name at position %d", vi)
			}
			m, ok := parseMonthName(name)
			if !ok {
				return time.Time{}, fmt.Errorf("strptime %%b: unrecognized month %q", name)
			}
			month = m
			vi += adv

		case 'B': // full month name
			name, adv := readAlpha(value, vi)
			if adv < 3 {
				return time.Time{}, fmt.Errorf("strptime %%B: expected month name at position %d", vi)
			}
			m, ok := parseMonthName(name)
			if !ok {
				return time.Time{}, fmt.Errorf("strptime %%B: unrecognized month %q", name)
			}
			month = m
			vi += adv

		case '%': // literal '%'
			if vi >= len(value) || value[vi] != '%' {
				return time.Time{}, fmt.Errorf("strptime %%%%: expected '%%' at position %d", vi)
			}
			vi++

		case 'n', 't': // whitespace
			if vi >= len(value) || (value[vi] != ' ' && value[vi] != '\t' && value[vi] != '\n' && value[vi] != '\r') {
				return time.Time{}, fmt.Errorf("strptime %%%c: expected whitespace at position %d", spec, vi)
			}
			for vi < len(value) && (value[vi] == ' ' || value[vi] == '\t' || value[vi] == '\n' || value[vi] == '\r') {
				vi++
			}

		default:
			return time.Time{}, fmt.Errorf("strptime: unsupported specifier %%%c", spec)
		}
	}

	// Check for trailing input.
	if vi < len(value) {
		return time.Time{}, fmt.Errorf("strptime: extra input %q after format consumed", value[vi:])
	}

	// Assemble the time.
	if year < 0 {
		year = 0
	}
	if month == 0 {
		month = time.January
	}
	if day < 0 {
		day = 1
	}
	if hour < 0 {
		hour = 0
	}

	// Apply 12h→24h conversion.
	if hour12 {
		if ampm == 2 && hour != 12 {
			hour += 12
		} else if ampm == 1 && hour == 12 {
			hour = 0
		}
	}

	if loc == nil {
		loc = time.UTC
	}

	// If day-of-year was specified and month/day were not explicitly set,
	// use yday to compute month and day.
	if yday > 0 && month == time.January && day == 1 {
		t := time.Date(year, time.January, yday, hour, minute, second, microsecond*1000, loc)
		return t, nil
	}

	t := time.Date(year, month, day, hour, minute, second, microsecond*1000, loc)
	return t, nil
}

// readDigits reads between minDigits and maxDigits decimal digits from s
// starting at position pos. Returns the parsed number and the count of
// characters consumed.
func readDigits(s string, pos, minDigits, maxDigits int) (int, int, error) {
	start := pos
	for pos < len(s) && pos-start < maxDigits && s[pos] >= '0' && s[pos] <= '9' {
		pos++
	}
	count := pos - start
	if count < minDigits {
		return 0, 0, fmt.Errorf("expected at least %d digits at position %d, got %d", minDigits, start, count)
	}
	n, _ := strconv.Atoi(s[start:pos])
	return n, count, nil
}

// readAlpha reads consecutive alphabetic characters from s starting at pos.
// Returns the consumed string and its length.
func readAlpha(s string, pos int) (string, int) {
	start := pos
	for pos < len(s) && ((s[pos] >= 'A' && s[pos] <= 'Z') || (s[pos] >= 'a' && s[pos] <= 'z')) {
		pos++
	}
	return s[start:pos], pos - start
}

// parseMonthName matches a month name (abbreviated or full, case-insensitive).
func parseMonthName(name string) (time.Month, bool) {
	lower := strings.ToLower(name)
	for m := time.January; m <= time.December; m++ {
		full := strings.ToLower(m.String())
		abbr := full[:3]
		if lower == abbr || lower == full {
			return m, true
		}
	}
	return 0, false
}
