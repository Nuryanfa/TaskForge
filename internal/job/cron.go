package job

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
)

type Schedule struct {
	ID          string
	Name        string
	Queue       string
	Kind        string
	Payload     json.RawMessage
	PayloadHash []byte
	Priority    int16
	CronExpr    string
	Timezone    string
	Paused      bool
	NextRunAt   time.Time
	LastRunAt   *time.Time
	LastJobID   *string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type ScheduleSubmission struct {
	Name        string
	Queue       string
	Kind        string
	Payload     json.RawMessage
	Priority    int16
	CronExpr    string
	Timezone    string
	Paused      bool
	Cron        CronSchedule
	Location    *time.Location
	Fingerprint [sha256.Size]byte
}

func NewScheduleSubmission(name, queue, kind string, payload json.RawMessage, priority int, cronExpr, timezone string, paused bool, maxPayloadBytes int64) (ScheduleSubmission, error) {
	if !identifierPattern.MatchString(name) {
		return ScheduleSubmission{}, errors.New("name must be 1-64 lowercase letters, digits, dots, underscores, or hyphens")
	}
	if queue == "" {
		queue = DefaultQueue
	}
	if !identifierPattern.MatchString(queue) {
		return ScheduleSubmission{}, errors.New("queue must be 1-64 lowercase letters, digits, dots, underscores, or hyphens")
	}
	if !identifierPattern.MatchString(kind) {
		return ScheduleSubmission{}, errors.New("kind must be a 1-64 character lowercase namespace-style identifier")
	}
	if priority < MinPriority || priority > MaxPriority {
		return ScheduleSubmission{}, fmt.Errorf("priority must be between %d and %d", MinPriority, MaxPriority)
	}
	loc, normalizedTZ, err := ValidateTimezone(timezone)
	if err != nil {
		return ScheduleSubmission{}, err
	}
	parsedCron, normalizedCron, err := ParseCron(cronExpr)
	if err != nil {
		return ScheduleSubmission{}, err
	}
	canonicalPayload, err := canonicalObject(payload, maxPayloadBytes)
	if err != nil {
		return ScheduleSubmission{}, err
	}
	submission := ScheduleSubmission{
		Name:     name,
		Queue:    queue,
		Kind:     kind,
		Payload:  canonicalPayload,
		Priority: int16(priority),
		CronExpr: normalizedCron,
		Timezone: normalizedTZ,
		Paused:   paused,
		Cron:     parsedCron,
		Location: loc,
	}
	fingerprintInput, err := json.Marshal(struct {
		Name     string          `json:"name"`
		Queue    string          `json:"queue"`
		Kind     string          `json:"kind"`
		Payload  json.RawMessage `json:"payload"`
		Priority int16           `json:"priority"`
		CronExpr string          `json:"cron_expr"`
		Timezone string          `json:"timezone"`
	}{
		Name:     submission.Name,
		Queue:    submission.Queue,
		Kind:     submission.Kind,
		Payload:  submission.Payload,
		Priority: submission.Priority,
		CronExpr: submission.CronExpr,
		Timezone: submission.Timezone,
	})
	if err != nil {
		return ScheduleSubmission{}, fmt.Errorf("encode schedule fingerprint: %w", err)
	}
	submission.Fingerprint = sha256.Sum256(fingerprintInput)
	return submission, nil
}

func ValidateTimezone(timezone string) (*time.Location, string, error) {
	if timezone == "" {
		timezone = DefaultTimezone
	}
	if len(timezone) > 64 || strings.EqualFold(timezone, "Local") || strings.Contains(timezone, "..") || strings.HasPrefix(timezone, "/") || strings.Contains(timezone, "\\") {
		return nil, "", errors.New("timezone must be a valid IANA timezone name")
	}
	for _, r := range timezone {
		if r <= 0x20 || r > 0x7e {
			return nil, "", errors.New("timezone must be a valid IANA timezone name")
		}
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, "", errors.New("timezone must be a valid IANA timezone name")
	}
	return loc, timezone, nil
}

type CronSchedule struct {
	minute     uint64
	hour       uint64
	dayOfMonth uint64
	month      uint64
	dayOfWeek  uint64
	domStar    bool
	dowStar    bool
}

func ParseCron(expr string) (CronSchedule, string, error) {
	trimmed := strings.TrimSpace(expr)
	if len(trimmed) < 9 || len(trimmed) > 128 {
		return CronSchedule{}, "", errors.New("cron_expr must be a valid 5-field cron expression")
	}
	fields := strings.Fields(trimmed)
	if len(fields) != 5 {
		return CronSchedule{}, "", errors.New("cron_expr must contain exactly 5 fields (minute hour day-of-month month day-of-week)")
	}
	minute, _, err := parseCronField(fields[0], 0, 59, false)
	if err != nil {
		return CronSchedule{}, "", fmt.Errorf("invalid cron minute field: %w", err)
	}
	hour, _, err := parseCronField(fields[1], 0, 23, false)
	if err != nil {
		return CronSchedule{}, "", fmt.Errorf("invalid cron hour field: %w", err)
	}
	dom, domStar, err := parseCronField(fields[2], 1, 31, false)
	if err != nil {
		return CronSchedule{}, "", fmt.Errorf("invalid cron day-of-month field: %w", err)
	}
	month, _, err := parseCronField(fields[3], 1, 12, false)
	if err != nil {
		return CronSchedule{}, "", fmt.Errorf("invalid cron month field: %w", err)
	}
	dow, dowStar, err := parseCronField(fields[4], 0, 6, true)
	if err != nil {
		return CronSchedule{}, "", fmt.Errorf("invalid cron day-of-week field: %w", err)
	}
	schedule := CronSchedule{
		minute:     minute,
		hour:       hour,
		dayOfMonth: dom,
		month:      month,
		dayOfWeek:  dow,
		domStar:    domStar,
		dowStar:    dowStar,
	}
	probe := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	if _, err := schedule.Next(probe, time.UTC); err != nil {
		return CronSchedule{}, "", errors.New("cron_expr does not match any valid calendar date")
	}
	return schedule, strings.Join(fields, " "), nil
}

func (c CronSchedule) Next(after time.Time, loc *time.Location) (time.Time, error) {
	if loc == nil {
		loc = time.UTC
	}
	current := after.In(loc).Truncate(time.Minute).Add(time.Minute)
	limit := current.AddDate(8, 0, 0)
	const maxSteps = 200000
	for step := 0; step < maxSteps && !current.After(limit); step++ {
		if (c.month & (uint64(1) << uint(current.Month()))) == 0 {
			next := time.Date(current.Year(), current.Month()+1, 1, 0, 0, 0, 0, loc)
			if !next.After(current) {
				next = current.Add(24 * time.Hour).In(loc)
			}
			current = next
			continue
		}
		if !c.matchDay(current) {
			next := time.Date(current.Year(), current.Month(), current.Day()+1, 0, 0, 0, 0, loc)
			if !next.After(current) {
				next = current.Add(time.Hour).In(loc)
			}
			current = next
			continue
		}
		if (c.hour & (uint64(1) << uint(current.Hour()))) == 0 {
			current = current.Add(time.Minute).In(loc)
			continue
		}
		if (c.minute & (uint64(1) << uint(current.Minute()))) == 0 {
			current = current.Add(time.Minute).In(loc)
			continue
		}
		return current.UTC(), nil
	}
	return time.Time{}, errors.New("no matching cron tick within search horizon")
}

func (c CronSchedule) matchDay(t time.Time) bool {
	domMatch := (c.dayOfMonth & (uint64(1) << uint(t.Day()))) != 0
	dowMatch := (c.dayOfWeek & (uint64(1) << uint(t.Weekday()))) != 0
	if !c.domStar && !c.dowStar {
		return domMatch || dowMatch
	}
	return domMatch && dowMatch
}

func parseCronField(field string, minVal, maxVal int, allowSevenSunday bool) (uint64, bool, error) {
	if field == "*" {
		var mask uint64
		for i := minVal; i <= maxVal; i++ {
			mask |= uint64(1) << uint(i)
		}
		return mask, true, nil
	}
	var mask uint64
	parts := strings.Split(field, ",")
	for _, part := range parts {
		if part == "" {
			return 0, false, errors.New("empty list element")
		}
		step := 1
		rangePart := part
		if strings.Contains(part, "/") {
			splitStep := strings.Split(part, "/")
			if len(splitStep) != 2 || splitStep[0] == "" || splitStep[1] == "" {
				return 0, false, errors.New("invalid step syntax")
			}
			parsedStep, err := strconv.Atoi(splitStep[1])
			if err != nil || parsedStep <= 0 || parsedStep > (maxVal-minVal+1) {
				return 0, false, errors.New("step out of bounds")
			}
			step = parsedStep
			rangePart = splitStep[0]
		}
		var start, end int
		if rangePart == "*" {
			start = minVal
			end = maxVal
		} else if strings.Contains(rangePart, "-") {
			bounds := strings.Split(rangePart, "-")
			if len(bounds) != 2 || bounds[0] == "" || bounds[1] == "" {
				return 0, false, errors.New("invalid range syntax")
			}
			var err error
			start, err = parseCronValue(bounds[0], minVal, maxVal, allowSevenSunday)
			if err != nil {
				return 0, false, err
			}
			end, err = parseCronValue(bounds[1], minVal, maxVal, allowSevenSunday)
			if err != nil {
				return 0, false, err
			}
			if allowSevenSunday && bounds[1] == "7" && start > 0 {
				end = 7
			}
			if start > end {
				return 0, false, errors.New("range start exceeds end")
			}
		} else {
			if step != 1 {
				return 0, false, errors.New("step requires '*' or a range")
			}
			val, err := parseCronValue(rangePart, minVal, maxVal, allowSevenSunday)
			if err != nil {
				return 0, false, err
			}
			start = val
			end = val
		}
		for i := start; i <= end; i += step {
			bit := i
			if allowSevenSunday && bit == 7 {
				bit = 0
			}
			mask |= uint64(1) << uint(bit)
		}
	}
	if mask == 0 {
		return 0, false, errors.New("field matched no values")
	}
	return mask, false, nil
}

func parseCronValue(raw string, minVal, maxVal int, allowSevenSunday bool) (int, error) {
	val, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New("non-numeric cron value")
	}
	if allowSevenSunday && val == 7 {
		return 0, nil
	}
	if val < minVal || val > maxVal {
		return 0, fmt.Errorf("value %d outside %d..%d", val, minVal, maxVal)
	}
	return val, nil
}
