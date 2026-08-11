package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

const defaultMigrationTablePrefix = "x_"

// MigrationOptions controls an offline SQLite-to-PostgreSQL copy. The target
// is intentionally a *gorm.DB so the command can validate the same logic with
// a disposable SQLite target in unit tests.
type MigrationOptions struct {
	TablePrefix  string
	TableNames   []string
	BatchSize    int
	SampleSize   int
	DryRun       bool
	ValidateOnly bool
}

type MigrationTableReport struct {
	Table              string           `json:"table"`
	SourceRows         int64            `json:"source_rows"`
	TargetRows         int64            `json:"target_rows"`
	CopiedRows         int64            `json:"copied_rows"`
	SourceMinID        string           `json:"source_min_id"`
	TargetMinID        string           `json:"target_min_id"`
	SourceMaxID        string           `json:"source_max_id"`
	TargetMaxID        string           `json:"target_max_id"`
	SourceSampleHash   string           `json:"source_sample_hash"`
	TargetSampleHash   string           `json:"target_sample_hash"`
	SourceStatusCounts map[string]int64 `json:"source_status_counts,omitempty"`
	TargetStatusCounts map[string]int64 `json:"target_status_counts,omitempty"`
	SourceDateBuckets  map[string]int64 `json:"source_date_buckets,omitempty"`
	TargetDateBuckets  map[string]int64 `json:"target_date_buckets,omitempty"`
}

type MigrationReport struct {
	Tables []MigrationTableReport `json:"tables"`
}

type MigrationTableSpec struct {
	Table          string
	PrimaryColumn  string
	OrderColumns   []string
	PrimaryNumeric bool
	StatusColumn   string
	DateColumn     string
	NewModel       func() any
}

func MigrationTableSpecs(tablePrefix string) ([]MigrationTableSpec, error) {
	return migrationTableSpecs(tablePrefix)
}

func MigrationModels(tablePrefix string, tableNames []string) ([]any, error) {
	specs, err := selectedMigrationTableSpecs(tablePrefix, tableNames)
	if err != nil {
		return nil, err
	}
	models := make([]any, 0, len(specs))
	for _, spec := range specs {
		models = append(models, spec.NewModel())
	}
	return models, nil
}

func MigrateSQLiteToPostgres(ctx context.Context, source, target *gorm.DB, options MigrationOptions) (MigrationReport, error) {
	if source == nil || target == nil {
		return MigrationReport{}, fmt.Errorf("source and target databases are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	options = normalizeMigrationOptions(options)
	if err := source.WithContext(ctx).Exec("SELECT 1").Error; err != nil {
		return MigrationReport{}, fmt.Errorf("check source database: %w", err)
	}
	if err := target.WithContext(ctx).Exec("SELECT 1").Error; err != nil {
		return MigrationReport{}, fmt.Errorf("check target database: %w", err)
	}
	specs, err := selectedMigrationTableSpecs(options.TablePrefix, options.TableNames)
	if err != nil {
		return MigrationReport{}, err
	}
	if !options.DryRun && !options.ValidateOnly {
		models := make([]any, 0, len(specs))
		for _, spec := range specs {
			models = append(models, spec.NewModel())
		}
		if err := target.AutoMigrate(models...); err != nil {
			return MigrationReport{}, fmt.Errorf("prepare target schema: %w", err)
		}
	}

	report := MigrationReport{Tables: make([]MigrationTableReport, 0, len(specs))}
	for _, spec := range specs {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		tableReport, err := migrateTable(ctx, source, target, spec, options)
		if err != nil {
			return report, err
		}
		report.Tables = append(report.Tables, tableReport)
	}
	if !options.DryRun && !options.ValidateOnly {
		if err := resetPostgreSQLSequences(ctx, target, specs); err != nil {
			return report, err
		}
	}
	return report, nil
}

func normalizeMigrationOptions(options MigrationOptions) MigrationOptions {
	if options.TablePrefix == "" {
		options.TablePrefix = defaultMigrationTablePrefix
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 500
	}
	if options.SampleSize <= 0 {
		options.SampleSize = 100
	}
	return options
}

func migrateTable(ctx context.Context, source, target *gorm.DB, spec MigrationTableSpec, options MigrationOptions) (MigrationTableReport, error) {
	sourceReport, err := inspectMigrationTable(ctx, source, spec, options.SampleSize)
	if err != nil {
		return MigrationTableReport{}, fmt.Errorf("inspect source table %s: %w", spec.Table, err)
	}
	if options.DryRun {
		return sourceReport, nil
	}
	if !options.ValidateOnly {
		copied, err := copyMigrationTable(ctx, source, target, spec, options.BatchSize)
		if err != nil {
			return sourceReport, fmt.Errorf("copy table %s: %w", spec.Table, err)
		}
		sourceReport.CopiedRows = copied
	}
	targetReport, err := inspectMigrationTable(ctx, target, spec, options.SampleSize)
	if err != nil {
		return sourceReport, fmt.Errorf("inspect target table %s: %w", spec.Table, err)
	}
	sourceReport.TargetRows = targetReport.SourceRows
	sourceReport.TargetMinID = targetReport.SourceMinID
	sourceReport.TargetMaxID = targetReport.SourceMaxID
	sourceReport.TargetSampleHash = targetReport.SourceSampleHash
	sourceReport.TargetStatusCounts = targetReport.SourceStatusCounts
	sourceReport.TargetDateBuckets = targetReport.SourceDateBuckets
	if sourceReport.SourceRows != sourceReport.TargetRows ||
		sourceReport.SourceMinID != sourceReport.TargetMinID ||
		sourceReport.SourceMaxID != sourceReport.TargetMaxID ||
		sourceReport.SourceSampleHash != sourceReport.TargetSampleHash ||
		!reflect.DeepEqual(sourceReport.SourceStatusCounts, sourceReport.TargetStatusCounts) ||
		!reflect.DeepEqual(sourceReport.SourceDateBuckets, sourceReport.TargetDateBuckets) {
		return sourceReport, fmt.Errorf("migration validation failed for %s: source rows=%d target rows=%d source range=%s..%s target range=%s..%s source hash=%s target hash=%s", spec.Table, sourceReport.SourceRows, sourceReport.TargetRows, sourceReport.SourceMinID, sourceReport.SourceMaxID, sourceReport.TargetMinID, sourceReport.TargetMaxID, sourceReport.SourceSampleHash, sourceReport.TargetSampleHash)
	}
	return sourceReport, nil
}

func inspectMigrationTable(ctx context.Context, database *gorm.DB, spec MigrationTableSpec, sampleSize int) (MigrationTableReport, error) {
	var count int64
	if err := database.WithContext(ctx).Table(spec.Table).Count(&count).Error; err != nil {
		return MigrationTableReport{}, err
	}
	minID, maxID, err := migrationTableBounds(ctx, database, spec)
	if err != nil {
		return MigrationTableReport{}, err
	}
	hash, err := migrationSampleHash(ctx, database, spec, sampleSize)
	if err != nil {
		return MigrationTableReport{}, err
	}
	statusCounts, dateBuckets, err := migrationDistribution(ctx, database, spec)
	if err != nil {
		return MigrationTableReport{}, err
	}
	return MigrationTableReport{
		Table:              spec.Table,
		SourceRows:         count,
		SourceMinID:        minID,
		SourceMaxID:        maxID,
		SourceSampleHash:   hash,
		SourceStatusCounts: statusCounts,
		SourceDateBuckets:  dateBuckets,
	}, nil
}

func migrationDistribution(ctx context.Context, database *gorm.DB, spec MigrationTableSpec) (map[string]int64, map[string]int64, error) {
	statusCounts, err := migrationGroupedCounts(ctx, database, spec.Table, spec.StatusColumn, false)
	if err != nil {
		return nil, nil, err
	}
	dateBuckets, err := migrationGroupedCounts(ctx, database, spec.Table, spec.DateColumn, true)
	if err != nil {
		return nil, nil, err
	}
	return statusCounts, dateBuckets, nil
}

func migrationGroupedCounts(ctx context.Context, database *gorm.DB, table, column string, dateBucket bool) (map[string]int64, error) {
	counts := make(map[string]int64)
	if column == "" {
		return counts, nil
	}
	valueExpression := quoteMigrationIdentifier(column)
	if dateBucket {
		valueExpression = "SUBSTR(CAST(" + valueExpression + " AS TEXT), 1, 10)"
	}
	rows, err := database.WithContext(ctx).Raw(
		"SELECT " + valueExpression + ", COUNT(*) FROM " + quoteMigrationIdentifier(table) + " GROUP BY " + valueExpression,
	).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var value any
		var count int64
		if err := rows.Scan(&value, &count); err != nil {
			return nil, err
		}
		counts[canonicalMigrationValue(value)] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return counts, nil
}

func migrationTableBounds(ctx context.Context, database *gorm.DB, spec MigrationTableSpec) (string, string, error) {
	if spec.PrimaryColumn == "" {
		return "", "", nil
	}
	row := database.WithContext(ctx).Raw(
		"SELECT MIN(" + quoteMigrationIdentifier(spec.PrimaryColumn) + "), MAX(" + quoteMigrationIdentifier(spec.PrimaryColumn) + ") FROM " + quoteMigrationIdentifier(spec.Table),
	).Row()
	var minValue, maxValue any
	if err := row.Scan(&minValue, &maxValue); err != nil {
		return "", "", err
	}
	return canonicalMigrationValue(minValue), canonicalMigrationValue(maxValue), nil
}

func migrationSampleHash(ctx context.Context, database *gorm.DB, spec MigrationTableSpec, sampleSize int) (string, error) {
	if sampleSize <= 0 {
		return "", nil
	}
	rows, err := database.WithContext(ctx).Raw(
		"SELECT * FROM " + quoteMigrationIdentifier(spec.Table) + " ORDER BY " + migrationOrderExpression(spec) + " LIMIT " + strconv.Itoa(sampleSize),
	).Rows()
	if err != nil {
		return "", err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return "", err
		}
		for i, value := range values {
			_, _ = fmt.Fprintf(hash, "%s\x00", canonicalMigrationValue(normalizeMigrationColumnValue(spec.Table, columns[i], value)))
		}
		_, _ = hash.Write([]byte{'\n'})
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func copyMigrationTable(ctx context.Context, source, target *gorm.DB, spec MigrationTableSpec, batchSize int) (int64, error) {
	rows, err := source.WithContext(ctx).Raw(
		"SELECT * FROM " + quoteMigrationIdentifier(spec.Table) + " ORDER BY " + migrationOrderExpression(spec),
	).Rows()
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	values := make([]any, 0, batchSize*len(columns))
	rowsInBatch := 0
	var copied int64
	flush := func() error {
		if rowsInBatch == 0 {
			return nil
		}
		count, err := insertMigrationBatch(ctx, target, spec.Table, columns, values, rowsInBatch)
		if err != nil {
			return err
		}
		copied += count
		values = values[:0]
		rowsInBatch = 0
		return nil
	}
	for rows.Next() {
		rowValues := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range rowValues {
			pointers[i] = &rowValues[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return copied, err
		}
		for i, value := range rowValues {
			values = append(values, normalizeMigrationColumnValue(spec.Table, columns[i], value))
		}
		rowsInBatch++
		if rowsInBatch >= batchSize {
			if err := flush(); err != nil {
				return copied, err
			}
		}
		if err := ctx.Err(); err != nil {
			return copied, err
		}
	}
	if err := rows.Err(); err != nil {
		return copied, err
	}
	if err := flush(); err != nil {
		return copied, err
	}
	return copied, nil
}

func insertMigrationBatch(ctx context.Context, target *gorm.DB, table string, columns []string, values []any, rowCount int) (int64, error) {
	var sql strings.Builder
	sql.WriteString("INSERT INTO ")
	sql.WriteString(quoteMigrationIdentifier(table))
	sql.WriteString(" (")
	for i, column := range columns {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(quoteMigrationIdentifier(column))
	}
	sql.WriteString(") VALUES ")
	argCount := len(columns)
	for row := 0; row < rowCount; row++ {
		if row > 0 {
			sql.WriteString(", ")
		}
		sql.WriteByte('(')
		for column := 0; column < argCount; column++ {
			if column > 0 {
				sql.WriteString(", ")
			}
			if target.Dialector.Name() == "postgres" {
				_, _ = fmt.Fprintf(&sql, "$%d", row*argCount+column+1)
			} else {
				sql.WriteByte('?')
			}
		}
		sql.WriteByte(')')
	}
	if target.Dialector.Name() == "postgres" || target.Dialector.Name() == "sqlite" {
		sql.WriteString(" ON CONFLICT DO NOTHING")
	}
	var rowsAffected int64
	err := target.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Exec(sql.String(), values...)
		rowsAffected = result.RowsAffected
		return result.Error
	})
	return rowsAffected, err
}

func resetPostgreSQLSequences(ctx context.Context, target *gorm.DB, specs []MigrationTableSpec) error {
	if target.Dialector.Name() != "postgres" {
		return nil
	}
	for _, spec := range specs {
		if !spec.PrimaryNumeric {
			continue
		}
		var sequence sql.NullString
		if err := target.WithContext(ctx).Raw("SELECT pg_get_serial_sequence(?, ?)", spec.Table, spec.PrimaryColumn).Row().Scan(&sequence); err != nil {
			return fmt.Errorf("find sequence for %s: %w", spec.Table, err)
		}
		if !sequence.Valid || sequence.String == "" {
			continue
		}
		query := "SELECT setval(?::regclass, GREATEST(COALESCE((SELECT MAX(" + quoteMigrationIdentifier(spec.PrimaryColumn) + ") FROM " + quoteMigrationIdentifier(spec.Table) + "), 1), 1), true)"
		if err := target.WithContext(ctx).Exec(query, sequence.String).Error; err != nil {
			return fmt.Errorf("reset sequence for %s: %w", spec.Table, err)
		}
	}
	return nil
}

func canonicalMigrationValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return "<nil>"
	case []byte:
		return string(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	case string:
		for _, layout := range []string{
			time.RFC3339Nano,
			"2006-01-02 15:04:05.999999999-07:00",
			"2006-01-02 15:04:05.999999999",
		} {
			if parsed, err := time.Parse(layout, typed); err == nil {
				return parsed.UTC().Format(time.RFC3339Nano)
			}
		}
		return typed
	default:
		return fmt.Sprint(value)
	}
}

func normalizeMigrationValue(value any) any {
	if bytes, ok := value.([]byte); ok {
		return string(bytes)
	}
	return value
}

func normalizeMigrationColumnValue(table, column string, value any) any {
	if value == nil && strings.EqualFold(strings.TrimSpace(column), "state_version") && strings.HasSuffix(strings.ToLower(strings.TrimSpace(table)), "subscription_items") {
		return int64(0)
	}
	return normalizeMigrationValue(value)
}

func quoteMigrationIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func migrationOrderExpression(spec MigrationTableSpec) string {
	if spec.PrimaryColumn != "" {
		return quoteMigrationIdentifier(spec.PrimaryColumn) + " ASC"
	}
	columns := make([]string, 0, len(spec.OrderColumns))
	for _, column := range spec.OrderColumns {
		columns = append(columns, quoteMigrationIdentifier(column)+" ASC")
	}
	return strings.Join(columns, ", ")
}

func selectedMigrationTableSpecs(tablePrefix string, tableNames []string) ([]MigrationTableSpec, error) {
	specs, err := migrationTableSpecs(tablePrefix)
	if err != nil {
		return nil, err
	}
	if len(tableNames) == 0 {
		return specs, nil
	}
	allowed := make(map[string]struct{}, len(tableNames))
	for _, name := range tableNames {
		allowed[strings.TrimSpace(name)] = struct{}{}
	}
	selected := make([]MigrationTableSpec, 0, len(tableNames))
	for _, spec := range specs {
		if _, ok := allowed[spec.Table]; ok {
			selected = append(selected, spec)
			delete(allowed, spec.Table)
		}
	}
	if len(allowed) > 0 {
		missing := make([]string, 0, len(allowed))
		for name := range allowed {
			missing = append(missing, name)
		}
		return nil, fmt.Errorf("unknown migration tables: %s", strings.Join(missing, ", "))
	}
	return selected, nil
}

func migrationTableSpecs(tablePrefix string) ([]MigrationTableSpec, error) {
	if tablePrefix == "" {
		tablePrefix = defaultMigrationTablePrefix
	}
	factories := []func() any{
		func() any { return new(model.Storage) },
		func() any { return new(model.User) },
		func() any { return new(model.Meta) },
		func() any { return new(model.SettingItem) },
		func() any { return new(model.SearchNode) },
		func() any { return new(model.TaskItem) },
		func() any { return new(model.SSHPublicKey) },
		func() any { return new(model.SharingDB) },
		func() any { return new(model.ETFArchiveRecord) },
		func() any { return new(model.MobileShareRecord) },
		func() any { return new(model.ETFMediaRoot) },
		func() any { return new(model.ETFMediaRootBatch) },
		func() any { return new(model.ETFSubscriptionJob) },
		func() any { return new(model.Subscription) },
		func() any { return new(model.ExternalSubscriptionRequest) },
		func() any { return new(model.SubscriptionItem) },
		func() any { return new(model.SubscriptionEpisodeSource) },
		func() any { return new(model.SubscriptionRun) },
		func() any { return new(model.SubscriptionTelegramEvent) },
		func() any { return new(model.SubscriptionRealtimeCandidate) },
		func() any { return new(model.ClusterNode) },
		func() any { return new(model.ClusterNodeSession) },
		func() any { return new(model.ClusterNodeInventory) },
		func() any { return new(model.ClusterCoordinatorLease) },
		func() any { return new(model.ClusterSecret) },
		func() any { return new(model.ClusterNodeDesiredConfig) },
		func() any { return new(model.ClusterStorageProfile) },
		func() any { return new(model.ClusterControlAudit) },
		func() any { return new(model.ClusterWorkerObservedState) },
		func() any { return new(model.ClusterJob) },
		func() any { return new(model.ClusterJobAttempt) },
		func() any { return new(model.ClusterJobStage) },
		func() any { return new(model.ClusterUploadManifest) },
		func() any { return new(model.ClusterShareInspectManifest) },
		func() any { return new(model.ClusterOutbox) },
		func() any { return new(model.ClusterInbox) },
	}
	cache := &sync.Map{}
	specs := make([]MigrationTableSpec, 0, len(factories))
	for _, factory := range factories {
		modelValue := factory()
		parsed, err := schema.Parse(modelValue, cache, schema.NamingStrategy{TablePrefix: tablePrefix})
		if err != nil {
			return nil, fmt.Errorf("parse migration model %T: %w", modelValue, err)
		}
		primary := parsed.PrioritizedPrimaryField
		if primary == nil && len(parsed.PrimaryFields) > 0 {
			primary = parsed.PrimaryFields[0]
		}
		orderColumns := make([]string, 0, len(parsed.Fields))
		for _, field := range parsed.Fields {
			if field.DBName != "" {
				orderColumns = append(orderColumns, field.DBName)
			}
		}
		primaryColumn := ""
		primaryNumeric := false
		if primary != nil {
			primaryColumn = primary.DBName
			primaryNumeric = primary.DataType == schema.Int || primary.DataType == schema.Uint || primary.DataType == schema.Float
		}
		statusColumn := ""
		dateColumn := ""
		switch modelValue.(type) {
		case *model.SubscriptionTelegramEvent:
			statusColumn, dateColumn = "status", "created_at"
		case *model.ClusterInbox:
			statusColumn, dateColumn = "status", "received_at"
		}
		specs = append(specs, MigrationTableSpec{
			Table:          parsed.Table,
			PrimaryColumn:  primaryColumn,
			OrderColumns:   orderColumns,
			PrimaryNumeric: primaryNumeric,
			StatusColumn:   statusColumn,
			DateColumn:     dateColumn,
			NewModel:       factory,
		})
	}
	return specs, nil
}
