package utils

import (
	"context"
	"fmt"
	"strings"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
)

func GenerateBatchID(cfg any, start, end uint64) (string, error) {
	switch v := cfg.(type) {
	case batch.LocalCSVBatchConfig:
		return fmt.Sprintf("%s/%s-%d-%d", v.Path, v.Name, start, end), nil
	case batch.CloudCSVBatchConfig:
		return fmt.Sprintf("%s-%s/%s-%d-%d", v.Bucket, v.Path, v.Name, start, end), nil
	case batch.LocalCSVMongoBatchConfig:
		return fmt.Sprintf(
			"%s/%s-%d-%d",
			v.LocalCSVBatchConfig.Path,
			v.LocalCSVBatchConfig.Name,
			start,
			end), nil
	case batch.CloudCSVMongoBatchConfig:
		return fmt.Sprintf(
			"%s-%s/%s-%s-%d-%d",
			v.CloudCSVBatchConfig.Bucket,
			v.CloudCSVBatchConfig.Path,
			v.CloudCSVBatchConfig.Name,
			v.Collection,
			start,
			end), nil
	default:
		return "", fmt.Errorf("unknown batch config type: %T", cfg)
	}
}

func GenerateRunID(cfg any) (string, error) {
	switch v := cfg.(type) {
	case batch.LocalCSVBatchConfig:
		return fmt.Sprintf("%s/%s", v.Path, v.Name), nil
	case batch.CloudCSVBatchConfig:
		return fmt.Sprintf("%s-%s/%s", v.Bucket, v.Path, v.Name), nil
	case batch.LocalCSVMongoBatchConfig:
		return fmt.Sprintf("%s/%s", v.LocalCSVBatchConfig.Path, v.LocalCSVBatchConfig.Name), nil
	case batch.CloudCSVMongoBatchConfig:
		return fmt.Sprintf(
			"%s-%s/%s-%s",
			v.CloudCSVBatchConfig.Bucket,
			v.CloudCSVBatchConfig.Path,
			v.CloudCSVBatchConfig.Name,
			v.Collection), nil
	default:
		return "", fmt.Errorf("unknown batch config type: %T", cfg)
	}
}

func ProcessCSVRecordStream(ctx context.Context, recStream <-chan batch.Result) (int, int, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("ProcessCSVRecordStream - error getting logger from context: %w", err)
	}

	var processedCount, errorCount int

	for {
		select {
		case <-ctx.Done():
			l.Info("Context cancelled, stopping processing CSV batch record stream")
			return processedCount, errorCount, ctx.Err()
		case rec, ok := <-recStream:
			if !ok {
				l.Info("Record stream closed")
				return processedCount, errorCount, nil
			}
			if rec.Error != "" {
				l.Debug("Error processing record", "error", rec.Error)
				errorCount++
			} else if rec.Record != nil {
				l.Debug("Processed record", "processedCount", processedCount+1, "record", rec.Record)
				processedCount++
			}
		}
	}
}

// BuildTransformer creates a transformer function based on the provided headers and rules.
// The rules map defines how to transform each header:
// - If a header maps to an empty string, it is ignored.
// - If a header maps to rename rule target, it is renamed to target.
// - If a header maps to a group rule (prefixed with '@'), it is grouped into the target.
// The transformer function takes a slice of string values (CSV record) and returns a map[string]any
// where keys are the transformed header names and values are the corresponding record values.
// Example usage:
//
//	headers := []string{"ENTITY_NUM", "PHYSICAL_CITY", "PHYSICAL_ADDRESS"}
//	rules := map[string]string{
//	  "ENTITY_NUM": "ENTITY_ID",        // rename
//	  "PHYSICAL_CITY": "@ADDRESS",      // group into ADDRESS
//	  "PHYSICAL_ADDRESS": "ADDRESS",    // rename
//	}
//
// The transformer function will then transform a CSV record like:
//
//	["123", "New York", "123 Main St"]
//
// into a map like:
//
//	map[string]any{
//	  "entity_id": "123",
//	  "address": "123 Main St New York",
//	}
//
// allowing for flexible transformations of CSV records based on the provided headers and rules.
func BuildTransformer(headers []string, rules map[string]string) batch.TransformerFunc {
	type colPlan struct {
		target string
		group  bool
	}
	plan := make([]colPlan, len(headers))
	for i, h := range headers {
		if tgt, ok := rules[h]; ok && tgt != "" {
			if strings.HasPrefix(tgt, "@") {
				plan[i] = colPlan{target: tgt[1:], group: true}
			} else {
				plan[i] = colPlan{target: tgt, group: false}
			}
		} else {
			plan[i] = colPlan{target: h, group: false}
		}
	}

	return func(values []string) map[string]any {
		out := make(map[string]any, len(plan))
		groupParts := map[string][]string{}
		n := len(values)

		for i := 0; i < len(plan) && i < n; i++ {
			var val string
			if i < n {
				val = strings.TrimSpace(values[i])
			} else {
				val = ""
			}
			p := plan[i]
			if p.group {
				if val != "" {
					groupParts[p.target] = append(groupParts[p.target], val)
				}
			} else {
				out[strings.ToLower(p.target)] = val
			}
		}

		// Materialize grouped fields
		for target, parts := range groupParts {
			// Skip empty parts, and join with single space
			joined := strings.TrimSpace(strings.Join(parts, " "))
			out[strings.ToLower(target)] = joined
		}

		return out
	}
}

// BuildTransformerWithRules creates a transformer function based on the provided headers and rules.
// The rules map defines how to transform each header:
// - If a header maps to an empty string, it is ignored.
// - If a header maps to rename rule, it is renamed to target.
// - If a header maps to a group rule, it is grouped into the target.
// The transformer function takes a slice of string values (CSV record) and returns a map[string]any
// where keys are the transformed header names and values are the corresponding record values.
// Example usage:
//
//	headers := []string{"ENTITY_NUM", "PHYSICAL_CITY", "PHYSICAL_ADDRESS", "WORK_ADDRESS"}
//	rules := map[string]Rule{
//	  "ENTITY_NUM": {Target: "ENTITY_ID"},        // rename
//	  "PHYSICAL_ADDRESS": {Target: "ADDRESS", Group: true},    // group into ADDRESS
//	  "PHYSICAL_CITY": {Target: "ADDRESS", Group: true},      // group into ADDRESS
//	  "WORK_ADDRESS": {Target: "WORK_ADDR"},      // rename into WORK_ADDR
//	}
//
// The transformer function will then transform a CSV record like:
//
//	["123", "New York", "123 Main St", "7648 Gotham St New York NY"]
//
// into a map like:
//
//	map[string]any{
//	  "entity_id": "123",
//	  "address": "123 Main St New York",
//	  "work_addr": "7648 Gotham St New York NY",
//	}
//
// allowing for flexible transformations of CSV records based on the provided headers and rules.
func BuildTransformerWithRules(headers []string, rules map[string]batch.Rule) batch.TransformerFunc {
	type colPlan struct {
		target   string
		group    bool
		newField string
		source   string
	}
	plan := make([]colPlan, len(headers))
	for i, h := range headers {
		if r, ok := rules[h]; ok && r.Target != "" {
			plan[i] = colPlan{source: h, target: r.Target, group: r.Group, newField: r.NewField}
		} else {
			plan[i] = colPlan{source: h, target: h}
		}
	}

	return func(values []string) map[string]any {
		out := make(map[string]any, len(plan))
		groupParts := map[string][]string{}
		n := len(values)

		for i := 0; i < len(plan) && i < n; i++ {
			var val string
			if i < n {
				val = strings.TrimSpace(values[i])
			} else {
				val = "" // ensure presence for non-grouped keys even if value slice is short
			}

			p := plan[i]
			if p.group {
				// Skip empty parts for grouped targets
				if val != "" {
					groupParts[p.target] = append(groupParts[p.target], val)
				}
			} else {
				// Always set the key, even if val == ""
				if p.newField != "" {
					out[strings.ToLower(p.target)] = p.newField
					out[strings.ToLower(p.source)] = val
				} else {
					out[strings.ToLower(p.target)] = val
				}
			}
		}

		// Materialize grouped fields (join non-empty parts with single spaces)
		for target, parts := range groupParts {
			// Join with single space
			joined := strings.TrimSpace(strings.Join(parts, " "))
			out[strings.ToLower(target)] = joined
		}

		return out
	}
}

func BuildBusinessEntityTransformRules() map[string]string {
	return map[string]string{
		"ENTITY_NUM":                  "ENTITY_ID",             // rename
		"PHYSICAL_ADDRESS":            "ADDRESS",               // rename
		"PHYSICAL_ADDRESS1":           "@ADDRESS",              // group
		"PHYSICAL_ADDRESS2":           "@ADDRESS",              // group
		"PHYSICAL_ADDRESS3":           "@ADDRESS",              // group
		"PHYSICAL_CITY":               "@ADDRESS",              // group
		"PHYSICAL_STATE":              "@ADDRESS",              // group
		"PHYSICAL_POSTAL_CODE":        "@ADDRESS",              // group
		"PHYSICAL_COUNTRY":            "@ADDRESS",              // group
		"ADDRESS1":                    "@ADDRESS",              // group
		"ADDRESS2":                    "@ADDRESS",              // group
		"ADDRESS3":                    "@ADDRESS",              // group
		"CITY":                        "@ADDRESS",              // group
		"STATE":                       "@ADDRESS",              // group
		"POSTAL_CODE":                 "@ADDRESS",              // group
		"COUNTRY":                     "@ADDRESS",              // group
		"PRINCIPAL_ADDRESS":           "@PRINCIPAL_ADDRESS",    // group
		"PRINCIPAL_ADDRESS1":          "@PRINCIPAL_ADDRESS",    // group
		"PRINCIPAL_ADDRESS2":          "@PRINCIPAL_ADDRESS",    // group
		"PRINCIPAL_CITY":              "@PRINCIPAL_ADDRESS",    // group
		"PRINCIPAL_STATE":             "@PRINCIPAL_ADDRESS",    // group
		"PRINCIPAL_POSTAL_CODE":       "@PRINCIPAL_ADDRESS",    // group
		"PRINCIPAL_COUNTRY":           "@PRINCIPAL_ADDRESS",    // group
		"MAILING_ADDRESS":             "@MAILING_ADDRESS",      // group
		"MAILING_ADDRESS1":            "@MAILING_ADDRESS",      // group
		"MAILING_ADDRESS2":            "@MAILING_ADDRESS",      // group
		"MAILING_ADDRESS3":            "@MAILING_ADDRESS",      // group
		"MAILING_CITY":                "@MAILING_ADDRESS",      // group
		"MAILING_STATE":               "@MAILING_ADDRESS",      // group
		"MAILING_POSTAL_CODE":         "@MAILING_ADDRESS",      // group
		"MAILING_COUNTRY":             "@MAILING_ADDRESS",      // group
		"PRINCIPAL_ADDRESS_IN_CA":     "@ADDRESS_IN_CA",        // group
		"PRINCIPAL_ADDRESS1_IN_CA":    "@ADDRESS_IN_CA",        // group
		"PRINCIPAL_ADDRESS2_IN_CA":    "@ADDRESS_IN_CA",        // group
		"PRINCIPAL_CITY_IN_CA":        "@ADDRESS_IN_CA",        // group
		"PRINCIPAL_STATE_IN_CA":       "@ADDRESS_IN_CA",        // group
		"PRINCIPAL_POSTAL_CODE_IN_CA": "@ADDRESS_IN_CA",        // group
		"PRINCIPAL_COUNTRY_IN_CA":     "@ADDRESS_IN_CA",        // group
		"POSITION_TYPE":               "#AGENT_TYPE#Principal", // new Target field AGENT_TYPE with value Principal
	}
}

func BuildBusinessModelTransformRules() map[string]batch.Rule {
	return map[string]batch.Rule{
		"ENTITY_NUM":                  {Target: "ENTITY_ID"},                         // rename to ENTITY_ID
		"PHYSICAL_ADDRESS":            {Target: "ADDRESS"},                           // rename to ADDRESS
		"PHYSICAL_ADDRESS1":           {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"PHYSICAL_ADDRESS2":           {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"PHYSICAL_ADDRESS3":           {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"PHYSICAL_CITY":               {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"PHYSICAL_STATE":              {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"PHYSICAL_POSTAL_CODE":        {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"PHYSICAL_COUNTRY":            {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"ADDRESS1":                    {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"ADDRESS2":                    {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"ADDRESS3":                    {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"CITY":                        {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"STATE":                       {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"POSTAL_CODE":                 {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"COUNTRY":                     {Target: "ADDRESS", Group: true},              // group into ADDRESS
		"PRINCIPAL_ADDRESS":           {Target: "PRINCIPAL_ADDRESS", Group: true},    // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_ADDRESS1":          {Target: "PRINCIPAL_ADDRESS", Group: true},    // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_ADDRESS2":          {Target: "PRINCIPAL_ADDRESS", Group: true},    // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_CITY":              {Target: "PRINCIPAL_ADDRESS", Group: true},    // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_STATE":             {Target: "PRINCIPAL_ADDRESS", Group: true},    // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_POSTAL_CODE":       {Target: "PRINCIPAL_ADDRESS", Group: true},    // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_COUNTRY":           {Target: "PRINCIPAL_ADDRESS", Group: true},    // group into PRINCIPAL_ADDRESS
		"MAILING_ADDRESS":             {Target: "MAILING_ADDRESS", Group: true},      // group into MAILING_ADDRESS
		"MAILING_ADDRESS1":            {Target: "MAILING_ADDRESS", Group: true},      // group into MAILING_ADDRESS
		"MAILING_ADDRESS2":            {Target: "MAILING_ADDRESS", Group: true},      // group into MAILING_ADDRESS
		"MAILING_ADDRESS3":            {Target: "MAILING_ADDRESS", Group: true},      // group into MAILING_ADDRESS
		"MAILING_CITY":                {Target: "MAILING_ADDRESS", Group: true},      // group into MAILING_ADDRESS
		"MAILING_STATE":               {Target: "MAILING_ADDRESS", Group: true},      // group into MAILING_ADDRESS
		"MAILING_POSTAL_CODE":         {Target: "MAILING_ADDRESS", Group: true},      // group into MAILING_ADDRESS
		"MAILING_COUNTRY":             {Target: "MAILING_ADDRESS", Group: true},      // group into MAILING_ADDRESS
		"PRINCIPAL_ADDRESS_IN_CA":     {Target: "ADDRESS_IN_CA", Group: true},        // group into ADDRESS_IN_CA
		"PRINCIPAL_ADDRESS1_IN_CA":    {Target: "ADDRESS_IN_CA", Group: true},        // group into ADDRESS_IN_CA
		"PRINCIPAL_ADDRESS2_IN_CA":    {Target: "ADDRESS_IN_CA", Group: true},        // group into ADDRESS_IN_CA
		"PRINCIPAL_CITY_IN_CA":        {Target: "ADDRESS_IN_CA", Group: true},        // group into ADDRESS_IN_CA
		"PRINCIPAL_STATE_IN_CA":       {Target: "ADDRESS_IN_CA", Group: true},        // group into ADDRESS_IN_CA
		"PRINCIPAL_POSTAL_CODE_IN_CA": {Target: "ADDRESS_IN_CA", Group: true},        // group into ADDRESS_IN_CA
		"PRINCIPAL_COUNTRY_IN_CA":     {Target: "ADDRESS_IN_CA", Group: true},        // group into ADDRESS_IN_CA
		"POSITION_TYPE":               {Target: "AGENT_TYPE", NewField: "Principal"}, // new Target field AGENT_TYPE with value Principal
	}
}
