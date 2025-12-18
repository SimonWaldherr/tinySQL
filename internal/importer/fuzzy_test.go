package importer

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestApplyFuzzyDefaultsAndCleanInput(t *testing.T) {
	opts := &FuzzyImportOptions{}
	applyFuzzyDefaults(opts)
	if opts.MaxSkippedRows == 0 || !opts.SkipInvalidRows {
		t.Fatalf("applyFuzzyDefaults did not set defaults")
	}

	// cleanInputData should remove BOM and normalize line endings
	in := strings.NewReader("\ufeffline1\r\nline2\r")
	out, err := cleanInputData(in, opts)
	if err != nil {
		t.Fatalf("cleanInputData error: %v", err)
	}
	if !strings.Contains(out, "line1\nline2") {
		t.Fatalf("cleanInputData unexpected output: %q", out)
	}
}

func TestTryParseAndNormalize(t *testing.T) {
	opts := &FuzzyImportOptions{}
	data := "a,b\n1,2\n3,4,extra\n"
	recs, score := tryParseWithDelimiter(data, ',', opts)
	if score <= 0 || len(recs) == 0 {
		t.Fatalf("tryParseWithDelimiter failed")
	}

	// normalize: expectedCols 2, third record should have merged extra into last column when SkipInvalidRows true
	opts.SkipInvalidRows = true
	norm := normalizeRecords(recs, 2, opts)
	if len(norm) == 0 || len(norm[0]) != 2 {
		t.Fatalf("normalizeRecords failed")
	}
}

func TestFuzzyTypeAndConvert(t *testing.T) {
	opts := &FuzzyImportOptions{ImportOptions: &ImportOptions{NullLiterals: []string{""}}, FuzzyJSON: true}
	// fuzzyDetectType
	if fuzzyDetectType("true", opts) != storage.BoolType {
		t.Fatalf("fuzzyDetectType bool failed")
	}
	if fuzzyDetectType("123", opts) != storage.IntType {
		t.Fatalf("fuzzyDetectType int failed")
	}
	if fuzzyDetectType("3.14", opts) != storage.Float64Type {
		t.Fatalf("fuzzyDetectType float failed")
	}
	// Single-quoted JSON is not valid JSON; fuzzyDetectType will not fix it here
	if fuzzyDetectType("{'a':1}", opts) != storage.TextType {
		t.Fatalf("fuzzyDetectType json failed")
	}

	// fuzzyConvertValue int
	if v, err := fuzzyConvertValue("42", storage.IntType, opts); err != nil || v.(int) != 42 {
		t.Fatalf("fuzzyConvertValue int failed: %v %v", v, err)
	}
	// float
	if v, err := fuzzyConvertValue("3.5", storage.Float64Type, opts); err != nil || v.(float64) != 3.5 {
		t.Fatalf("fuzzyConvertValue float failed: %v %v", v, err)
	}
	// bool
	if v, err := fuzzyConvertValue("yes", storage.BoolType, opts); err != nil || v.(bool) != true {
		t.Fatalf("fuzzyConvertValue bool failed: %v %v", v, err)
	}
	// json (fixCommonJSONIssues should convert single quotes)
	if v, err := fuzzyConvertValue("{'x':1}", storage.JsonType, opts); err != nil || v == nil {
		t.Fatalf("fuzzyConvertValue json failed: %v %v", v, err)
	}
}

func TestRemoveNonNumericAndFixJSON(t *testing.T) {
	if removeNonNumeric("$1,234.56", true) != "1.23456" {
		t.Fatalf("removeNonNumeric failed: %q", removeNonNumeric("$1,234.56", true))
	}
	fixed := fixCommonJSONIssues("{a: 'b'}")
	if !strings.Contains(fixed, `"a"`) || strings.Contains(fixed, "'") {
		t.Fatalf("fixCommonJSONIssues failed: %s", fixed)
	}
}

func TestParseLineDelimitedJSON(t *testing.T) {
	opts := &FuzzyImportOptions{ImportOptions: &ImportOptions{}, FuzzyJSON: true, SkipInvalidRows: true}
	data := "{\"id\":1}\n{invalid}\n{\"id\":2}\n"
	recs, err := parseLineDelimitedJSON(data, opts)
	if err != nil {
		t.Fatalf("parseLineDelimitedJSON error: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 parsed records, got %d", len(recs))
	}
}
