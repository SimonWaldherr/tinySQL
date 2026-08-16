package engine

import "testing"

func TestBlobComparisonSemantics(t *testing.T) {
	left := []byte{0x00, 0xff, 0x10}
	if !rawEqual(left, []byte{0x00, 0xff, 0x10}) {
		t.Fatal("equal BLOBs did not compare equal")
	}
	if rawEqual(left, []byte{0x00, 0xff, 0x11}) {
		t.Fatal("different BLOBs compared equal")
	}
	if rawEqual(left, "\x00\xff\x10") {
		t.Fatal("BLOB unexpectedly compared equal to text")
	}

	if cmp, err := compare(left, []byte{0x00, 0xff, 0x10}); err != nil || cmp != 0 {
		t.Fatalf("equal BLOB compare = %d, %v; want 0, nil", cmp, err)
	}
	if cmp, err := compare([]byte{0x01}, []byte{0x02}); err != nil || cmp >= 0 {
		t.Fatalf("BLOB ordering compare = %d, %v; want negative, nil", cmp, err)
	}
	if _, err := compare(left, "text"); err == nil {
		t.Fatal("BLOB/text comparison unexpectedly succeeded")
	}
}
