package uuid

import (
	"testing"
)

func BenchmarkCreateNew(b *testing.B) {
	for n := 0; n < b.N; n++ {
		NewRandom()
	}
}

func BenchmarkParse(b *testing.B) {
	uuid := NewRandom()
	testUUID := uuid.String()
	for n := 0; n < b.N; n++ {
		Parse(testUUID)
	}
}

func BenchmarkComparison(b *testing.B) {
	uuid := NewRandom()
	emptyUUID, _ := Parse("00000000-0000-0000-0000-000000000000")
	for n := 0; n < b.N; n++ {
		if uuid == Nil {
		}
		if emptyUUID == Nil {
		}
	}
}
