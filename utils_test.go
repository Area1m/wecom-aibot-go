package wecomaibot

import "testing"

func TestGenerateRandomString(t *testing.T) {
	if got := GenerateRandomString(8); len(got) != 8 {
		t.Fatalf("长度应为 8, got %d", len(got))
	}
	if got := GenerateRandomString(0); len(got) != 8 {
		t.Fatalf("非法长度应回落默认 8, got %d", len(got))
	}
	for _, r := range GenerateRandomString(16) {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			t.Fatalf("含非 hex 字符: %q", r)
		}
	}
}

func TestGenerateRandomStringOddAndNegative(t *testing.T) {
	if got := GenerateRandomString(-1); len(got) != 8 {
		t.Errorf("负长度应回落默认 8, got %d", len(got))
	}
	if got := GenerateRandomString(5); len(got) != 5 {
		t.Errorf("奇长度应截断为 5, got %d", len(got))
	}
}
