package auth

import (
	"strings"
	"testing"
)

func TestIssueVerifyRoundTrip(t *testing.T) {
	a := New("secret", "admin")
	tok, err := a.Issue("alice")
	if err != nil {
		t.Fatal(err)
	}
	user, err := a.Verify(tok)
	if err != nil || user != "alice" {
		t.Fatalf("user=%q err=%v", user, err)
	}
}

func TestTamperedTokenRejected(t *testing.T) {
	a := New("secret", "admin")
	alice, _ := a.Issue("alice")
	bob, _ := a.Issue("bob")
	// Swap bob's user id onto alice's signature.
	forged := strings.SplitN(bob, ".", 2)[0] + "." + strings.SplitN(alice, ".", 2)[1]
	if _, err := a.Verify(forged); err == nil {
		t.Fatal("forged token accepted")
	}
	if _, err := New("other-secret", "admin").Verify(alice); err == nil {
		t.Fatal("token from another secret accepted")
	}
	for _, bad := range []string{"", ".", "abc", "YWxpY2U.", ".sig", alice + "x"} {
		if _, err := a.Verify(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}

func TestAdminTokenIsExact(t *testing.T) {
	a := New("secret", "admin-token")
	if !a.IsAdmin("admin-token") || a.IsAdmin("admin-token2") || a.IsAdmin("") {
		t.Fatal("admin check wrong")
	}
	if _, ok := BearerToken("Basic xyz"); ok {
		t.Fatal("non-bearer accepted")
	}
	if tok, ok := BearerToken("bearer  abc "); !ok || tok != "abc" {
		t.Fatalf("tok=%q ok=%v", tok, ok)
	}
}
