// Package auth issues and verifies bearer tokens.
//
// Tokens are a stand-in for an identity provider: `<base64url(user_id)>.<base64url(hmac-sha256)>`.
// The service never trusts a user_id from a request body; identity is only ever
// derived from a verified token.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"regexp"
	"strings"
)

var ErrInvalidToken = errors.New("invalid token")

var userIDPattern = regexp.MustCompile(`^[A-Za-z0-9._@-]{1,64}$`)

type Authenticator struct {
	secret     []byte
	adminToken []byte
}

func New(secret, adminToken string) *Authenticator {
	return &Authenticator{secret: []byte(secret), adminToken: []byte(adminToken)}
}

func ValidUserID(id string) bool { return userIDPattern.MatchString(id) }

func (a *Authenticator) Issue(userID string) (string, error) {
	if !ValidUserID(userID) {
		return "", errors.New("user_id must match " + userIDPattern.String())
	}
	enc := base64.RawURLEncoding
	return enc.EncodeToString([]byte(userID)) + "." + enc.EncodeToString(a.sign(userID)), nil
}

// Verify returns the user id embedded in a token after checking its signature.
func (a *Authenticator) Verify(token string) (string, error) {
	i := strings.IndexByte(token, '.')
	if i <= 0 || i == len(token)-1 {
		return "", ErrInvalidToken
	}
	enc := base64.RawURLEncoding
	idBytes, err := enc.DecodeString(token[:i])
	if err != nil {
		return "", ErrInvalidToken
	}
	sig, err := enc.DecodeString(token[i+1:])
	if err != nil {
		return "", ErrInvalidToken
	}
	userID := string(idBytes)
	if !ValidUserID(userID) || !hmac.Equal(sig, a.sign(userID)) {
		return "", ErrInvalidToken
	}
	return userID, nil
}

func (a *Authenticator) IsAdmin(token string) bool {
	return subtle.ConstantTimeCompare([]byte(token), a.adminToken) == 1
}

func (a *Authenticator) sign(userID string) []byte {
	m := hmac.New(sha256.New, a.secret)
	m.Write([]byte(userID))
	return m.Sum(nil)
}

// BearerToken extracts the token from an Authorization header value.
func BearerToken(header string) (string, bool) {
	const prefix = "bearer "
	if len(header) > len(prefix) && strings.EqualFold(header[:len(prefix)], prefix) {
		return strings.TrimSpace(header[len(prefix):]), true
	}
	return "", false
}
