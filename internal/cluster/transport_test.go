package cluster

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testSecret = []byte("test-secret-at-least-16-chars")

func TestSignVerify_RoundTrip(t *testing.T) {
	req := LockRequest{Path: "a/b", NodeID: "node1", LockID: "lock-1", LamportTime: 42}
	datagram, err := signMessage(testSecret, TypRequestLock, "00112233aabbccdd", req)
	require.NoError(t, err)

	claims, err := verifyMessage(testSecret, datagram)
	require.NoError(t, err)
	assert.Equal(t, 1, claims.V)
	assert.Equal(t, TypRequestLock, claims.Typ)
	assert.Equal(t, "00112233aabbccdd", claims.RID)

	var got LockRequest
	require.NoError(t, json.Unmarshal(claims.Body, &got))
	assert.Equal(t, req, got)
}

func TestVerify_TamperedSignature(t *testing.T) {
	datagram, err := signMessage(testSecret, TypRequestLock, newRID(), LockRequest{Path: "p"})
	require.NoError(t, err)

	// Flip the first byte of the signature segment. The final base64 character
	// carries padding bits the decoder ignores, so tampering the last byte
	// would not always change the decoded signature.
	tampered := append([]byte(nil), datagram...)
	first := bytes.LastIndexByte(tampered, '.') + 1
	if tampered[first] == 'A' {
		tampered[first] = 'B'
	} else {
		tampered[first] = 'A'
	}

	_, err = verifyMessage(testSecret, tampered)
	assert.ErrorIs(t, err, errBadSignature)
}

func TestVerify_TamperedClaims(t *testing.T) {
	datagram, err := signMessage(testSecret, TypRequestLock, newRID(), LockRequest{Path: "p"})
	require.NoError(t, err)

	parts := bytes.Split(datagram, []byte("."))
	assert.Len(t, parts, 3)

	otherClaims, err := json.Marshal(wireClaims{V: 1, Typ: TypReleaseLock, RID: newRID(), Body: json.RawMessage(`{}`)})
	require.NoError(t, err)
	parts[1] = []byte(base64.RawURLEncoding.EncodeToString(otherClaims))
	tampered := bytes.Join(parts, []byte("."))

	_, err = verifyMessage(testSecret, tampered)
	assert.ErrorIs(t, err, errBadSignature)
}

func TestVerify_WrongHeaderRejected(t *testing.T) {
	// An alg:none header correctly HMAC-signed with the real secret must
	// STILL be rejected: the verifier compares the header segment against
	// the pinned constant and never parses the algorithm from the wire.
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	claimsJSON, err := json.Marshal(wireClaims{V: 1, Typ: TypRequestLock, RID: newRID(), Body: json.RawMessage(`{}`)})
	require.NoError(t, err)
	signingInput := header + "." + base64.RawURLEncoding.EncodeToString(claimsJSON)

	mac := hmac.New(sha256.New, testSecret)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	_, err = verifyMessage(testSecret, []byte(signingInput+"."+sig))
	assert.ErrorIs(t, err, errBadHeader)
}

func TestVerify_WrongVersionRejected(t *testing.T) {
	claimsJSON, err := json.Marshal(wireClaims{V: 2, Typ: TypRequestLock, RID: newRID(), Body: json.RawMessage(`{}`)})
	require.NoError(t, err)
	signingInput := pinnedHeader + "." + base64.RawURLEncoding.EncodeToString(claimsJSON)

	mac := hmac.New(sha256.New, testSecret)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	_, err = verifyMessage(testSecret, []byte(signingInput+"."+sig))
	assert.ErrorIs(t, err, errBadVersion)
}

func TestVerify_Malformed(t *testing.T) {
	_, err := verifyMessage(testSecret, []byte("not-a-jws"))
	require.ErrorIs(t, err, errMalformed)

	_, err = verifyMessage(testSecret, []byte("a.b.c.d"))
	assert.ErrorIs(t, err, errMalformed)
}

func TestVerify_OversizedDatagramRejected(t *testing.T) {
	big := bytes.Repeat([]byte("x"), maxDatagramSize+1)
	_, err := verifyMessage(testSecret, big)
	assert.ErrorIs(t, err, errMalformed)
}

func TestNewRID(t *testing.T) {
	a := newRID()
	b := newRID()
	assert.Len(t, a, 16)
	assert.Len(t, b, 16)
	assert.NotEqual(t, a, b)
}

func TestCallUDP_Integration(t *testing.T) {
	m := NewLockManager("server-node")
	m.Secret = testSecret
	addr, err := m.Start("127.0.0.1:0")
	require.NoError(t, err)
	defer m.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := LockRequest{Path: "integration/path", NodeID: "client-node", LockID: "lock-1", LamportTime: 7, FencingToken: "fence-1"}
	var resp LockResponse
	err = CallUDP(ctx, testSecret, addr, TypRequestLock, req, &resp)
	require.NoError(t, err)
	assert.Equal(t, LockOK, resp.Status)
	assert.NotEmpty(t, resp.FencingToken)
}

func TestCallUDP_WrongSecretTimesOut(t *testing.T) {
	m := NewLockManager("server-node")
	m.Secret = testSecret
	addr, err := m.Start("127.0.0.1:0")
	require.NoError(t, err)
	defer m.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	req := LockRequest{Path: "wrong-secret/path", NodeID: "client-node", LockID: "lock-1"}
	var resp LockResponse
	err = CallUDP(ctx, []byte("a-different-secret-16-chars"), addr, TypRequestLock, req, &resp)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCallUDP_ForeignRIDIgnored(t *testing.T) {
	// A raw UDP server that replies to the first request with a
	// validly-signed response carrying the WRONG request ID, then goes
	// silent. The client must ignore it and eventually time out.
	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer serverConn.Close()

	go func() {
		buf := make([]byte, 64*1024)
		n, src, err := serverConn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		claims, err := verifyMessage(testSecret, buf[:n])
		if err != nil {
			return
		}
		reply, err := signMessage(testSecret, claims.Typ+respSuffix, "ffffffffffffffff", LockResponse{Status: LockOK, FencingToken: "forged"})
		if err != nil {
			return
		}
		_, _ = serverConn.WriteToUDP(reply, src)
		// Then nothing: subsequent retransmissions are ignored.
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var resp LockResponse
	err = CallUDP(ctx, testSecret, serverConn.LocalAddr().String(), TypRequestLock, LockRequest{Path: "p"}, &resp)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Empty(t, resp.FencingToken)
}
