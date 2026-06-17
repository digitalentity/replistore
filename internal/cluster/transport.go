package cluster

import (
	"context"
	"crypto/hmac"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"
)

// Message types carried in the "typ" claim of a lock datagram. Responses use
// the same type with the "_resp" suffix appended.
const (
	TypRequestLock = "request_lock"
	TypRenewLock   = "renew_lock"
	TypReleaseLock = "release_lock"

	respSuffix = "_resp"

	ridSize  = 8
	jwsParts = 3
)

// pinnedHeader is the base64url-encoded form of the one and only JOSE header
// this transport ever produces or accepts: {"alg":"HS256","typ":"JWT"}.
//
// The verifier compares the first datagram segment byte-for-byte against this
// constant and never parses the algorithm from the wire. This forecloses the
// classic JWT alg-confusion and alg:"none" attacks: an attacker cannot select
// a weaker (or absent) algorithm, because the algorithm is not negotiable.
var pinnedHeader = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))

// maxDatagramSize is the largest datagram the verifier will even look at.
// Lock messages are tiny; anything larger is garbage or abuse.
const maxDatagramSize = 8 * 1024

// wireClaims is the claims body of every lock datagram.
//
// There are deliberately no exp/iat claims: lease TTLs handle expiry, and
// timestamp claims would reintroduce cross-node clock comparison, which this
// codebase deliberately avoids.
type wireClaims struct {
	V    int             `json:"v"`    // format version, 1
	Typ  string          `json:"typ"`  // message type
	RID  string          `json:"rid"`  // 16 hex chars from crypto/rand (8 bytes)
	Body json.RawMessage `json:"body"` // marshaled lock message
}

var (
	errMalformed    = errors.New("malformed datagram")
	errBadHeader    = errors.New("unexpected JOSE header")
	errBadSignature = errors.New("bad signature")
	errBadVersion   = errors.New("unsupported message version")
)

// newRID returns a fresh request ID: 8 bytes from crypto/rand, hex-encoded.
//
// RID unpredictability is part of the response-authenticity story: an
// off-path attacker who knows the shared secret distribution boundary but
// cannot observe the request must guess the RID to forge a response the
// client will accept. Never use math/rand here.
func newRID() string {
	b := make([]byte, ridSize)
	if _, err := cryptorand.Read(b); err != nil {
		// crypto/rand never fails on supported platforms.
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}

	return hex.EncodeToString(b)
}

// signMessage builds a complete JWS compact serialization (HS256) datagram
// for the given message type, request ID and body.
func signMessage(secret []byte, typ, rid string, body any) ([]byte, error) {
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	claimsJSON, err := json.Marshal(wireClaims{V: 1, Typ: typ, RID: rid, Body: bodyJSON})
	if err != nil {
		return nil, err
	}

	signingInput := pinnedHeader + "." + base64.RawURLEncoding.EncodeToString(claimsJSON)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return []byte(signingInput + "." + sig), nil
}

// verifyMessage checks a received datagram: size, structure, pinned header,
// HMAC, and format version. It returns the parsed claims on success.
func verifyMessage(secret []byte, datagram []byte) (*wireClaims, error) {
	if len(datagram) > maxDatagramSize {
		return nil, errMalformed
	}
	parts := strings.Split(string(datagram), ".")
	if len(parts) != jwsParts {
		return nil, errMalformed
	}

	// Byte-for-byte comparison against the pinned header; the algorithm is
	// never parsed from the wire (see pinnedHeader).
	if parts[0] != pinnedHeader {
		return nil, errBadHeader
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, errMalformed
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return nil, errBadSignature
	}

	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, errMalformed
	}
	var claims wireClaims
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		return nil, errMalformed
	}
	if claims.V != 1 {
		return nil, errBadVersion
	}

	return &claims, nil
}

const (
	callInitialRetransmit = 200 * time.Millisecond
	callReadBufferSize    = 64 * 1024
)

// CallUDP sends a signed lock request datagram to peerAddr and waits for a
// signed response of type typ+"_resp" carrying the same request ID,
// retransmitting with exponential backoff (200, 400, 800 ms, ...) until ctx
// expires. The response body is unmarshaled into resp.
//
// Handlers are idempotent per (NodeID, LockID), so retransmitted requests
// are harmless duplicates on the server side.
func CallUDP(ctx context.Context, secret []byte, peerAddr, typ string, req, resp any) error {
	raddr, err := net.ResolveUDPAddr("udp", peerAddr)
	if err != nil {
		return err
	}
	// A connected socket: the kernel filters out datagrams arriving from any
	// other source address.
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	rid := newRID()
	datagram, err := signMessage(secret, typ, rid, req)
	if err != nil {
		return err
	}
	wantTyp := typ + respSuffix

	interval := callInitialRetransmit
	buf := make([]byte, callReadBufferSize)

	if _, err := conn.Write(datagram); err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	nextRetransmit := time.Now().Add(interval)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Wake up at the earlier of the next retransmission and ctx expiry.
		readDeadline := nextRetransmit
		if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(readDeadline) {
			readDeadline = ctxDeadline
		}
		_ = conn.SetReadDeadline(readDeadline)

		n, err := conn.Read(buf)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
				// Read window elapsed: retransmit the same datagram and back off.
				if time.Now().Before(nextRetransmit) {
					continue
				}
				_, _ = conn.Write(datagram)
				interval *= 2
				nextRetransmit = time.Now().Add(interval)

				continue
			}
			// Any other read error (e.g. ECONNREFUSED delivered via ICMP on a
			// connected UDP socket): the peer may simply not be listening yet
			// and may come up mid-retry, so do NOT fail fast — keep looping
			// until ctx expires.
			continue
		}

		claims, verr := verifyMessage(secret, buf[:n])
		if verr != nil || claims.Typ != wantTyp || claims.RID != rid {
			// Forged, stale, or mismatched datagram: drop silently, keep reading.
			continue
		}

		return json.Unmarshal(claims.Body, resp)
	}
}

func isTimeout(err error) bool {
	var ne net.Error

	return errors.As(err, &ne) && ne.Timeout()
}

// serveLoop reads lock datagrams off the manager's UDP socket, verifies
// them, dispatches to the (unchanged) lock handlers, and replies to the
// source address. Handlers are fast in-memory operations, so datagrams are
// handled inline without a goroutine per packet.
func (m *LockManager) serveLoop() {
	buf := make([]byte, callReadBufferSize)
	for {
		n, src, err := m.conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-m.stopCh:
				return
			default:
			}
			if !isTimeout(err) {
				// Closed socket or fatal error.
				return
			}

			continue
		}

		claims, err := verifyMessage(m.Secret, buf[:n])
		if err != nil {
			// Unauthenticated or malformed traffic is dropped silently (no
			// response: nothing here is worth telling an unauthenticated
			// sender).
			m.log.Debug("Dropping lock datagram", slog.String("src", src.String()), slog.Any("error", err))

			continue
		}

		var respBody any
		switch claims.Typ {
		case TypRequestLock:
			var req LockRequest
			if err := json.Unmarshal(claims.Body, &req); err != nil {
				m.log.Debug("Dropping lock datagram: bad body", slog.String("src", src.String()), slog.Any("error", err))

				continue
			}
			var resp LockResponse
			_ = m.RequestLock(req, &resp)
			respBody = resp
		case TypRenewLock:
			var req LockRenewal
			if err := json.Unmarshal(claims.Body, &req); err != nil {
				m.log.Debug("Dropping lock datagram: bad body", slog.String("src", src.String()), slog.Any("error", err))

				continue
			}
			var status LockStatus
			_ = m.RenewLock(req, &status)
			respBody = status
		case TypReleaseLock:
			var req LockRelease
			if err := json.Unmarshal(claims.Body, &req); err != nil {
				m.log.Debug("Dropping lock datagram: bad body", slog.String("src", src.String()), slog.Any("error", err))

				continue
			}
			var status LockStatus
			_ = m.ReleaseLock(req, &status)
			respBody = status
		default:
			m.log.Debug("Dropping lock datagram: unknown type", slog.String("src", src.String()), slog.String("type", claims.Typ))

			continue
		}

		reply, err := signMessage(m.Secret, claims.Typ+respSuffix, claims.RID, respBody)
		if err != nil {
			m.log.Debug("Failed to sign lock response", slog.Any("error", err))

			continue
		}
		if _, err := m.conn.WriteToUDP(reply, src); err != nil {
			m.log.Debug("Failed to send lock response", slog.String("src", src.String()), slog.Any("error", err))
		}
	}
}
