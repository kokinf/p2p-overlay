package tunnel

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
)

func NewE2EKeypair() (*ecdh.PrivateKey, []byte) {
	priv, _ := ecdh.X25519().GenerateKey(rand.Reader)
	return priv, priv.PublicKey().Bytes()
}

func DeriveE2EKey(priv *ecdh.PrivateKey, peerPub []byte, tunnelID [16]byte) ([]byte, error) {
	peer, err := ecdh.X25519().NewPublicKey(peerPub)
	if err != nil {
		return nil, err
	}
	shared, err := priv.ECDH(peer)
	if err != nil {
		return nil, err
	}
	return hkdf.Key(sha256.New, shared, nil, string(tunnelID[:]), 32)
}

func Encrypt(key, nonce, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(nonce) != gcm.NonceSize() {
		return nil, errors.New("invalid nonce size")
	}
	return gcm.Seal(nil, nonce, plaintext, nil), nil
}

func Decrypt(key, nonce, ct []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(nonce) != gcm.NonceSize() {
		return nil, errors.New("invalid nonce size")
	}
	return gcm.Open(nil, nonce, ct, nil)
}

func RandomNonce() []byte {
	n := make([]byte, 12)
	io.ReadFull(rand.Reader, n)
	return n
}

func DataContext(id []byte, seq uint64) []byte {
	b := append([]byte("tunnel-data-v1"), id...)
	return binary.BigEndian.AppendUint64(b, seq)
}
func SealData(key, nonce, data, id []byte, seq uint64) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(nonce) != gcm.NonceSize() {
		return nil, errors.New("invalid nonce")
	}
	return gcm.Seal(nil, nonce, data, DataContext(id, seq)), nil
}
func OpenData(key, nonce, data, id []byte, seq uint64) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(nonce) != gcm.NonceSize() {
		return nil, errors.New("invalid nonce")
	}
	return gcm.Open(nil, nonce, data, DataContext(id, seq))
}
func AckTag(key, id []byte, seq uint64, ct []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte("tunnel-ack-v1"))
	h.Write(DataContext(id, seq))
	h.Write(ct)
	return h.Sum(nil)
}
func VerifyAck(key, id []byte, seq uint64, ct, tag []byte) bool {
	return hmac.Equal(AckTag(key, id, seq, ct), tag)
}
