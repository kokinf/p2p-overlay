package tunnel

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"io"
)

func hkdfSHA256(secret, salt, info []byte, length int) []byte {
	if len(salt) == 0 {
		salt = make([]byte, sha256.Size)
	}
	h := hmac.New(sha256.New, salt)
	h.Write(secret)
	prk := h.Sum(nil)

	var out, prev []byte
	for i := byte(1); len(out) < length; i++ {
		h := hmac.New(sha256.New, prk)
		h.Write(prev)
		h.Write(info)
		h.Write([]byte{i})
		prev = h.Sum(nil)
		out = append(out, prev...)
	}
	return out[:length]
}

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
	return hkdfSHA256(shared, nil, tunnelID[:], 32), nil
}

func Encrypt(key, nonce, plaintext []byte) ([]byte, error) {
	block, _ := aes.NewCipher(key)
	gcm, _ := cipher.NewGCM(block)
	return gcm.Seal(nil, nonce, plaintext, nil), nil
}

func Decrypt(key, nonce, ct []byte) ([]byte, error) {
	block, _ := aes.NewCipher(key)
	gcm, _ := cipher.NewGCM(block)
	return gcm.Open(nil, nonce, ct, nil)
}

func RandomNonce() []byte {
	n := make([]byte, 12)
	io.ReadFull(rand.Reader, n)
	return n
}
