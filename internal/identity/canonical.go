package identity

func CanonicalEncodePublicKey(pub []byte) []byte {
	out := make([]byte, len(pub))
	copy(out, pub)
	return out
}
