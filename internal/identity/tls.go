package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"time"

	"p2p-overlay/internal/nodeid"
)

var errInvalidCertKey = errors.New("invalid public key type in cert")

func (i *Identity) GenerateTLSCertificate() (tls.Certificate, error) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: i.NodeID().String(),
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, i.PublicKey, i.privateKey)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  i.privateKey,
	}, nil
}

func ExtractNodeIDFromCert(cert *x509.Certificate) (nodeid.ID, error) {
	pubKey, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return nodeid.ID{}, errInvalidCertKey
	}
	return NodeIDFromPublicKey(pubKey)
}
