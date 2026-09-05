package crypto

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
)

func TestKeyPairPEMRoundTrip(t *testing.T) {
	priv, pub, err := GenerateKeyPair(2048)
	if err != nil {
		t.Fatal(err)
	}

	block, _ := pem.Decode(EncodePrivateKey(priv))
	if block == nil || block.Type != "RSA PRIVATE KEY" {
		t.Fatalf("private PEM block: %+v", block)
	}
	if parsed, err := x509.ParsePKCS1PrivateKey(block.Bytes); err != nil || !parsed.Equal(priv) {
		t.Fatalf("private key did not survive PEM: %v", err)
	}

	pubPEM, err := EncodePublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	block, _ = pem.Decode(pubPEM)
	if block == nil || block.Type != "PUBLIC KEY" {
		t.Fatalf("public PEM block: %+v", block)
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil || !parsed.(*rsa.PublicKey).Equal(pub) {
		t.Fatalf("public key did not survive PEM: %v", err)
	}
}
