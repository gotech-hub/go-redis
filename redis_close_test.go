package redis

import "testing"

func TestClient_CloseIsSafeOnNil(t *testing.T) {
	c := &Client{}
	if err := c.Close(); err != nil {
		t.Fatalf("expected nil error closing empty client, got %v", err)
	}
}
