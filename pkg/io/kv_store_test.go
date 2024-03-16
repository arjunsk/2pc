package io

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewKeyValueStore(t *testing.T) {
	a := NewKeyValueStore(getBasePath() + "/data/")

	err := a.Put("foo", "bar")
	assert.Nil(t, err)

	val, err := a.Get("foo")
	assert.Nil(t, err)
	assert.Equal(t, "bar", val)
}
