package uint128

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUUID(t *testing.T) {
	s := "208f3afd-4faf-5761-b65a-bf568a99ccb5"
	UUID, err := uuid.Parse(s)
	require.Nil(t, err)

	u := FromUUID(UUID)
	assert.Equal(t, Uint128{Lo: 0xB65ABF568A99CCB5, Hi: 0x208F3AFD4FAF5761}, u)

	assert.Equal(t, s, u.AsUUID().String())
}

func TestString(t *testing.T) {
	u := Uint128{Lo: 0xB65ABF568A99CCB5, Hi: 0x208F3AFD4FAF5761}
	assert.Equal(t, "208f3afd4faf5761b65abf568a99ccb5", u.String())
}
