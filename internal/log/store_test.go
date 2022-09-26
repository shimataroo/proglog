package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("1m9BMKwJsCVFU1v4YZEOMPHYg6lBwxzwXbjLQgFJ15fQw72AOczitzVHDVjnVhp5wgNsgQh1INmZwA93NDOGJPemWfbs7jr0l5hDa3q0FzN7cOQGJgW3AmimYp3QpHPsj1RHw7TaEo1IzIRigd8GnNIuyiUP0WwyjseMzniX1kaiOEzUytuhDSmtJip2jf4Nj7WTnTWLOv7SQRYpxNucZQjzHrQTn3O6nnChsKYPDIKi8tKMJLVfzy3dAo4rHIEjOT0OgZZxSpe8yrydL9rkkXiwb3zYCgWTUW8eVPnA2A8h7N8CmkE39hjeKJCAYdN27YbctXPDwoW6qV6K7MgXXkj4Cn6MlLwg8XGAPllgFQ7RILcEiFyoJO984cpG5tMadW4yojOi7CLa7dUmDfJ5voJVj44EBleV6p8CEsuwI9jnDXqHlcBeV5e1zUYc0owsvk21t55rWwMtBbbh4iLyXjd3WNb6SPclnhCFf7Ra393O9bFGnRoT")
	width = uint64(len(write) + lenWidth)
)

func TestStoreAppend(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	s, err := newStore(f)
	require.NoError(t, err)
	testAppend(t, s)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(1); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, write, n)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size uint64, err error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, uint64(fi.Size()), nil
}
