package encoding

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

// test that we didn't break GOB.
func TestGOB(t *testing.T) {
	type T1 struct {
		T1int0    int
		T1int1    int
		T1string0 string
		T1string1 string
	}

	type T2 struct {
		T2slice []T1
		T2map   map[int]*T1
		T2t3    interface{}
	}

	type T3 struct {
		T3int999 int
	}

	e0 := errorCount

	writer := new(bytes.Buffer)

	Register(T3{})

	var err error
	// encode
	{
		enc := NewEncoder(writer)

		x0 := 0
		err = enc.Encode(x0)
		assert.Nil(t, err)

		x1 := 1
		err = enc.Encode(x1)
		assert.Nil(t, err)

		t1 := T1{
			T1int1:    1,
			T1string1: "6.5840",
		}
		err = enc.Encode(t1)
		assert.Nil(t, err)

		t2 := T2{
			T2slice: []T1{T1{}, t1},
			T2map:   map[int]*T1{},
			T2t3:    T3{999},
		}
		t2.T2map[99] = &T1{1, 2, "x", "y"}
		err = enc.Encode(t2)
		assert.Nil(t, err)
	}
	data := writer.Bytes()

	// decode
	{
		reader := bytes.NewBuffer(data)
		dec := NewDecoder(reader)

		var x0 int
		err = dec.Decode(&x0)
		assert.Nil(t, err)
		assert.Equal(t, 0, x0)

		var x1 int
		err = dec.Decode(&x1)
		assert.Nil(t, err)
		assert.Equal(t, 1, x1)

		var t1 T1
		err = dec.Decode(&t1)
		assert.Nil(t, err)
		assert.Equal(t, 1, t1.T1int1)
		assert.Equal(t, "6.5840", t1.T1string1)
		assert.Equal(t, 0, t1.T1int0)
		assert.Equal(t, "", t1.T1string0)

		var t2 T2
		err = dec.Decode(&t2)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(t2.T2slice))
		assert.Equal(t, T1{}, t2.T2slice[0])
		assert.Equal(t, t1, t2.T2slice[1])
		assert.Equal(t, 1, len(t2.T2map))
		assert.Equal(t, &T1{1, 2, "x", "y"}, t2.T2map[99])
		assert.Equal(t, T3{999}, t2.T2t3)
	}

	assert.Equal(t, e0, errorCount)
}

// make sure we check capitalization
// encoding prints one warning during this test.
func TestCapital(t *testing.T) {
	type T4 struct {
		Yes int
		no  int
	}

	e0 := errorCount

	var val []map[*T4]int

	writer := new(bytes.Buffer)
	enc := NewEncoder(writer)
	err := enc.Encode(val)
	assert.Nil(t, err)

	data := writer.Bytes()

	var v1 []map[T4]int
	reader := bytes.NewBuffer(data)
	dec := NewDecoder(reader)
	err = dec.Decode(&v1)
	assert.Nil(t, err)

	assert.Equal(t, e0+1, errorCount)
}

// check that we warn when someone sends a default value over
// RPC but the target into which we're decoding holds a non-default
// value, which GOB seems not to overwrite as you'd expect.
//
// encoding does not print a warning.
func TestDefault(t *testing.T) {
	type DD struct {
		X int
	}

	e0 := errorCount

	// send a default value...
	dd1 := DD{}

	writer := new(bytes.Buffer)
	enc := NewEncoder(writer)
	err := enc.Encode(dd1)
	assert.Nil(t, err)
	data := writer.Bytes()

	// and receive it into memory that already
	// holds non-default values.
	reply := DD{99}

	reader := bytes.NewBuffer(data)
	d := NewDecoder(reader)
	err = d.Decode(&reply)
	assert.Nil(t, err)

	assert.Equal(t, e0+1, errorCount)
}
