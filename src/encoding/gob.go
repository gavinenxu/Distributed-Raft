package encoding

//
// trying to send non-capitalized fields over RPC produces a range of
// misbehavior, including both mysterious incorrect computation and
// outright crashes. so this wrapper around Go's encoding/gob warns
// about non-capitalized field names.
//

import (
	"encoding/gob"
	"log"
)
import "io"
import "reflect"
import "sync"
import "unicode"
import "unicode/utf8"

var mu sync.Mutex
var errorCount int // for TestCapital
var checked map[reflect.Type]bool

// Encoder

type Encoder struct {
	gob *gob.Encoder
}

func NewEncoder(w io.Writer) *Encoder {
	enc := &Encoder{
		gob: gob.NewEncoder(w),
	}
	return enc
}

func (enc *Encoder) Encode(e interface{}) error {
	checkValue(e)
	return enc.gob.Encode(e)
}

func (enc *Encoder) EncodeValue(value reflect.Value) error {
	checkValue(value.Interface())
	return enc.gob.EncodeValue(value)
}

// Decoder

type Decoder struct {
	gob *gob.Decoder
}

func NewDecoder(r io.Reader) *Decoder {
	dec := &Decoder{}
	dec.gob = gob.NewDecoder(r)
	return dec
}

// Decode the value underlying e must be a pointer to the
// correct type for the next data item received.
func (dec *Decoder) Decode(e interface{}) error {
	checkValue(e)
	checkDefaultValue(e)
	return dec.gob.Decode(e)
}

// Register Only types that will be transferred as implementations of interface values need to be registered.
// Expecting to be used only during initialization
func Register(value interface{}) {
	checkValue(value)
	gob.Register(value)
}

func RegisterName(name string, value interface{}) {
	checkValue(value)
	gob.RegisterName(name, value)
}

func checkValue(value interface{}) {
	checkType(reflect.TypeOf(value))
}

func checkType(t reflect.Type) {
	k := t.Kind()

	mu.Lock()
	// only complain once, and avoid recursion.
	if checked == nil {
		checked = map[reflect.Type]bool{}
	}
	if checked[t] {
		mu.Unlock()
		return
	}
	checked[t] = true
	mu.Unlock()

	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			rune, _ := utf8.DecodeRuneInString(f.Name)
			if !unicode.IsUpper(rune) {
				// ta da
				log.Printf("encoding error: lower-case field %v of %v in RPC or persist/snapshot will break your Raft\n",
					f.Name, t.Name())
				mu.Lock()
				errorCount += 1
				mu.Unlock()
			}
			checkType(f.Type)
		}
		return
	case reflect.Slice, reflect.Array, reflect.Ptr:
		checkType(t.Elem())
		return
	case reflect.Map:
		checkType(t.Elem())
		checkType(t.Key())
		return
	default:
		return
	}
}

// warn if the value contains non-default values,
// as it would if one sent an RPC but the reply
// struct was already modified. if the RPC reply
// contains default values, GOB won't overwrite
// the non-default value.
func checkDefaultValue(value interface{}) {
	if value == nil {
		return
	}
	checkDefaultHelper(reflect.ValueOf(value), 1, "")
}

func checkDefaultHelper(value reflect.Value, depth int, name string) {
	if depth > 3 {
		return
	}

	t := value.Type()
	k := t.Kind()

	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			vv := value.Field(i)
			name1 := t.Field(i).Name
			if name != "" {
				name1 = name + "." + name1
			}
			checkDefaultHelper(vv, depth+1, name1)
		}
		return
	case reflect.Ptr:
		if value.IsNil() {
			return
		}
		checkDefaultHelper(value.Elem(), depth+1, name)
		return
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr, reflect.Float32, reflect.Float64,
		reflect.String:
		//	value not equal to default value 0, "", false, log warning
		if !reflect.DeepEqual(reflect.Zero(t).Interface(), value.Interface()) {
			mu.Lock()
			if errorCount < 1 {
				what := name
				if what == "" {
					what = t.Name()
				}
				// this warning typically arises if code re-uses the same RPC reply
				// variable for multiple RPC calls, or if code restores persisted
				// state into variable that already have non-default values.
				log.Printf("encoding warning: Decoding into a non-default variable/field %v may not work\n",
					what)
			}
			errorCount += 1
			mu.Unlock()
		}
	default:
		return
	}
}
