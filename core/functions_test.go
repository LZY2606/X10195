package core

import (
	"bytes"
	"os"
	"testing"

	"github.com/hdt3213/rdb/model"
)

func TestParseFunctions(t *testing.T) {
	rdbFile, err := os.Open("../cases/function.rdb")
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()

	dec := NewDecoder(rdbFile).WithSpecialOpCode()
	var functionLua string
	dec.Parse(func(object model.RedisObject) bool {
		if object.GetType() == model.FunctionsType {
			functionObj := object.(*model.FunctionsObject)
			functionLua = functionObj.FunctionsLua
			return false
		}
		return true
	})
	expect := "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)"
	if functionLua != expect {
		t.Error("function lua is not equals")
	}
}
func TestWriteFunctions(t *testing.T) {
	payload := "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)"
	buf := bytes.NewBuffer(nil)
	enc := NewEncoder(buf)
	if err := enc.WriteHeader(); err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}
	if err := enc.WriteFunctions(payload); err != nil {
		t.Fatalf("Failed to write functions: %v", err)
	}
	if err := enc.WriteEnd(); err != nil {
		t.Fatalf("Failed to write end: %v", err)
	}
	dec := NewDecoder(buf).WithSpecialOpCode()
	var decoded string
	err := dec.Parse(func(object model.RedisObject) bool {
		if fn, ok := object.(*model.FunctionsObject); ok {
			decoded = fn.FunctionsLua
		}
		return true
	})
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	if decoded != payload {
		t.Errorf("functions payload mismatch: expected %q, got %q", payload, decoded)
	}
}
