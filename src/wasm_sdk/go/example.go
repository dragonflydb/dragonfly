package main

import "encoding/binary"
import "strings"

var buffer []byte

//export provide_buffer
func provide_buffer(bytes int) *byte {
  buffer = []byte(strings.Repeat("x", bytes))
  return &buffer[0]
}

//go:wasm-module dragonfly
//export call
func call(str *byte)

func toByteArray(i int) (arr []byte) {
    arr = []byte("xxxx")
    binary.LittleEndian.PutUint32(arr[0:4], uint32(i))
    return
}

func send(args... string) string {
  var data []byte
  data = append(data, toByteArray(len(args))...)
  data = data[0:4]

  for _, element := range args {
    var sz = toByteArray(len(element))
    var payload = []byte(element)
    data = append(data, sz...)
    data = append(data, payload...)
  }

  call(&data[0])
  return string(buffer)
}

//export go_hi
func go_hi() {
  var res = send("set", "foo", "bar");
  println("Result is ", res)
  res = send("get", "foo");
  println("Result is ", res)
}

// main is required for the `wasi` target, even if it isn't used.
func main() {}
