package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

var kBigEmptyBytes = make([]byte, 100_000)

func parseStrings(file io.Reader) (out []interface{}, err error) {
	var num, strLen uint32
	err = binary.Read(file, binary.LittleEndian, &num)
	if err != nil {
		return nil, err
	}

	out = make([]interface{}, num)
	for i := range out {
		err = binary.Read(file, binary.LittleEndian, &strLen)
		if err != nil {
			return nil, err
		}
		out[i] = strLen
	}

	for i := range out {
		strLen = out[i].(uint32)

		if strLen == 0 {
			err = binary.Read(file, binary.LittleEndian, &strLen)
			if err != nil {
				return nil, err
			}
			out[i] = kBigEmptyBytes[:strLen]
			continue
		}

		buf := make([]byte, strLen)
		_, err := io.ReadFull(file, buf)
		if err != nil {
			return nil, err
		}

		out[i] = string(buf)
	}
	return
}

func parseRecords(filename string, cb func(Record) bool) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		var rec Record
		err := binary.Read(reader, binary.LittleEndian, &rec.RecordHeader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		rec.values, err = parseStrings(reader)
		if err != nil {
			return err
		}

		if !cb(rec) {
			return nil
		}
	}

	return nil
}
