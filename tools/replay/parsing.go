package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
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
		buf := make([]byte, strLen)

		_, err := io.ReadFull(file, buf)
		if err != nil {
			return nil, err
		}

		out[i] = string(buf)
	}
	return
}

// parseRecords reads a traffic log file. The file header (version + listener
// type) is delivered via onHeader before any record callback runs, so callers
// can configure per-listener behaviour up-front. onHeader may be nil.
//
// File format v2: one byte version = 2, then records. Legacy main-listener
// only, treated as RESP on read.
// File format v3: one byte version = 3, one byte listener_type, then records.
func parseRecords(filename string, onHeader func(listenerType uint8),
	cb func(Record) bool, ignoreErrors bool) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	var version uint8
	if err := binary.Read(reader, binary.LittleEndian, &version); err != nil {
		return err
	}

	var listenerType uint8
	switch version {
	case 2:
		listenerType = ListenerMainRESP
	case 3:
		if err := binary.Read(reader, binary.LittleEndian, &listenerType); err != nil {
			return err
		}
	default:
		log.Fatalf("Unsupported traffic log version %d (supported: 2, 3)", version)
	}
	if onHeader != nil {
		onHeader(listenerType)
	}

	recordNum := 0
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
			log.Printf("Could not parse %vth record", recordNum)
			if !ignoreErrors {
				return err
			}
			log.Printf("Ignoring parse error and continuing")
			recordNum++
			continue
		}

		if !cb(rec) {
			return nil
		}
		recordNum++
	}

	return nil
}
