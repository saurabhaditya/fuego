package main

import (
	"fmt"
	"io"
	"log"
	"time"

	"cloud.google.com/go/firestore"
)

// wrapper to stream the json serialized results
type displayItemWriter struct {
	isFirst bool
	writer  *io.Writer
}

func newDisplayItemWriter(writer *io.Writer) displayItemWriter {
	return displayItemWriter{true, writer}
}

func (d *displayItemWriter) Write(doc *firestore.DocumentSnapshot, extendedJson bool) error {
	if d.isFirst {
		_, err := fmt.Fprintln(*d.writer, "[")
		if err != nil {
			return err
		}
		d.isFirst = false
	} else {
		_, err := fmt.Fprintln(*d.writer, ",")
		if err != nil {
			return err
		}
	}

	return writeSnapshot(*d.writer, doc, extendedJson)
}

func (d *displayItemWriter) Close() {
	if !d.isFirst {
		_, err := fmt.Fprintln(*d.writer, "]")
		if err != nil {
			log.Panicf("Could not write finishing part of results. %v", err)
		}
	}
}

func writeSnapshot(writer io.Writer, doc *firestore.DocumentSnapshot, extendedJson bool) error {
	var data = doc.Data()

	if extendedJson {
		transformFirestoreMapToExtendedJsonMap(data)
	}
	// Convert timestamp fields to _seconds and _nanoseconds format
	// transformTimestampFields(data)
	/*
		"lastUpdatedAt": {
			"_seconds": 1739319347,
			"_nanoseconds": 418681000
		}
	*/

	jsonString, err := marshallData(data, false)

	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(writer, jsonString)

	if err != nil {
		return err
	}
	return nil
}

// transformTimestampFields recursively converts all timestamp fields in a map to _seconds and _nanoseconds format
func transformTimestampFields(m map[string]interface{}) {
	for k, v := range m {
		switch v := v.(type) {
		case time.Time:
			m[k] = map[string]interface{}{
				"_seconds":     v.Unix(),
				"_nanoseconds": int64(v.Nanosecond()),
			}
		case []interface{}:
			for _, item := range v {
				if itemMap, ok := item.(map[string]interface{}); ok {
					transformTimestampFields(itemMap)
				}
			}
		case map[string]interface{}:
			transformTimestampFields(v)
		}
	}
}
