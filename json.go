package gohtools

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"os"
)

func FromJsonBytes(buf []byte, o interface{}) error {
	err := json.Unmarshal(buf, &o)
	return err
}

func ToJsonBytes(v interface{}) []byte {
	bytes, err := json.MarshalIndent(v, " ", " ")
	CheckNotFatal(err)

	if err == nil {
		return bytes
	}
	return []byte("{}")
}

func ToJsonBytesNoIndent(v interface{}) []byte {
	bytes, err := json.Marshal(v)
	CheckNotFatal(err)
	if err == nil {
		return bytes
	}
	return []byte("{}")
}

func FromJsonString(buf string, o interface{}) error {
	err := json.Unmarshal([]byte(buf), &o)
	return err
}

func ToJsonString(v interface{}) string {
	bytes := ToJsonBytes(v)
	return string(bytes)
}

func ToJsonStringNoIndent(v interface{}) string {
	bytes := ToJsonBytesNoIndent(v)
	return string(bytes)
}

func ReadJsonFile(filename string, o interface{}) error {
	file, err := ioutil.ReadFile(filename)
	CheckFatal(err)
	if err != nil {
		return err
	}
	err = json.Unmarshal(file, o)
	CheckFatal(err)
	return err
}

func WriteJsonFileGZIP(filename string, o interface{}) error {
	var b bytes.Buffer
	w, err := gzip.NewWriterLevel(&b, gzip.BestCompression)
	CheckNotFatal(err)

	if err != nil {
		return err
	}
	defer w.Close()

	_, err = w.Write(ToJsonBytes(o))
	CheckNotFatal(err)
	if err != nil {
		return err
	}

	w.Close()

	err = ioutil.WriteFile(filename, b.Bytes(), 0666)
	CheckNotFatal(err)
	if err != nil {
		return err
	}

	return err
}

func ReadJsonFileGZIP(filename string, o interface{}) error {
	f, err := os.Open(filename)
	CheckNotFatal(err)

	w, err := gzip.NewReader(f)
	CheckNotFatal(err)
	if err != nil {
		return err
	}
	defer w.Close()

	b, err := ioutil.ReadAll(w)
	CheckNotFatal(err)
	if err != nil {
		return err
	}

	err = FromJsonBytes(b, o)
	CheckNotFatal(err)
	if err != nil {
		return err
	}
	return err
}
