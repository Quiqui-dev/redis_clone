package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// RESP consts
const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

// RESP Structures

type Value struct {
	typ   string
	str   string
	num   int
	bulk  string
	array []Value
}

// Reader and Writer structs and inits

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

// Reading RESP

func (resp *Resp) readLine() (line []byte, n int, err error) {
	/*
		This func reads one byte at a time until it reaches '\r'
		as this indicates the end of the line.

		We return the line wihout the last two bytes which are \r\n
	*/

	for {
		b, err := resp.reader.ReadByte()

		if err != nil {
			return nil, 0, err
		}

		n += 1
		line = append(line, b)

		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], n, nil
}

func (resp *Resp) readInteger() (x int, n int, err error) {
	/*

	 */

	line, n, err := resp.readLine()

	if err != nil {
		return 0, 0, err
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)

	if err != nil {
		return 0, 0, err
	}

	return int(i64), n, nil
}

func (resp *Resp) Read() (Value, error) {
	_type, err := resp.reader.ReadByte()

	if err != nil {
		return Value{}, err
	}

	switch _type {

	case ARRAY:
		return resp.readArray()
	case BULK:
		return resp.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

func (resp *Resp) readArray() (Value, error) {
	value := Value{}
	value.typ = "array"

	// read length of array
	len, _, err := resp.readInteger()

	if err != nil {
		return value, err
	}

	// for each line, parse and read the value
	value.array = make([]Value, 0)

	for i := 0; i < len; i++ {
		val, err := resp.Read()

		if err != nil {
			return value, err
		}

		// append parsed value to array

		value.array = append(value.array, val)
	}

	return value, nil
}

func (resp *Resp) readBulk() (Value, error) {
	value := Value{}

	value.typ = "bulk"

	len, _, err := resp.readInteger()

	if err != nil {
		return value, err
	}

	bulk := make([]byte, len)

	resp.reader.Read(bulk)

	value.bulk = string(bulk)

	// read the trailing CRLF
	resp.readLine()

	return value, nil
}

// Writing RESP

func (wrtr *Writer) Write(val Value) error {
	var bytes = val.Marshal()

	_, err := wrtr.writer.Write(bytes)

	if err != nil {
		return err
	}

	return nil
}

func (val Value) Marshal() []byte {

	switch val.typ {
	case "array":
		return val.marshalArray()
	case "bulk":
		return val.marshalBulk()
	case "string":
		return val.marshalString()
	case "null":
		return val.marshalNull()
	case "error":
		return val.marshalError()
	default:
		return []byte{}
	}
}

func (val Value) marshalString() []byte {
	var bytes []byte

	bytes = append(bytes, STRING)
	bytes = append(bytes, val.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (val Value) marshalBulk() []byte {

	var bytes []byte

	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(val.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, val.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (val Value) marshalArray() []byte {
	len := len(val.array)

	var bytes []byte

	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, val.array[i].Marshal()...)
	}

	return bytes
}

func (val Value) marshalError() []byte {
	var bytes []byte

	bytes = append(bytes, ERROR)
	bytes = append(bytes, val.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (val Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}
