package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// protobufCodec encodes the server's existing transport structs as protobuf
// wire messages. Row maps remain self-contained JSON byte fields because SQL
// cells are dynamically typed; snapshot and WAL []byte fields stay raw and no
// longer pay JSON's base64 expansion. See tinysql.proto for the public schema.
type protobufCodec struct{}

// Name deliberately replaces grpc-go's default "proto" codec inside this
// server process. The bytes follow tinysql.proto exactly, so ordinary clients
// generated from that schema can use gRPC's standard application/grpc content
// type without knowing about the server's reflection-based implementation.
func (protobufCodec) Name() string { return "proto" }

type protobufFieldKind uint8

const (
	protobufString protobufFieldKind = iota + 1
	protobufBool
	protobufInt
	protobufUint
	protobufBytes
	protobufStrings
	protobufJSONMap
	protobufJSONMaps
)

type protobufField struct {
	number protowire.Number
	index  int
	kind   protobufFieldKind
}

type protobufDescriptor struct {
	fields   []protobufField
	byNumber map[protowire.Number]protobufField
}

var protobufDescriptors sync.Map // map[reflect.Type]*protobufDescriptor

func protobufDescriptorFor(t reflect.Type) (*protobufDescriptor, error) {
	if cached, ok := protobufDescriptors.Load(t); ok {
		return cached.(*protobufDescriptor), nil
	}
	d := &protobufDescriptor{byNumber: make(map[protowire.Number]protobufField)}
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("protobuf")
		if tag == "" {
			continue
		}
		n, err := strconv.Atoi(tag)
		if err != nil || n <= 0 || protowire.Number(n) > protowire.MaxValidNumber {
			return nil, fmt.Errorf("invalid protobuf field number %q on %s.%s", tag, t, t.Field(i).Name)
		}
		kind, err := protobufKindFor(t.Field(i).Type)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", t, t.Field(i).Name, err)
		}
		field := protobufField{number: protowire.Number(n), index: i, kind: kind}
		if _, exists := d.byNumber[field.number]; exists {
			return nil, fmt.Errorf("duplicate protobuf field number %d on %s", field.number, t)
		}
		d.fields = append(d.fields, field)
		d.byNumber[field.number] = field
	}
	actual, _ := protobufDescriptors.LoadOrStore(t, d)
	return actual.(*protobufDescriptor), nil
}

var (
	bytesType    = reflect.TypeFor[[]byte]()
	stringsType  = reflect.TypeFor[[]string]()
	jsonMapType  = reflect.TypeFor[map[string]any]()
	jsonMapsType = reflect.TypeFor[[]map[string]any]()
)

func protobufKindFor(t reflect.Type) (protobufFieldKind, error) {
	switch {
	case t == bytesType:
		return protobufBytes, nil
	case t == stringsType:
		return protobufStrings, nil
	case t == jsonMapType:
		return protobufJSONMap, nil
	case t == jsonMapsType:
		return protobufJSONMaps, nil
	}
	switch t.Kind() {
	case reflect.String:
		return protobufString, nil
	case reflect.Bool:
		return protobufBool, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return protobufInt, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return protobufUint, nil
	default:
		return 0, fmt.Errorf("unsupported protobuf field type %s", t)
	}
}

func protobufMessage(v any, writable bool) (reflect.Value, *protobufDescriptor, error) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return reflect.Value{}, nil, fmt.Errorf("protobuf message is nil")
	}
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return reflect.Value{}, nil, fmt.Errorf("protobuf message is nil")
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct || (writable && !rv.CanSet()) {
		return reflect.Value{}, nil, fmt.Errorf("protobuf message must be a writable struct pointer, got %T", v)
	}
	d, err := protobufDescriptorFor(rv.Type())
	return rv, d, err
}

func (protobufCodec) Marshal(v any) ([]byte, error) {
	if message, ok := v.(proto.Message); ok {
		return proto.Marshal(message)
	}
	rv, descriptor, err := protobufMessage(v, false)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 256)
	for _, field := range descriptor.fields {
		value := rv.Field(field.index)
		switch field.kind {
		case protobufString:
			if value.Len() > 0 {
				out = appendProtoBytes(out, field.number, []byte(value.String()))
			}
		case protobufBool:
			if value.Bool() {
				out = appendProtoVarint(out, field.number, 1)
			}
		case protobufInt:
			if value.Int() != 0 {
				out = appendProtoVarint(out, field.number, uint64(value.Int()))
			}
		case protobufUint:
			if value.Uint() != 0 {
				out = appendProtoVarint(out, field.number, value.Uint())
			}
		case protobufBytes:
			if value.Len() > 0 {
				out = appendProtoBytes(out, field.number, value.Bytes())
			}
		case protobufStrings:
			for i := 0; i < value.Len(); i++ {
				out = appendProtoBytes(out, field.number, []byte(value.Index(i).String()))
			}
		case protobufJSONMap:
			if !value.IsNil() {
				encoded, err := storage.JSONMarshal(value.Interface())
				if err != nil {
					return nil, err
				}
				out = appendProtoBytes(out, field.number, encoded)
			}
		case protobufJSONMaps:
			for i := 0; i < value.Len(); i++ {
				encoded, err := storage.JSONMarshal(value.Index(i).Interface())
				if err != nil {
					return nil, err
				}
				out = appendProtoBytes(out, field.number, encoded)
			}
		}
	}
	return out, nil
}

func appendProtoBytes(dst []byte, number protowire.Number, value []byte) []byte {
	dst = protowire.AppendTag(dst, number, protowire.BytesType)
	return protowire.AppendBytes(dst, value)
}

func appendProtoVarint(dst []byte, number protowire.Number, value uint64) []byte {
	dst = protowire.AppendTag(dst, number, protowire.VarintType)
	return protowire.AppendVarint(dst, value)
}

func (protobufCodec) Unmarshal(data []byte, v any) error {
	if message, ok := v.(proto.Message); ok {
		return proto.Unmarshal(data, message)
	}
	rv, descriptor, err := protobufMessage(v, true)
	if err != nil {
		return err
	}
	rv.SetZero()
	for len(data) > 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
		field, known := descriptor.byNumber[number]
		if !known {
			n = protowire.ConsumeFieldValue(number, wireType, data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
			continue
		}
		value := rv.Field(field.index)
		switch field.kind {
		case protobufString, protobufBytes, protobufStrings, protobufJSONMap, protobufJSONMaps:
			if wireType != protowire.BytesType {
				return fmt.Errorf("protobuf field %d: got wire type %d, want bytes", number, wireType)
			}
			encoded, consumed := protowire.ConsumeBytes(data)
			if consumed < 0 {
				return protowire.ParseError(consumed)
			}
			data = data[consumed:]
			if err := setProtoBytes(value, field.kind, encoded); err != nil {
				return fmt.Errorf("protobuf field %d: %w", number, err)
			}
		case protobufBool, protobufInt, protobufUint:
			if wireType != protowire.VarintType {
				return fmt.Errorf("protobuf field %d: got wire type %d, want varint", number, wireType)
			}
			numeric, consumed := protowire.ConsumeVarint(data)
			if consumed < 0 {
				return protowire.ParseError(consumed)
			}
			data = data[consumed:]
			switch field.kind {
			case protobufBool:
				value.SetBool(numeric != 0)
			case protobufInt:
				value.SetInt(int64(numeric))
			case protobufUint:
				value.SetUint(numeric)
			}
		}
	}
	return nil
}

func setProtoBytes(dst reflect.Value, kind protobufFieldKind, encoded []byte) error {
	switch kind {
	case protobufString:
		dst.SetString(string(encoded))
	case protobufBytes:
		dst.SetBytes(append(dst.Bytes()[:0], encoded...))
	case protobufStrings:
		dst.Set(reflect.Append(dst, reflect.ValueOf(string(encoded))))
	case protobufJSONMap:
		var row map[string]any
		if err := json.Unmarshal(encoded, &row); err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(row))
	case protobufJSONMaps:
		var row map[string]any
		if err := json.Unmarshal(encoded, &row); err != nil {
			return err
		}
		dst.Set(reflect.Append(dst, reflect.ValueOf(row)))
	}
	return nil
}
