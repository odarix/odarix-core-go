package model

import (
	"encoding/json"
	"sort"
	"strings"
	"unsafe"

	"github.com/mailru/easyjson/jwriter"
)

// LabelSet container for label pairs
//
// Warning! This container used for Go-C++ interatcion and shouldn't be modified.
type LabelSet struct {
	data  []byte // contain result of Stringify labelset
	pairs []pair
}

type pair struct{ key, value delegatedStringView }

func (p *pair) shift(d int32) {
	p.key.begin += d
	p.value.begin += d
}

type delegatedStringView struct{ begin, len int32 }

func (view delegatedStringView) reveal(data []byte) string {
	b := data[view.begin : view.begin+view.len]
	return *(*string)(unsafe.Pointer(&b)) //nolint:gosec // this is memory optimisation
}

// number of bytes addet to each key-value pair in LabelSet data
const additionalSymbols = 2 // ':' between key and value and ';' at the end

// EmptyLabelSet is a constructor of empty LabelSet
func EmptyLabelSet() LabelSet {
	return LabelSet{}
}

// LabelSetFromMap is a constructor for predefined LabelSet
func LabelSetFromMap(m map[string]string) LabelSet {
	var size int
	keys := make([]string, 0, len(m))
	for key, value := range m {
		keys = append(keys, key)
		size += len(key) + len(value) + additionalSymbols
	}
	if len(keys) > 1 {
		sort.Strings(keys)
	}

	ls := LabelSet{
		data:  make([]byte, 0, size),
		pairs: make([]pair, 0, len(m)),
	}

	for _, key := range keys {
		ls.append(key, m[key])
	}

	return ls
}

// LabelSetFromPairs is a short constructor for tests
func LabelSetFromPairs(kv ...string) LabelSet {
	if len(kv)%2 != 0 {
		panic("kv is not pairs")
	}

	idx := make([]int, len(kv)/2)
	for i := range idx {
		idx[i] = 2 * i
	}
	if len(idx) > 1 {
		sort.Slice(idx, func(i, j int) bool { return kv[idx[i]] < kv[idx[j]] })
	}

	var size int
	for i := range kv {
		size += len(kv[i]) + 1
	}

	ls := LabelSet{
		data:  make([]byte, 0, size),
		pairs: make([]pair, 0, len(kv)/2),
	}

	for _, i := range idx {
		ls.append(kv[i], kv[i+1])
	}

	return ls
}

// String return pairs in format k1:v1;k2:v2;
//
// Implements fmt.Stringer.
func (ls LabelSet) String() string {
	return *(*string)(unsafe.Pointer(&ls.data)) //nolint:gosec // memory and cpu optimization
}

// IsEmpty returns true if label set is empty
func (ls LabelSet) IsEmpty() bool {
	return ls.Len() == 0
}

// Get returns label value by key or default value if there is no key in label set
func (ls LabelSet) Get(key, defaultValue string) string {
	if n, ok := ls.get(key); ok {
		return ls.Value(n)
	}
	return defaultValue
}

// Len returns number of label pairs
func (ls LabelSet) Len() int {
	return len(ls.pairs)
}

// Key returns i-th key
func (ls LabelSet) Key(i int) string {
	return ls.pairs[i].key.reveal(ls.data)
}

// Value returns i-th value
func (ls LabelSet) Value(i int) string {
	return ls.pairs[i].value.reveal(ls.data)
}

// ToMap returns label pairs as map
func (ls LabelSet) ToMap() map[string]string {
	m := make(map[string]string, ls.Len())
	for i := 0; i < ls.Len(); i++ {
		m[ls.Key(i)] = ls.Value(i)
	}
	return m
}

// With return LabelSet with label key-value
func (ls LabelSet) With(key, value string) LabelSet {
	i, ok := ls.get(key)
	if ok && ls.Value(i) == value {
		// key-value already equal given
		return ls
	}

	if ok {
		// we should replace value
		oldValue := ls.Value(i)
		d := int32(len(value) - len(oldValue))
		res := LabelSet{
			data:  make([]byte, 0, len(ls.data)+int(d)),
			pairs: make([]pair, 0, len(ls.pairs)),
		}
		res.appendFrom(ls, 0, i)
		res.append(key, value)
		res.appendFrom(ls, i+1, ls.Len())
		return res
	}

	// we should insert key and value
	d := int32(len(key) + len(value) + additionalSymbols)
	res := LabelSet{
		data:  make([]byte, 0, len(ls.data)+int(d)),
		pairs: make([]pair, 0, len(ls.pairs)+1),
	}
	res.appendFrom(ls, 0, i)
	res.append(key, value)
	res.appendFrom(ls, i, ls.Len())
	return res
}

// WithPairs returns result of merge label set with kv pairs
func (ls LabelSet) WithPairs(kv ...string) LabelSet {
	return ls.Merge(LabelSetFromPairs(kv...))
}

// Merge returns result of merge ls with updates
func (ls LabelSet) Merge(updates LabelSet) LabelSet {
	if updates.IsEmpty() {
		return ls
	}
	if updates.Len() == 1 {
		return ls.With(updates.Key(0), updates.Value(0))
	}
	walk := func(fn func(i int, set LabelSet)) (lsPos, updatesPos int) {
		for lsPos < ls.Len() && updatesPos < updates.Len() {
			switch strings.Compare(ls.Key(lsPos), updates.Key(updatesPos)) {
			case -1:
				fn(lsPos, ls)
				lsPos++
			case 0:
				lsPos++
				fallthrough
			case 1:
				fn(updatesPos, updates)
				updatesPos++
			}
		}
		return lsPos, updatesPos
	}
	size, n := 0, 0
	lsPos, updatesPos := walk(func(i int, set LabelSet) {
		size += set.keyLen(i) + set.valueLen(i) + additionalSymbols
		n++
	})
	if lsPos+1 < ls.Len() {
		size += len(ls.data) - int(ls.pairs[lsPos+1].key.begin)
		n += ls.Len() - lsPos
	}
	if updatesPos+1 < updates.Len() {
		size += len(updates.data) - int(updates.pairs[updatesPos+1].key.begin)
		n += updates.Len() - updatesPos
	}

	res := LabelSet{
		data:  make([]byte, 0, size),
		pairs: make([]pair, 0, n),
	}
	lsPos, updatesPos = walk(func(i int, set LabelSet) {
		res.append(set.Key(i), set.Value(i))
	})
	res.appendFrom(ls, lsPos, ls.Len())
	res.appendFrom(updates, updatesPos, updates.Len())
	return res
}

// SplitBy returns label set splited into 2 label sets:
// * extracted contains labels with given keys
// * rest all others
//
//nolint:gocyclo // this function will be removed
func (ls LabelSet) SplitBy(keys ...string) (extracted, rest LabelSet) {
	if len(keys) == 0 {
		return EmptyLabelSet(), ls
	}
	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	sort.Strings(sortedKeys)
	extracted = LabelSet{
		data:  make([]byte, 0, len(ls.data)),
		pairs: make([]pair, 0, len(ls.pairs)),
	}
	for i, j := 0, 0; i < ls.Len() && j < len(sortedKeys); {
		switch strings.Compare(ls.Key(i), sortedKeys[j]) {
		case -1:
			i++
		case 0:
			extracted.append(ls.Key(i), ls.Value(i))
			i++
			j++
		case 1:
			j++
		}
	}
	if len(extracted.data) == cap(extracted.data) {
		return extracted, EmptyLabelSet()
	}
	rest = LabelSet{
		data:  extracted.data[len(extracted.data):],
		pairs: extracted.pairs[len(extracted.pairs):],
	}
	i, j := 0, 0
	for i < ls.Len() && j < len(sortedKeys) {
		switch strings.Compare(ls.Key(i), sortedKeys[j]) {
		case -1:
			rest.append(ls.Key(i), ls.Value(i))
			i++
		case 0:
			i++
			j++
		case 1:
			j++
		}
	}
	rest.appendFrom(ls, i, ls.Len())
	return extracted, rest
}

// Without returns new label set without given keys
func (ls LabelSet) Without(keys ...string) LabelSet {
	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	sort.Strings(sortedKeys)
	res := LabelSet{
		data:  make([]byte, 0, len(ls.data)),
		pairs: make([]pair, 0, len(ls.pairs)),
	}
	i, j := 0, 0
	for i < ls.Len() && j < len(sortedKeys) {
		switch strings.Compare(ls.Key(i), sortedKeys[j]) {
		case -1:
			res.append(ls.Key(i), ls.Value(i))
			i++
		case 0:
			i++
			j++
		case 1:
			j++
		}
	}
	res.appendFrom(ls, i, ls.Len())
	return res
}

// MarshalJSON implements json.Marshaler
func (ls LabelSet) MarshalJSON() ([]byte, error) {
	w := &jwriter.Writer{}
	w.RawByte('{')
	first := true
	for i := range ls.pairs {
		if !first {
			w.RawByte(',')
		}
		w.String(ls.Key(i))
		w.RawByte(':')
		w.String(ls.Value(i))
		first = false
	}
	w.RawByte('}')
	return w.BuildBytes()
}

// UnmarshalJSON implements json.Unmarshaler
func (ls *LabelSet) UnmarshalJSON(data []byte) error {
	m := map[string]string{}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	*ls = LabelSetFromMap(m)
	return nil
}

// MarshalYAML implements yaml.Marshaler
func (ls LabelSet) MarshalYAML() (interface{}, error) {
	return ls.ToMap(), nil
}

// UnmarshalYAML implements yaml/v2.Unmarshaler (v3 compatible)
func (ls *LabelSet) UnmarshalYAML(unmarshal func(interface{}) error) error {
	m := map[string]string{}
	if err := unmarshal(&m); err != nil {
		return err
	}
	*ls = LabelSetFromMap(m)
	return nil
}

func (ls *LabelSet) append(key, value string) {
	dKey := delegatedStringView{int32(len(ls.data)), int32(len(key))}
	ls.data = append(ls.data, []byte(key)...)
	ls.data = append(ls.data, ':')

	dValue := delegatedStringView{int32(len(ls.data)), int32(len(value))}
	ls.data = append(ls.data, []byte(value)...)
	ls.data = append(ls.data, ';')

	ls.pairs = append(ls.pairs, pair{dKey, dValue})
}

// appendFrom appends items from other in half-interval [a; b)
func (ls *LabelSet) appendFrom(other LabelSet, a, b int) {
	if a >= b {
		return
	}

	begin := other.pairs[a].key.begin
	end := int32(len(other.data))
	if b < other.Len() {
		end = other.pairs[b].key.begin
	}
	delta := int32(len(ls.data)) - begin

	ls.data = append(ls.data, other.data[begin:end]...)

	i := len(ls.pairs)
	n := i + b - a
	ls.pairs = append(ls.pairs, other.pairs[a:b]...)
	for ; i < n; i++ {
		ls.pairs[i].shift(delta)
	}
}

func (ls LabelSet) get(key string) (int, bool) {
	cmp := func(i int) int {
		return strings.Compare(key, ls.Key(i))
	}
	return sort.Find(len(ls.pairs), cmp)
}

func (ls LabelSet) keyLen(i int) int {
	return int(ls.pairs[i].key.len)
}

func (ls LabelSet) valueLen(i int) int {
	return int(ls.pairs[i].value.len)
}

// LabelSetBuilder used for carry labels pairs
type LabelSetBuilder struct {
	pairs map[string]string
}

// NewLabelSetBuilder is a constructor
func NewLabelSetBuilder() *LabelSetBuilder {
	return &LabelSetBuilder{
		pairs: map[string]string{},
	}
}

// Build label set
func (builder *LabelSetBuilder) Build() LabelSet {
	return LabelSetFromMap(builder.pairs)
}

// Set key-value in label set
func (builder *LabelSetBuilder) Set(key, value string) *LabelSetBuilder {
	builder.pairs[key] = value
	return builder
}

// Delete key-value pair from label set
func (builder *LabelSetBuilder) Delete(keys ...string) *LabelSetBuilder {
	for _, key := range keys {
		delete(builder.pairs, key)
	}
	return builder
}

// NewWith clone builder and add given key-value
func (builder *LabelSetBuilder) NewWith(key, value string) *LabelSetBuilder {
	res := make(map[string]string, len(builder.pairs)+1)
	for k, v := range builder.pairs {
		res[k] = v
	}
	res[key] = value
	return &LabelSetBuilder{pairs: res}
}