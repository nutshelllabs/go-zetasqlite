package internal

import (
	"fmt"
	"time"

	"github.com/goccy/go-zetasql/types"
)

type SQLiteFunction func(...interface{}) (interface{}, error)
type BindFunction func(...Value) (Value, error)
type AggregateBindFunction func(ReturnValueConverter) func() *Aggregator
type WindowBindFunction func(ReturnValueConverter) func() *WindowAggregator

type FuncInfo struct {
	Name        string
	BindFunc    BindFunction
	ReturnTypes []types.TypeKind
}

type AggregateFuncInfo struct {
	Name        string
	BindFunc    AggregateBindFunction
	ReturnTypes []types.TypeKind
}

type WindowFuncInfo struct {
	Name        string
	BindFunc    WindowBindFunction
	ReturnTypes []types.TypeKind
}

func existsNull(args []Value) bool {
	for _, v := range args {
		if v == nil {
			return true
		}
	}
	return false
}

func convertArgs(args ...interface{}) ([]Value, error) {
	values := make([]Value, 0, len(args))
	for _, arg := range args {
		v, err := ValueOf(arg)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, nil
}

func bindIntFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToInt64()
	}
}

func bindFloatFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToFloat64()
	}
}

func bindStringFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToString()
	}
}

func bindBytesFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToString()
	}
}

func bindBoolFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToBool()
	}
}

func bindDateFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToString()
	}
}

func bindDatetimeFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToString()
	}
}

func bindTimeFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToString()
	}
}

func bindTimestampFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToString()
	}
}

func bindJsonFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToString()
	}
}

func bindArrayFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToString()
	}
}

func bindStructFunc(fn BindFunction) SQLiteFunction {
	return func(args ...interface{}) (interface{}, error) {
		values, err := convertArgs(args...)
		if err != nil {
			return nil, err
		}
		ret, err := fn(values...)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, nil
		}
		return ret.ToString()
	}
}

type ReturnValueConverter func(Value) (interface{}, error)

var (
	intValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToInt64()
	}
	floatValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToFloat64()
	}
	stringValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToString()
	}
	boolValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToBool()
	}
	dateValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToString()
	}
	datetimeValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToString()
	}
	timeValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToString()
	}
	timestampValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToString()
	}
	jsonValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToString()
	}
	arrayValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToString()
	}
	structValueConverter = func(v Value) (interface{}, error) {
		if v == nil {
			return nil, nil
		}
		return v.ToString()
	}
)

func bindAggregateIntFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(intValueConverter)
}

func bindAggregateFloatFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(floatValueConverter)
}

func bindAggregateStringFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(stringValueConverter)
}

func bindAggregateBytesFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(stringValueConverter)
}

func bindAggregateBoolFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(boolValueConverter)
}

func bindAggregateDateFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(dateValueConverter)
}

func bindAggregateDatetimeFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(datetimeValueConverter)
}

func bindAggregateTimeFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(timeValueConverter)
}

func bindAggregateTimestampFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(timestampValueConverter)
}

func bindAggregateJsonFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(jsonValueConverter)
}

func bindAggregateArrayFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(arrayValueConverter)
}

func bindAggregateStructFunc(bindFunc func(ReturnValueConverter) func() *Aggregator) func() *Aggregator {
	return bindFunc(structValueConverter)
}

type Aggregator struct {
	distinctMap map[string]struct{}
	step        func([]Value, *AggregatorOption) error
	done        func() (Value, error)
	converter   ReturnValueConverter
}

func (a *Aggregator) Step(stepArgs ...interface{}) error {
	args, opt, err := parseAggregateOptions(stepArgs...)
	if err != nil {
		return err
	}
	values, err := convertArgs(args...)
	if err != nil {
		return err
	}
	if opt.IgnoreNulls {
		filtered := []Value{}
		for _, v := range values {
			if v == nil {
				continue
			}
			filtered = append(filtered, v)
		}
		values = filtered
		if len(values) == 0 {
			return nil
		}
	}
	if opt.Distinct {
		if len(values) < 1 {
			return fmt.Errorf("DISTINCT option required at least one argument")
		}
		if values[0] == nil {
			// if value is nil, ignore it.
			return nil
		}
		key, err := values[0].ToString()
		if err != nil {
			return err
		}
		if _, exists := a.distinctMap[key]; exists {
			return nil
		}
		a.distinctMap[key] = struct{}{}
	}
	return a.step(values, opt)
}

func (a *Aggregator) Done() (interface{}, error) {
	ret, err := a.done()
	if err != nil {
		return nil, err
	}
	return a.converter(ret)
}

func newAggregator(
	step func([]Value, *AggregatorOption) error,
	done func() (Value, error),
	converter ReturnValueConverter) *Aggregator {
	return &Aggregator{
		distinctMap: map[string]struct{}{},
		step:        step,
		done:        done,
		converter:   converter,
	}
}

func bindWindowIntFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(intValueConverter)
}

func bindWindowFloatFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(floatValueConverter)
}

func bindWindowStringFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(stringValueConverter)
}

func bindWindowBytesFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(stringValueConverter)
}

func bindWindowBoolFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(boolValueConverter)
}

func bindWindowDateFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(dateValueConverter)
}

func bindWindowDatetimeFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(datetimeValueConverter)
}

func bindWindowTimeFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(timeValueConverter)
}

func bindWindowTimestampFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(timestampValueConverter)
}

func bindWindowJsonFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(jsonValueConverter)
}

func bindWindowArrayFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(arrayValueConverter)
}

func bindWindowStructFunc(bindFunc func(ReturnValueConverter) func() *WindowAggregator) func() *WindowAggregator {
	return bindFunc(structValueConverter)
}

type WindowAggregator struct {
	distinctMap map[string]struct{}
	agg         *WindowFuncAggregatedStatus
	step        func([]Value, *AggregatorOption, *WindowFuncStatus, *WindowFuncAggregatedStatus) error
	done        func(*WindowFuncAggregatedStatus) (Value, error)
	converter   ReturnValueConverter
}

func (a *WindowAggregator) Step(stepArgs ...interface{}) error {
	args, opt, err := parseAggregateOptions(stepArgs...)
	if err != nil {
		return err
	}
	newArgs, windowOpt, err := parseWindowOptions(args...)
	if err != nil {
		return err
	}
	values, err := convertArgs(newArgs...)
	if err != nil {
		return err
	}
	if opt.IgnoreNulls {
		filtered := []Value{}
		for _, v := range values {
			if v == nil {
				continue
			}
			filtered = append(filtered, v)
		}
		values = filtered
	}
	if opt.Distinct {
		if len(values) < 1 {
			return fmt.Errorf("DISTINCT option required at least one argument")
		}
		key, err := values[0].ToString()
		if err != nil {
			return err
		}
		if _, exists := a.distinctMap[key]; exists {
			return nil
		}
		a.distinctMap[key] = struct{}{}
	}
	return a.step(values, opt, windowOpt, a.agg)
}

func (a *WindowAggregator) Done() (interface{}, error) {
	ret, err := a.done(a.agg)
	if err != nil {
		return nil, err
	}
	return a.converter(ret)
}

func newWindowAggregator(
	step func([]Value, *AggregatorOption, *WindowFuncStatus, *WindowFuncAggregatedStatus) error,
	done func(*WindowFuncAggregatedStatus) (Value, error),
	converter ReturnValueConverter) *WindowAggregator {
	return &WindowAggregator{
		distinctMap: map[string]struct{}{},
		agg:         newWindowFuncAggregatedStatus(),
		step:        step,
		done:        done,
		converter:   converter,
	}
}

func bindAdd(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ADD: invalid argument num %d", len(args))
	}
	return ADD(args[0], args[1])
}

func bindSub(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("SUB: invalid argument num %d", len(args))
	}
	return SUB(args[0], args[1])
}

func bindMul(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("MUL: invalid argument num %d", len(args))
	}
	return MUL(args[0], args[1])
}

func bindOpDiv(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("OP_DIV: invalid argument num %d", len(args))
	}
	return OP_DIV(args[0], args[1])
}

func bindEqual(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("EQ: invalid argument num %d", len(args))
	}
	return EQ(args[0], args[1])
}

func bindNotEqual(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("NOT_EQ: invalid argument num %d", len(args))
	}
	return NOT_EQ(args[0], args[1])
}

func bindGreater(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("GT: invalid argument num %d", len(args))
	}
	return GT(args[0], args[1])
}

func bindGreaterOrEqual(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("GT: invalid argument num %d", len(args))
	}
	return GTE(args[0], args[1])
}

func bindLess(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("LT: invalid argument num %d", len(args))
	}
	return LT(args[0], args[1])
}

func bindLessOrEqual(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("LTE: invalid argument num %d", len(args))
	}
	return LTE(args[0], args[1])
}

func bindBitNot(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("BIT_NOT: invalid argument num %d", len(args))
	}
	return BIT_NOT(args[0])
}

func bindBitLeftShift(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("BIT_LEFT_SHIFT: invalid argument num %d", len(args))
	}
	return BIT_LEFT_SHIFT(args[0], args[1])
}

func bindBitRightShift(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("BIT_RIGHT_SHIFT: invalid argument num %d", len(args))
	}
	return BIT_RIGHT_SHIFT(args[0], args[1])
}

func bindBitAnd(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("BIT_AND: invalid argument num %d", len(args))
	}
	return BIT_AND(args[0], args[1])
}

func bindBitOr(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("BIT_OR: invalid argument num %d", len(args))
	}
	return BIT_OR(args[0], args[1])
}

func bindBitXor(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("BIT_XOR: invalid argument num %d", len(args))
	}
	return BIT_XOR(args[0], args[1])
}

func bindInArray(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ARRAY_IN: invalid argument num %d", len(args))
	}
	return ARRAY_IN(args[0], args[1])
}

func bindStructField(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("STRUCT_FIELD: invalid argument num %d", len(args))
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return STRUCT_FIELD(args[0], int(i64))
}

func bindArrayAtOffset(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ARRAY_OFFSET: invalid argument num %d", len(args))
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return ARRAY_OFFSET(args[0], int(i64))
}

func bindSafeArrayAtOffset(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ARRAY_SAFE_OFFSET: invalid argument num %d", len(args))
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return ARRAY_SAFE_OFFSET(args[0], int(i64))
}

func bindArrayAtOrdinal(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ARRAY_ORDINAL: invalid argument num %d", len(args))
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return ARRAY_ORDINAL(args[0], int(i64))
}

func bindSafeArrayAtOrdinal(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ARRAY_SAFE_ORDINAL: invalid argument num %d", len(args))
	}
	i64, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return ARRAY_SAFE_ORDINAL(args[0], int(i64))
}

func bindIsDistinctFrom(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("IS_DISTINCT_FROM: invalid argument num %d", len(args))
	}
	return IS_DISTINCT_FROM(args[0], args[1])
}

func bindIsNotDistinctFrom(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("IS_NOT_DISTINCT_FROM: invalid argument num %d", len(args))
	}
	return IS_NOT_DISTINCT_FROM(args[0], args[1])
}

func bindExtract(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("EXTRACT: invalid argument num %d", len(args))
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return EXTRACT(t, part)
}

func bindGenerateUUID(args ...Value) (Value, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("GENERATE_UUID: invalid argument num %d", len(args))
	}
	return GENERATE_UUID()
}

func bindLike(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("LIKE: invalid argument num %d", len(args))
	}
	return LIKE(args[0], args[1])
}

func bindBetween(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("BETWEEN: invalid argument num %d", len(args))
	}
	return BETWEEN(args[0], args[1], args[2])
}

func bindIn(args ...Value) (Value, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("IN: invalid argument num %d", len(args))
	}
	return IN(args[0], args[1:]...)
}

func bindIsNull(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("IS_NULL: invalid argument num %d", len(args))
	}
	return IS_NULL(args[0])
}

func bindIsTrue(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("IS_TRUE: invalid argument num %d", len(args))
	}
	return IS_TRUE(args[0])
}

func bindIsFalse(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("IS_FALSE: invalid argument num %d", len(args))
	}
	return IS_FALSE(args[0])
}

func bindNot(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("NOT: invalid argument num %d", len(args))
	}
	return NOT(args[0])
}

func bindAnd(args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("AND: invalid argument num %d", len(args))
	}
	return AND(args...)
}

func bindOr(args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("OR: invalid argument num %d", len(args))
	}
	return OR(args...)
}

func bindCaseWithValue(args ...Value) (Value, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("CASE_WITH_VALUE: invalid argument num %d", len(args))
	}
	return CASE_WITH_VALUE(args[0], args[1:]...)
}

func bindCaseNoValue(args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("CASE_NO_VALUE: invalid argument num %d", len(args))
	}
	return CASE_NO_VALUE(args...)
}

func bindCoalesce(args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("COALESCE: invalid argument num %d", len(args))
	}
	return COALESCE(args...)
}

func bindIf(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("IF: invalid argument num %d", len(args))
	}
	return IF(args[0], args[1], args[2])
}

func bindIfNull(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("IFNULL: invalid argument num %d", len(args))
	}
	return IFNULL(args[0], args[1])
}

func bindNullIf(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("NULLIF: invalid argument num %d", len(args))
	}
	return NULLIF(args[0], args[1])
}

func bindCast(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CAST: invalid argument num %d", len(args))
	}
	return args[0], nil
}

func bindCastBoolString(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CAST: invalid argument num %d", len(args))
	}
	b, err := args[0].ToBool()
	if err != nil {
		return nil, err
	}
	return StringValue(fmt.Sprintf("%t", b)), nil
}

func bindSafeCast(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SAFE_CAST: invalid argument num %d", len(args))
	}
	return &SafeValue{value: args[0]}, nil
}

func bindFarmFingerprint(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FARM_FINGERPRINT: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return FARM_FINGERPRINT(v)
}

func bindMD5(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("MD5: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return MD5(v)
}

func bindSha1(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SHA1: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return SHA1(v)
}

func bindSha256(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SHA256: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return SHA256(v)
}

func bindSha512(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SHA512: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return SHA512(v)
}

func timeFromUnixNano(unixNano int64) time.Time {
	return time.Unix(0, unixNano)
}

func bindAscii(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ASCII: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	ascii, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	if ascii == "" {
		return IntValue(0), nil
	}
	return ASCII(ascii)
}

func bindByteLength(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("BYTE_LENGTH: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return BYTE_LENGTH(v)
}

func bindCharLength(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CHAR_LENGTH: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return CHAR_LENGTH(v)
}

func bindChr(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CHR: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return CHR(v)
}

func bindCodePointsToBytes(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CODE_POINTS_TO_BYTES: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToArray()
	if err != nil {
		return nil, err
	}
	return CODE_POINTS_TO_BYTES(v)
}

func bindCodePointsToString(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CODE_POINTS_TO_STRING: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToArray()
	if err != nil {
		return nil, err
	}
	return CODE_POINTS_TO_STRING(v)
}

func bindCollate(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("COLLATE: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	if args[1] == nil {
		return nil, fmt.Errorf("COLLATE: collation_specification must be string literal")
	}
	value, err := args[0].ToString()
	if err != nil {
		return nil, fmt.Errorf("COLLATE: value must be string: %w", err)
	}
	spec, err := args[1].ToString()
	if err != nil {
		return nil, fmt.Errorf("COLLATE: collation_specification must be string literal: %w", err)
	}
	return COLLATE(value, spec)
}

func bindConcat(args ...Value) (Value, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("CONCAT: invalid argument num %d", len(args))
	}
	return CONCAT(args...)
}

func bindEndsWith(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ENDS_WITH: invalid argument num %d", len(args))
	}
	return ENDS_WITH(args[0], args[1])
}

func bindFormat(args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("FORMAT: invalid argument num %d", len(args))
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	if len(args) > 1 {
		return FORMAT(format, args[1:]...)
	}
	return FORMAT(format)
}

func bindFromBase32(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FROM_BASE32: invalid argument num %d", len(args))
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return FROM_BASE32(v)
}

func bindFromBase64(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FROM_BASE64: invalid argument num %d", len(args))
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return FROM_BASE64(v)
}

func bindFromHex(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FROM_HEX: invalid argument num %d", len(args))
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return FROM_HEX(v)
}

func bindInitcap(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("INITCAP: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	var delimiters []rune
	if len(args) == 2 {
		if args[1] == nil {
			return nil, nil
		}
		v, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		delimiters = []rune{}
		for _, vv := range v {
			delimiters = append(delimiters, vv)
		}
	}
	value, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return INITCAP(value, delimiters)
}

func bindInstr(args ...Value) (Value, error) {
	if len(args) != 2 && len(args) != 3 && len(args) != 4 {
		return nil, fmt.Errorf("INSTR: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	if args[1] == nil {
		return nil, nil
	}
	var (
		position   int64 = 0
		occurrence int64 = 1
	)
	if len(args) >= 3 {
		pos, err := args[2].ToInt64()
		if err != nil {
			return nil, err
		}
		position = pos
	}
	if len(args) == 4 {
		occur, err := args[3].ToInt64()
		if err != nil {
			return nil, err
		}
		occurrence = occur
	}
	return INSTR(args[0], args[1], position, occurrence)
}

func bindLeft(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("LEFT: invalid argument num %d", len(args))
	}
	length, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return LEFT(args[0], length)
}

func bindLength(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LENGTH: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return IntValue(0), nil
	}
	return LENGTH(args[0])
}

func bindLower(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOWER: invalid argument num %d", len(args))
	}
	return LOWER(args[0])
}

func bindLtrim(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("LTRIM: invalid argument num %d", len(args))
	}
	cutset := " "
	if len(args) == 2 {
		v, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		cutset = v
	}
	return LTRIM(args[0], cutset)
}

func bindStartsWith(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("STARTS_WITH: invalid argument num %d", len(args))
	}
	return STARTS_WITH(args[0], args[1])
}

func bindStrpos(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("STRPOS: invalid argument num %d", len(args))
	}
	return STRPOS(args[0], args[1])
}

func bindSubstr(args ...Value) (Value, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, fmt.Errorf("SUBSTR: invalid argument num %d", len(args))
	}
	pos, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	var length *int64
	if len(args) == 3 {
		v, err := args[2].ToInt64()
		if err != nil {
			return nil, err
		}
		length = &v
	}
	return SUBSTR(args[0], pos, length)
}

func bindToBase32(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TO_BASE32: invalid argument num %d", len(args))
	}
	b, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return TO_BASE32(b)
}

func bindToBase64(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TO_BASE64: invalid argument num %d", len(args))
	}
	b, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return TO_BASE64(b)
}

func bindToCodePoints(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TO_CODE_POINTS: invalid argument num %d", len(args))
	}
	return TO_CODE_POINTS(args[0])
}

func bindToHex(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TO_HEX: invalid argument num %d", len(args))
	}
	b, err := args[0].ToBytes()
	if err != nil {
		return nil, err
	}
	return TO_HEX(b)
}

func bindTranslate(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TRANSLATE: invalid argument num %d", len(args))
	}
	return TRANSLATE(args[0], args[1], args[2])
}

func bindTrim(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("TRIM: invalid argument num %d", len(args))
	}
	if len(args) == 2 {
		return TRIM(args[0], args[1])
	}
	return TRIM(args[0], nil)
}

func bindUnicode(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UNICODE: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	v, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return UNICODE(v)
}

func bindUpper(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UPPER: invalid argument num %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	return UPPER(args[0])
}

func bindToJson(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("TO_JSON: invalid argument num %d", len(args))
	}
	var stringifyWideNumbers bool
	if len(args) == 2 {
		b, err := args[1].ToBool()
		if err != nil {
			return nil, err
		}
		stringifyWideNumbers = b
	}
	return TO_JSON(args[0], stringifyWideNumbers)
}

func bindToJsonString(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("TO_JSON_STRING: invalid argument num %d", len(args))
	}
	var prettyPrint bool
	if len(args) == 2 {
		b, err := args[1].ToBool()
		if err != nil {
			return nil, err
		}
		prettyPrint = b
	}
	return TO_JSON_STRING(args[0], prettyPrint)
}

func bindBool(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("BOOL: invalid argument num %d", len(args))
	}
	return args[0], nil
}

func bindInt64(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("INT64: invalid argument num %d", len(args))
	}
	return args[0], nil
}

func bindDouble(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("FLOAT64: invalid argument num %d", len(args))
	}
	mode, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	switch mode {
	case "exact":
		return args[0], nil
	case "round":
		return args[0], nil
	}
	return nil, fmt.Errorf("unexpected wide_number_mode: %s", mode)
}

func bindJsonType(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("JSON_TYPE: invalid argument num %d", len(args))
	}
	value, ok := args[0].(JsonValue)
	if !ok {
		return nil, fmt.Errorf("JSON_TYPE: failed to convert %T to JSON value", args[0])
	}
	return JSON_TYPE(value)
}

func bindAbs(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ABS: invalid argument num %d", len(args))
	}
	return ABS(args[0])
}

func bindSign(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SIGN: invalid argument num %d", len(args))
	}
	return SIGN(args[0])
}

func bindIsInf(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("IS_INF: invalid argument num %d", len(args))
	}
	return IS_INF(args[0])
}

func bindIsNaN(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("IS_NAN: invalid argument num %d", len(args))
	}
	return IS_NAN(args[0])
}

func bindIEEEDivide(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("IEEE_DIVIDE: invalid argument num %d", len(args))
	}
	return IEEE_DIVIDE(args[0], args[1])
}

func bindRand(args ...Value) (Value, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("RAND: invalid argument num %d", len(args))
	}
	return RAND()
}

func bindSqrt(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SQRT: invalid argument num %d", len(args))
	}
	return SQRT(args[0])
}

func bindPow(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("POW(ER): invalid argument num %d", len(args))
	}
	return POW(args[0], args[1])
}

func bindExp(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("EXP: invalid argument num %d", len(args))
	}
	return EXP(args[0])
}

func bindLn(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LN: invalid argument num %d", len(args))
	}
	return LN(args[0])
}

func bindLog(args ...Value) (Value, error) {
	if len(args) == 1 {
		return LN(args[0])
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("LOG: invalid argument num %d", len(args))
	}
	return LOG(args[0], args[1])
}

func bindLog10(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOG10: invalid argument num %d", len(args))
	}
	return LOG10(args[0])
}

func bindGreatest(args ...Value) (Value, error) {
	return GREATEST(args...)
}

func bindLeast(args ...Value) (Value, error) {
	return LEAST(args...)
}

func bindDiv(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("DIV: invalid argument num %d", len(args))
	}
	return DIV(args[0], args[1])
}

func bindSafeDivide(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("SAFE_DIVIDE: invalid argument num %d", len(args))
	}
	return SAFE_DIVIDE(args[0], args[1])
}

func bindSafeMultiply(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("SAFE_MULTIPLY: invalid argument num %d", len(args))
	}
	return SAFE_MULTIPLY(args[0], args[1])
}

func bindSafeNegate(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SAFE_NEGATE: invalid argument num %d", len(args))
	}
	return SAFE_NEGATE(args[0])
}

func bindSafeAdd(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("SAFE_ADD: invalid argument num %d", len(args))
	}
	return SAFE_ADD(args[0], args[1])
}

func bindSafeSubtract(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("SAFE_SUBTRACT: invalid argument num %d", len(args))
	}
	return SAFE_SUBTRACT(args[0], args[1])
}

func bindMod(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("MOD: invalid argument num %d", len(args))
	}
	return MOD(args[0], args[1])
}

func bindRound(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ROUND: invalid argument num %d", len(args))
	}
	return ROUND(args[0])
}

func bindTrunc(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TRUNC: invalid argument num %d", len(args))
	}
	return TRUNC(args[0])
}

func bindCeil(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CEIL(ING): invalid argument num %d", len(args))
	}
	return CEIL(args[0])
}

func bindFloor(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FLOOR: invalid argument num %d", len(args))
	}
	return FLOOR(args[0])
}

func bindCos(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("COS: invalid argument num %d", len(args))
	}
	return COS(args[0])
}

func bindCosh(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("COSH: invalid argument num %d", len(args))
	}
	return COSH(args[0])
}

func bindAcos(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ACOS: invalid argument num %d", len(args))
	}
	return ACOS(args[0])
}

func bindAcosh(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ACOSH: invalid argument num %d", len(args))
	}
	return ACOSH(args[0])
}

func bindSin(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SIN: invalid argument num %d", len(args))
	}
	return SIN(args[0])
}

func bindSinh(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SINH: invalid argument num %d", len(args))
	}
	return SINH(args[0])
}

func bindAsin(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ASIN: invalid argument num %d", len(args))
	}
	return ASIN(args[0])
}

func bindAsinh(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ASINH: invalid argument num %d", len(args))
	}
	return ASINH(args[0])
}

func bindTan(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TAN: invalid argument num %d", len(args))
	}
	return TAN(args[0])
}

func bindTanh(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TANH: invalid argument num %d", len(args))
	}
	return TANH(args[0])
}

func bindAtan(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ATAN: invalid argument num %d", len(args))
	}
	return ATAN(args[0])
}

func bindAtanh(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ATANH: invalid argument num %d", len(args))
	}
	return ATANH(args[0])
}

func bindAtan2(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ATAN2: invalid argument num %d", len(args))
	}
	return ATAN2(args[0], args[1])
}

func bindRangeBucket(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("RANGE_BUCKET: invalid argument num %d", len(args))
	}
	array, err := args[1].ToArray()
	if err != nil {
		return nil, err
	}
	return RANGE_BUCKET(args[0], array)
}

func bindCurrentDate(args ...Value) (Value, error) {
	if len(args) == 0 {
		return CURRENT_DATE("")
	}
	if len(args) == 2 {
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		zone, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		loc, err := time.LoadLocation(zone)
		if err != nil {
			return nil, err
		}
		return CURRENT_DATE_WITH_TIME(timeFromUnixNano(unixNano).In(loc))
	}
	switch args[0].(type) {
	case IntValue:
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		return CURRENT_DATE_WITH_TIME(timeFromUnixNano(unixNano))
	case StringValue:
		zone, err := args[0].ToString()
		if err != nil {
			return nil, err
		}
		return CURRENT_DATE(zone)
	}
	return nil, fmt.Errorf("CURRENT_DATE: unexpected argument type %T", args[0])
}

func bindDate(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return DATE(args...)
}

func bindDateAdd(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATE_ADD: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return DATE_ADD(t, num, part)
}

func bindDateSub(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATE_SUB: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return DATE_SUB(t, num, part)
}

func bindDateDiff(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATE_DIFF: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	t2, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return DATE_DIFF(t, t2, part)
}

func bindDateTrunc(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("DATE_TRUNC: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return DATE_TRUNC(t, part)
}

func bindDateFromUnixDate(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("DATE_FROM_UNIX_DATE: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	unixdate, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return DATE_FROM_UNIX_DATE(unixdate)
}

func bindFormatDate(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("FORMAT_DATE: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	t, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	return FORMAT_DATE(format, t)
}

func bindLastDay(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("LAST_DAY: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	var part = "MONTH"
	if len(args) == 2 {
		p, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		part = p
	}
	return LAST_DAY(t, part)
}

func bindParseDate(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("PARSE_DATE: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	target, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return PARSE_DATE(format, target)
}

func bindUnixDate(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UNIX_DATE: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	return UNIX_DATE(t)
}

func bindCurrentDatetime(args ...Value) (Value, error) {
	if len(args) == 0 {
		return CURRENT_DATETIME("")
	}
	if len(args) == 2 {
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		zone, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		loc, err := time.LoadLocation(zone)
		if err != nil {
			return nil, err
		}
		return CURRENT_DATETIME_WITH_TIME(timeFromUnixNano(unixNano).In(loc))
	}
	switch args[0].(type) {
	case IntValue:
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		return CURRENT_DATETIME_WITH_TIME(timeFromUnixNano(unixNano))
	case StringValue:
		zone, err := args[0].ToString()
		if err != nil {
			return nil, err
		}
		return CURRENT_DATETIME(zone)
	}
	return nil, fmt.Errorf("CURRENT_DATETIME: unexpected argument type %T", args[0])
}

func bindDatetime(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return DATETIME(args...)
}

func bindDatetimeAdd(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATETIME_ADD: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return DATETIME_ADD(t, num, part)
}

func bindDatetimeSub(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATETIME_SUB: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return DATETIME_SUB(t, num, part)
}

func bindDatetimeDiff(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATETIME_DIFF: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	t2, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return DATETIME_DIFF(t, t2, part)
}

func bindDatetimeTrunc(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("DATETIME_TRUNC: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return DATETIME_TRUNC(t, part)
}

func bindFormatDatetime(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("FORMAT_DATETIME: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	t, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	return FORMAT_DATETIME(format, t)
}

func bindParseDatetime(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("PARSE_DATETIME: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	target, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return PARSE_DATETIME(format, target)
}

func bindCurrentTime(args ...Value) (Value, error) {
	if len(args) == 0 {
		return CURRENT_TIME("")
	}
	if len(args) == 2 {
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		zone, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		loc, err := time.LoadLocation(zone)
		if err != nil {
			return nil, err
		}
		return CURRENT_TIME_WITH_TIME(timeFromUnixNano(unixNano).In(loc))
	}
	switch args[0].(type) {
	case IntValue:
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		return CURRENT_TIME_WITH_TIME(timeFromUnixNano(unixNano))
	case StringValue:
		zone, err := args[0].ToString()
		if err != nil {
			return nil, err
		}
		return CURRENT_TIME(zone)
	}
	return nil, fmt.Errorf("CURRENT_TIME: unexpected argument type %T", args[0])
}

func bindTime(args ...Value) (Value, error) {
	if existsNull(args) {
		return nil, nil
	}
	return TIME(args...)
}

func bindTimeAdd(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TIME_ADD: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return TIME_ADD(t, num, part)
}

func bindTimeSub(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TIME_SUB: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return TIME_SUB(t, num, part)
}

func bindTimeDiff(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TIME_DIFF: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	t2, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return TIME_DIFF(t, t2, part)
}

func bindTimeTrunc(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("TIME_TRUNC: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return TIME_TRUNC(t, part)
}

func bindFormatTime(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("FORMAT_TIME: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	t, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	return FORMAT_TIME(format, t)
}

func bindParseTime(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("PARSE_TIME: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	target, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return PARSE_TIME(format, target)
}

func bindCurrentTimestamp(args ...Value) (Value, error) {
	if len(args) == 0 {
		return CURRENT_TIMESTAMP("")
	}
	if len(args) == 2 {
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		zone, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		loc, err := time.LoadLocation(zone)
		if err != nil {
			return nil, err
		}
		return CURRENT_TIMESTAMP_WITH_TIME(timeFromUnixNano(unixNano).In(loc))
	}
	switch args[0].(type) {
	case IntValue:
		unixNano, err := args[0].ToInt64()
		if err != nil {
			return nil, err
		}
		return CURRENT_TIMESTAMP_WITH_TIME(timeFromUnixNano(unixNano))
	case StringValue:
		zone, err := args[0].ToString()
		if err != nil {
			return nil, err
		}
		return CURRENT_TIMESTAMP(zone)
	}
	return nil, fmt.Errorf("CURRENT_TIMESTAMP: unexpected argument type %T", args[0])
}

func bindString(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("STRING: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	jsonValue, ok := args[0].(JsonValue)
	if ok {
		return StringValue(fmt.Sprint(jsonValue.Interface())), nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	var zone string
	if len(args) == 2 {
		z, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		zone = z
	}
	return STRING(t, zone)
}

func bindTimestamp(args ...Value) (Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, fmt.Errorf("TIMESTAMP: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	var zone string
	if len(args) == 2 {
		z, err := args[1].ToString()
		if err != nil {
			return nil, err
		}
		zone = z
	}
	return TIMESTAMP(args[0], zone)
}

func bindTimestampAdd(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TIMESTAMP_ADD: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return TIMESTAMP_ADD(t, num, part)
}

func bindTimestampSub(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TIMESTAMP_SUB: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	num, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return TIMESTAMP_SUB(t, num, part)
}

func bindTimestampDiff(args ...Value) (Value, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("TIMESTAMP_DIFF: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	t2, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[2].ToString()
	if err != nil {
		return nil, err
	}
	return TIMESTAMP_DIFF(t, t2, part)
}

func bindTimestampTrunc(args ...Value) (Value, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, fmt.Errorf("TIMESTAMP_TRUNC: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	part, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	var zone string
	if len(args) == 3 {
		z, err := args[2].ToString()
		if err != nil {
			return nil, err
		}
		zone = z
	}
	return TIMESTAMP_TRUNC(t, part, zone)
}

func bindFormatTimestamp(args ...Value) (Value, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, fmt.Errorf("FORMAT_TIMESTAMP: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	t, err := args[1].ToTime()
	if err != nil {
		return nil, err
	}
	var zone string
	if len(args) == 3 {
		z, err := args[2].ToString()
		if err != nil {
			return nil, err
		}
		zone = z
	}
	return FORMAT_TIMESTAMP(format, t, zone)
}

func bindParseTimestamp(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("PARSE_TIMESTAMP: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	format, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	target, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	return PARSE_TIMESTAMP(format, target)
}

func bindTimestampSeconds(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TIMESTAMP_SECONDS: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	sec, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return TIMESTAMP_SECONDS(sec)
}

func bindTimestampMillis(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TIMESTAMP_MILLIS: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	millisec, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return TIMESTAMP_MILLIS(millisec)
}

func bindTimestampMicros(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TIMESTAMP_MICROS: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	microsec, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return TIMESTAMP_MICROS(microsec)
}

func bindUnixSeconds(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UNIX_SECONDS: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	return UNIX_SECONDS(t)
}

func bindUnixMillis(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UNIX_MILLIS: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	return UNIX_MILLIS(t)
}

func bindUnixMicros(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UNIX_MICROS: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	t, err := args[0].ToTime()
	if err != nil {
		return nil, err
	}
	return UNIX_MICROS(t)
}

func bindDecodeArray(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("DECODE_ARRAY: invalid argument num %d", len(args))
	}
	if existsNull(args) {
		return nil, nil
	}
	s, err := args[0].ToString()
	if err != nil {
		return nil, err
	}
	return DECODE_ARRAY(s)
}

func bindArrayConcat(args ...Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("ARRAY_CONCAT: required arguments")
	}
	return ARRAY_CONCAT(args...)
}

func bindArrayLength(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ARRAY_LENGTH: invalid argument num %d", len(args))
	}
	arr, err := args[0].ToArray()
	if err != nil {
		return nil, err
	}
	return ARRAY_LENGTH(arr)
}

func bindArrayToString(args ...Value) (Value, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("ARRAY_TO_STRING: invalid argument num %d", len(args))
	}
	arr, err := args[0].ToArray()
	if err != nil {
		return nil, err
	}
	delim, err := args[1].ToString()
	if err != nil {
		return nil, err
	}
	if len(args) == 3 {
		nullText, err := args[2].ToString()
		if err != nil {
			return nil, err
		}
		return ARRAY_TO_STRING(arr, delim, nullText)
	}
	return ARRAY_TO_STRING(arr, delim)
}

func bindGenerateArray(args ...Value) (Value, error) {
	if len(args) != 3 && len(args) != 2 {
		return nil, fmt.Errorf("GENERATE_ARRAY: invalid argument num %d", len(args))
	}
	if len(args) == 3 {
		return GENERATE_ARRAY(args[0], args[1], args[2])
	}
	return GENERATE_ARRAY(args[0], args[1])
}

func bindGenerateDateArray(args ...Value) (Value, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("GENERATE_DATE_ARRAY: invalid argument num %d", len(args))
	}
	if len(args) == 2 {
		return GENERATE_DATE_ARRAY(args[0], args[1])
	}
	return GENERATE_DATE_ARRAY(args[0], args[1], args[2:]...)
}

func bindGenerateTimestampArray(args ...Value) (Value, error) {
	if len(args) != 4 {
		return nil, fmt.Errorf("GENERATE_TIMESTAMP_ARRAY: invalid argument num %d", len(args))
	}
	step, err := args[2].ToInt64()
	if err != nil {
		return nil, err
	}
	part, err := args[3].ToString()
	if err != nil {
		return nil, err
	}
	return GENERATE_TIMESTAMP_ARRAY(args[0], args[1], step, part)
}

func bindArrayReverse(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ARRAY_REVERSE: invalid argument num %d", len(args))
	}
	arr, err := args[0].ToArray()
	if err != nil {
		return nil, err
	}
	return ARRAY_REVERSE(arr)
}

func bindMakeArray(args ...Value) (Value, error) {
	return MAKE_ARRAY(args...)
}

func bindMakeStruct(args ...Value) (Value, error) {
	if len(args)%2 != 0 {
		return nil, fmt.Errorf("MAKE_STRUCT: unexpected argument num %d", len(args))
	}
	return MAKE_STRUCT(args...)
}

func bindDistinct(args ...Value) (Value, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("DISTINCT: invalid argument num %d", len(args))
	}
	return DISTINCT()
}

func bindLimit(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LIMIT: invalid argument num %d", len(args))
	}
	i64, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return LIMIT(i64)
}

func bindIgnoreNulls(args ...Value) (Value, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("IGNORE_NULLS: invalid argument num %d", len(args))
	}
	return IGNORE_NULLS()
}

func bindOrderBy(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ORDER_BY: invalid argument num %d", len(args))
	}
	b, err := args[1].ToBool()
	if err != nil {
		return nil, err
	}
	return ORDER_BY(args[0], b)
}

func bindWindowFrameUnit(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("WINDOW_FRAME_UNIT: invalid argument num %d", len(args))
	}
	i64, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return WINDOW_FRAME_UNIT(i64)
}

func bindWindowPartition(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("WINDOW_PARTITION: invalid argument num %d", len(args))
	}
	return WINDOW_PARTITION(args[0])
}

func bindWindowBoundaryStart(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("WINDOW_BOUNDARY_START: invalid argument num %d", len(args))
	}
	a0, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	a1, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return WINDOW_BOUNDARY_START(a0, a1)
}

func bindWindowBoundaryEnd(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("WINDOW_BOUNDARY_END: invalid argument num %d", len(args))
	}
	a0, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	a1, err := args[1].ToInt64()
	if err != nil {
		return nil, err
	}
	return WINDOW_BOUNDARY_END(a0, a1)
}

func bindWindowRowID(args ...Value) (Value, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("WINDOW_ROWID: invalid argument num %d", len(args))
	}
	a0, err := args[0].ToInt64()
	if err != nil {
		return nil, err
	}
	return WINDOW_ROWID(a0)
}

func bindWindowOrderBy(args ...Value) (Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("WINDOW_ORDER_BY: invalid argument num %d", len(args))
	}
	isAsc, err := args[1].ToBool()
	if err != nil {
		return nil, err
	}
	return WINDOW_ORDER_BY(args[0], isAsc)
}

func bindArrayAgg(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &ARRAY_AGG{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("ARRAY_AGG: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindArrayConcatAgg(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &ARRAY_CONCAT_AGG{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("ARRAY_CONCAT_AGG: invalid argument num %d", len(args))
				}
				if args[0] == nil {
					return nil
				}
				array, err := args[0].ToArray()
				if err != nil {
					return err
				}
				return fn.Step(array, opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindSum(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &SUM{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("SUM: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindAvg(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &AVG{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("AVG: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindCount(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &COUNT{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("COUNT: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindCountStar(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &COUNT_STAR{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 0 {
					return fmt.Errorf("COUNT_STAR: invalid argument num %d", len(args))
				}
				return fn.Step(opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindBitAndAgg(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &BIT_AND_AGG{IntValue(-1)}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("BIT_AND_AGG: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindBitOrAgg(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &BIT_OR_AGG{-1}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("BIT_OR_AGG: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindBitXorAgg(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &BIT_XOR_AGG{1}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("BIT_XOR_AGG: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindCountIf(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &COUNTIF{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("COUNT_IF: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindLogicalAnd(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &LOGICAL_AND{true}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("LOGICAL_AND: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindLogicalOr(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &LOGICAL_OR{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("LOGICAL_OR: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindMax(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &MAX{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("MAX: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindMin(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &MIN{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("MIN: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindStringAgg(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &STRING_AGG{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 && len(args) != 2 {
					return fmt.Errorf("STRING_AGG: invalid argument num %d", len(args))
				}
				if len(args) == 1 {
					return fn.Step(args[0], "", opt)
				}
				delim, err := args[1].ToString()
				if err != nil {
					return err
				}
				return fn.Step(args[0], delim, opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindArray(converter ReturnValueConverter) func() *Aggregator {
	return func() *Aggregator {
		fn := &ARRAY{}
		return newAggregator(
			func(args []Value, opt *AggregatorOption) error {
				if len(args) != 1 {
					return fmt.Errorf("ARRAY: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], opt)
			},
			func() (Value, error) {
				return fn.Done()
			},
			converter,
		)
	}
}

func bindWindowSum(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_SUM{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_SUM: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowMax(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_MAX{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_MAX: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowMin(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_MIN{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_MIN: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowCount(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_COUNT{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_COUNT: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowCountStar(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_COUNT_STAR{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 0 {
					return fmt.Errorf("WINDOW_COUNT_STAR: invalid argument num %d", len(args))
				}
				return fn.Step(windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowAvg(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_AVG{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_AVG: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowFirstValue(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_FIRST_VALUE{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_FIRST_VALUE: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowLastValue(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_LAST_VALUE{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_LAST_VALUE: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowLag(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_LAG{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 && len(args) != 2 && len(args) != 3 {
					return fmt.Errorf("WINDOW_LAG: invalid argument num %d", len(args))
				}
				var offset int64 = 1
				if len(args) >= 2 {
					v, err := args[1].ToInt64()
					if err != nil {
						return err
					}
					offset = v
				}
				if offset < 0 {
					return fmt.Errorf("WINDOW_LAG: offset is must be positive value %d", offset)
				}
				var defaultValue Value
				if len(args) == 3 {
					defaultValue = args[2]
				}
				return fn.Step(args[0], offset, defaultValue, windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowRank(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_RANK{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 0 {
					return fmt.Errorf("WINDOW_RANK: invalid argument num %d", len(args))
				}
				return fn.Step(windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowDenseRank(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_DENSE_RANK{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 0 {
					return fmt.Errorf("WINDOW_DENSE_RANK: invalid argument num %d", len(args))
				}
				return fn.Step(windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowRowNumber(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_ROW_NUMBER{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 0 {
					return fmt.Errorf("WINDOW_ROW_NUMBER: invalid argument num %d", len(args))
				}
				return fn.Step(windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowCorr(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_CORR{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 2 {
					return fmt.Errorf("WINDOW_CORR: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], args[1], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowCovarPop(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_COVAR_POP{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 2 {
					return fmt.Errorf("WINDOW_COVAR_POP: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], args[1], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowCovarSamp(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_COVAR_SAMP{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 2 {
					return fmt.Errorf("WINDOW_COVAR_SAMP: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], args[1], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowStdDevPop(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_STDDEV_POP{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_STDDEV_POP: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowStdDevSamp(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_STDDEV_SAMP{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_STDDEV_SAMP: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowStdDev(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_STDDEV_SAMP{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_STDDEV: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowVarPop(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_VAR_POP{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_VAR_POP: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowVarSamp(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_VAR_SAMP{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_VAR_SAMP: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}

func bindWindowVariance(converter ReturnValueConverter) func() *WindowAggregator {
	return func() *WindowAggregator {
		fn := &WINDOW_VAR_SAMP{}
		return newWindowAggregator(
			func(args []Value, opt *AggregatorOption, windowOpt *WindowFuncStatus, agg *WindowFuncAggregatedStatus) error {
				if len(args) != 1 {
					return fmt.Errorf("WINDOW_VARIANCE: invalid argument num %d", len(args))
				}
				return fn.Step(args[0], windowOpt, agg)
			},
			func(agg *WindowFuncAggregatedStatus) (Value, error) {
				return fn.Done(agg)
			},
			converter,
		)
	}
}
