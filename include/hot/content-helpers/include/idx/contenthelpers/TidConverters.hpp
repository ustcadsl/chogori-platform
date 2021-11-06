#ifndef __IDX__CONTENTHELPERS__TID_CONVERTERS__HPP__
#define __IDX__CONTENTHELPERS__TID_CONVERTERS__HPP__

/** @author robert.binna@uibk.ac.at */

#include <cstdint>
#include <log.h>
namespace idx { namespace contenthelpers {

/**
 * A TidToValueConverter converts a tuple identifier to its value representation
 * Depending on the value representation different template specializations exists.
 * In the default case the tuple is reinterpreted as a value.
 * Therefore, the default case is only suitable for ValueTypes which span 8 bytes.
 *
 * @tparam ValueType the type of the target value
 */
template<typename ValueType>
class TidToValueConverter {
public:
	__attribute__((always_inline)) inline ValueType operator()(intptr_t tid) {
		K2LOG_D(k2::log::hot, "TID {} ValueType {}", tid, typeid(ValueType).name());
		tid &= INTPTR_MAX;
		K2LOG_D(k2::log::hot, "TID {} VALUE {}", tid, *reinterpret_cast<ValueType *>(&tid));
		return *reinterpret_cast<ValueType *>(&tid);
	}
};

/**
 * A ValueToTidConverter Converts a given value to its tuple identifier
 * Depending on the value representation different template specializations exist.
 * In the default case the value is reinterpreted as a a tuple identifier.
 * This is only suitable for value types which sare shorter than 8 bytes.
 *
 * @tparam ValueType
 */
template<typename ValueType>
class TidToValueConverter<ValueType *> {
public:
	__attribute__((always_inline)) inline ValueType *operator()(intptr_t tid) {
		K2LOG_D(k2::log::hot, "Convert TID {} to valueType* {} value {}", (void*)tid, typeid(ValueType).name(), (void*)reinterpret_cast<ValueType *>(tid));
		return reinterpret_cast<ValueType *>(tid);
	}
};

template<typename ValueType>
class ValueToTidConverter {
public:
	__attribute__((always_inline)) inline intptr_t operator()(ValueType value) {
		K2LOG_D(k2::log::hot, "Convert ValueType {} to tid {}", value, *reinterpret_cast<intptr_t *>(&value));
		return *reinterpret_cast<intptr_t *>(&value);
	}
};

template<typename ValueType>
class ValueToTidConverter<ValueType *> {
public:
	__attribute__((always_inline)) inline intptr_t operator()(ValueType *value) {
		K2LOG_D(k2::log::hot, "Convert ValueType* {} to tid {}", (void*)value, (void*)reinterpret_cast<intptr_t>(value));
		return reinterpret_cast<intptr_t>(value);
	}
};

template<typename ValueType>
__attribute__((always_inline)) inline ValueType tidToValue(intptr_t tid) {
	TidToValueConverter<ValueType> convert;
	return convert(tid);
}

template<typename ValueType>
__attribute__((always_inline)) inline intptr_t valueToTid(ValueType value) {
	ValueToTidConverter<ValueType> convert;
	return convert(value);
}

} }

#endif