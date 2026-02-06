package testutil

// Ptr returns a pointer to the given value.
func Ptr[T any](v T) *T {
	return &v
}

// Keys returns the keys of a map.
func Keys[K comparable, V any](m map[K]V) []K {
	result := make([]K, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}
