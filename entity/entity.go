package entity

type HttpMethod int32

const (
	// HTTP POST
	HttpMethod_POST HttpMethod = 1
	// HTTP GET
	HttpMethod_GET HttpMethod = 2
	// HTTP HEAD
	HttpMethod_HEAD HttpMethod = 3
	// HTTP PUT
	HttpMethod_PUT HttpMethod = 4
	// HTTP DELETE
	HttpMethod_DELETE HttpMethod = 5
	// HTTP PATCH
	HttpMethod_PATCH HttpMethod = 6
	// HTTP OPTIONS
	HttpMethod_OPTIONS HttpMethod = 7
)

func (ht HttpMethod) Int() int32 {
	return int32(ht)
}
