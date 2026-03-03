package api

type ResponseError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func (e *ResponseError) Error() string {
	return e.Msg
}

func NewResponseError(code int, msg string) *ResponseError {
	return &ResponseError{
		Code: code,
		Msg:  msg,
	}
}
