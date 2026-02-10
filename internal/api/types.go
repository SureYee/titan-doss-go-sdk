package api

type Response[T any] struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data T      `json:"data"`
}

type CreateUploadReq struct {
	Folder      int64  `json:"folder"`
	Filename    string `json:"filename"`
	ContentType string `json:"contentType"`
	Hash        string `json:"hash"` // 文件hash
	Size        uint64 `json:"size"` // 文件大小
}

type CreateUploadResp struct {
	SessionID string `json:"sessionId"` // 文件上传sessionKey
}

type UploadConfig struct {
	EnableMultiNode    bool  `json:"enable"`
	EnableErasure      bool  `json:"enableErasure"`
	EnableMultipart    bool  `json:"enableMultipart"`
	MultinodeChunkSize int64 `json:"multinodeChunkSize"`
	DataShard          int64 `json:"dataShard"`
	ParityShard        int64 `json:"parityShard"`
	MultipartChunkSize int64 `json:"multipartChunkSize"`
}
type PresignedItem struct {
	ID        string    `json:"id"`
	Presigned Presigned `json:"presigned"`
}

type Presigned struct {
	Url     string              `json:"url"`
	Method  string              `json:"method"`
	Headers map[string][]string `json:"headers"`
}

type UploadNodesResponse struct {
	UploadId string          `json:"uploadId"`
	ObjectId string          `json:"objectId"`
	List     []PresignedItem `json:"list"`
	Config   UploadConfig    `json:"config"`
}

type CommitObjectReq struct {
	SessionID string  `json:"sessionId"` // 文件上传sessionKey
	ShardList []Shard `json:"shardList"`
}

type PreCheckReq struct {
	SessionID string `json:"sessionId"` // 文件上传sessionKey
	PreHash   string `json:"preHash"`   // 文件预检hash
}

type PreCheckHashResponse struct {
	Match bool `json:"match"`
}

type LeafHash struct {
	Hash  string `json:"hash"`
	Index int64  `json:"index"`
}

type HashCheckReq struct {
	SessionID string     `json:"sessionId"` // 文件上传sessionKey
	RootHash  string     `json:"rootHash"`
	LeafHash  []LeafHash `json:"leafHash"`
}

type Part struct{}

type PresignMultipartResponse struct {
	ObjectID string `json:"objectId"`
	Parts    []Part `json:"parts"`
}
