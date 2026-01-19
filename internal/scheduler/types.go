package scheduler

type Response[T any] struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data T      `json:"data"`
}

type UploadConfig struct {
	EnableErasure       bool  `json:"enable"`
	ErasureDataShard    int64 `json:"erasureDataShard"`
	ErasureParityShard  int64 `json:"erasureParityShard"`
	EnableMultipart     bool  `json:"enableMultipart"`
	MultipartChunkSize  int64 `json:"multipartChunkSize"`
	MultipartChunkCount int64 `json:"multipartChunkCount"`
}

type CommitConfig struct {
	EnableMultipart     bool  `json:"enableMultipart"`
	MultipartChunkCount int64 `json:"multipartChunkCount"`
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
	Config     CommitConfig `json:"config"`
	Bucket     string       `json:"bucket"`
	Key        string       `json:"key"`
	Size       uint64       `json:"size"`
	MerkleHash string       `json:"merkleHash"`
	Hash       string       `json:"hash"`
	HashType   string       `json:"hashType"`
	PreHash    string       `json:"preHash"`
	PreSize    uint64       `json:"preSize"`
	UploadId   string       `json:"uploadId"`
	ObjectId   string       `json:"objectId"`
	ShardList  []Shard      `json:"shardList"`
}

type PreCheckReq struct {
	Hash     string `json:"hash"`
	HashType string `json:"hashType"`
	Size     int64  `json:"size"`
}

type PreCheckHashResponse struct {
	Match bool `json:"match"`
}

type LeafHash struct {
	Hash  string `json:"hash"`
	Index int64  `json:"index"`
}

type HashCheckReq struct {
	Hash     string     `json:"hash"`
	Bucket   string     `json:"bucket"`
	Key      string     `json:"key"`
	LeafHash []LeafHash `json:"leafHash"`
}

type Part struct{}

type PresignMultipartResponse struct {
	ObjectID string `json:"objectId"`
	Parts    []Part `json:"parts"`
}
