package scheduler

type Response[T any] struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data T      `json:"data"`
}

type ErasureConfig struct {
	Encode      bool `json:"encode"`
	DataShard   int  `json:"dataShard"`
	ParityShard int  `json:"parityShard"`

	Partsize int64 `json:"partsize"`
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
	Config   ErasureConfig   `json:"config"`
}

type CommitObjectReq struct {
	Config    ErasureConfig `json:"config"`
	Bucket    string        `json:"bucket"`
	Key       string        `json:"key"`
	Size      uint64        `json:"size"`
	Hash      string        `json:"hash"`
	HashType  string        `json:"hashType"`
	PreHash   string        `json:"preHash"`
	PreSize   uint64        `json:"preSize"`
	UploadId  string        `json:"uploadId"`
	ObjectId  string        `json:"objectId"`
	ShardList []Shard       `json:"shardList"`
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
	Size  uint64 `json:"size"`
	Index int64  `json:"index"`
}

type HashCheckReq struct {
	RootHash string     `json:"rootHash"`
	HashType string     `json:"hashType"`
	LeafHash []LeafHash `json:"leafHash"`
}

type Part struct{}

type PresignMultipartResponse struct {
	ObjectID string `json:"objectId"`
	Parts    []Part `json:"parts"`
}
