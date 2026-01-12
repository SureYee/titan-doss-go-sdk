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
}

type Node struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
}

type UploadNodesResponse struct {
	Shards []Node        `json:"shards"`
	Config ErasureConfig `json:"config"`
}

type CommitObjectReq struct {
	Config    ErasureConfig `json:"config"`
	Bucket    string        `json:"bucket"`
	Key       string        `json:"key"`
	Size      uint64        `json:"size"`
	Hash      string        `json:"hash"`
	HashType  string        `json:"hashType"`
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
