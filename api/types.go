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
	EnableMultiNode    bool  `json:"enableMultiNode"`
	EnableErasure      bool  `json:"enableErasure"`
	EnableMultipart    bool  `json:"enableMultipart"`
	MultinodeChunkSize int64 `json:"multinodeChunkSize"`
	DataShard          int64 `json:"dataShard"`
	ParityShard        int64 `json:"parityShard"`
	MultipartChunkSize int64 `json:"multipartChunkSize"`
}
type PresignedItem struct {
	ID         string    `json:"id"`
	Index      int       `json:"index"`
	Size       int64     `json:"size"`
	RangeStart int64     `json:"rangeStart"`
	RangeEnd   int64     `json:"rangeEnd"`
	Presigned  Presigned `json:"presigned"`
}

type Presigned struct {
	Url     string              `json:"url"`
	Method  string              `json:"method"`
	Headers map[string][]string `json:"headers"`
}

type UploadNodesResponse struct {
	UploadId string          `json:"uploadId"`
	List     []PresignedItem `json:"list"`
	Config   UploadConfig    `json:"config"`
}

type CommitObjectReq struct {
	SessionID string  `json:"sessionId"` // 文件上传sessionKey
	ShardList []Shard `json:"shardList"`
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

type Shard struct {
	Index    int    `json:"index"`
	Status   int    `json:"status"`
	Size     uint64 `json:"size"`
	Hash     string `json:"hash"`
	HashType string `json:"hashType"`
	NodeID   string `json:"nodeID"`
	Message  string `json:"message"`
	Etag     string `json:"etag"`
}

type Fileinfo struct {
	Size     int64  `json:"size"`
	Hash     string `json:"hash"`
	HashType string `json:"hashType"`
}

type DownloadConfig struct {
	EnableMultiNode    bool  `json:"enableMultiNode"`    // 是否开启多节点存储
	EnableErasure      bool  `json:"enableErasure"`      // 多节点存储时，是否启用纠删码
	EnableMultipart    bool  `json:"enableMultipart"`    // 是否开启分片上传
	MultinodeChunkSize int64 `json:"multinodeChunkSize"` // 分节点存储时，每个节点存储数据的大小
	DataShard          int64 `json:"dataShard"`          // 数据分片数量
	ParityShard        int64 `json:"parityShard"`        // 校验分片数量
	MultipartChunkSize int64 `json:"multipartChunkSize"` // 分片大小
}

type DownloadNodesResponse struct {
	Fileinfo Fileinfo        `json:"fileinfo"`
	Shards   []PresignedItem `json:"shards"`
	Config   DownloadConfig  `json:"config"`
}

type HashCheckResponse struct {
	Match bool `json:"match"`
}
