package util

type Config struct {
	WAL      WALConfig      `yaml:"WAL"`
	MemTable MemTableConfig `yaml:"MemTable"`
	SSTable  SSTableConfig  `yaml:"SSTable"`
	LMS      LMSConfig      `yaml:"LMS"`
	Cache    CacheConfig    `yaml:"Cache"`
}

type WALConfig struct {
	SegmentSize int `yaml:"segmentSize"`
}

type MemTableConfig struct {
	MaxSize   int    `yaml:"maxSize"`
	Structure string `yaml:"structure"`
	Instances int    `yaml:"instances"`
}

type SSTableConfig struct {
	SavePath      string `yaml:"savePath"`
	SummeryDegree int    `yaml:"summeryDegree"`
	IndexDegree   int    `yaml:"indexDegree"`
	Compression   bool   `yaml:"compression"`
}

type LMSConfig struct {
	MaxLevel int `yaml:"maxLevel"`
}

type CacheConfig struct {
	MaxSize int `yaml:"maxSize"`
}
