package util

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	WAL         WALConfig         `yaml:"WAL"`
	Memtable    MemtableConfig    `yaml:"Memtable"`
	SSTable     SSTableConfig     `yaml:"SSTable"`
	LSMTree     LSMTreeConfig     `yaml:"LSMTree"`
	Cache       CacheConfig       `yaml:"Cache"`
	TokenBucket TokenBucketConfig `yaml:"TokenBucket"`
}

type WALConfig struct {
	SegmentSize   uint64 `yaml:"segmentSize"`
	BufferSize    int    `yaml:"bufferSize"`
	WALFolderPath string `yaml:"walFolderPath"`
}

type MemtableConfig struct {
	MaxSize   int            `yaml:"maxSize"`
	Structure string         `yaml:"structure"`
	Instances int            `yaml:"instances"`
	SkipList  SkipListConfig `yaml:"SkipList"`
	BTree     BTreeConfig    `yaml:"BTree"`
}

type BTreeConfig struct {
	MinSize int `yaml:"minSize"`
}

type SkipListConfig struct {
	MaxHeight int `yaml:"maxHeight"`
}

type SSTableConfig struct {
	SavePath            string  `yaml:"savePath"`
	SingleFile          bool    `yaml:"singleFile"`
	SummaryDegree       int     `yaml:"summaryDegree"`
	IndexDegree         int     `yaml:"indexDegree"`
	Compression         bool    `yaml:"compression"`
	FilterPrecision     float64 `yaml:"filterPrecision"`
	MerkleTreeChunkSize int64   `yaml:"merkleTreeChunkSize"`
}

type LSMTreeConfig struct {
	MaxLevel            int    `yaml:"maxLevel"`
	CompactionAlgorithm string `yaml:"compactionAlgorithm"`
	MaxLsmNodesPerLevel int    `yaml:"maxLsmNodesPerLevel"`
}

type CacheConfig struct {
	MaxSize uint64 `yaml:"maxSize"`
}

type TokenBucketConfig struct {
	MaxTokenSize int64 `yaml:"maxTokenSize"`
}

var config = &Config{
	WAL: WALConfig{
		SegmentSize:   32768,
		BufferSize:    8,
		WALFolderPath: "./wal",
	},
	Memtable: MemtableConfig{
		MaxSize:   1024,
		Structure: "SkipList",
		Instances: 1,
		BTree: BTreeConfig{
			MinSize: 16,
		},
		SkipList: SkipListConfig{
			MaxHeight: 32,
		},
	},
	SSTable: SSTableConfig{
		SavePath:            "./data",
		SingleFile:          false,
		SummaryDegree:       5,
		IndexDegree:         5,
		Compression:         true,
		FilterPrecision:     0.01,
		MerkleTreeChunkSize: 1024,
	},
	LSMTree: LSMTreeConfig{
		MaxLevel:            4,
		CompactionAlgorithm: "Size-Tiered",
		MaxLsmNodesPerLevel: 8,
	},
	Cache: CacheConfig{
		MaxSize: 1024,
	},
	TokenBucket: TokenBucketConfig{
		MaxTokenSize: 1024,
	},
}

// GetConfig returns config struct. Returns default config if LoadConfig is not called.
func GetConfig() *Config {
	return config
}

// LoadConfig loads config from path. If configuration does not exist, it sets a default value.
func LoadConfig(path string) *Config {
	file, err := os.ReadFile(path)
	if err != nil {
		return config
	}

	_ = yaml.Unmarshal(file, &config)
	return config
}

// SaveConfig saves config into `path`.
func SaveConfig(path string) {
	data, err := yaml.Marshal(&config)
	if err != nil {
		log.Fatal(err)
	}

	err = os.WriteFile(path, data, 0)
	if err != nil {
		log.Fatal(err)
	}
}
