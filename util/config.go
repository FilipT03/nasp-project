package util

// TODO: Add support for size-tiered, leveled and token bucket algorithms

import (
	"gopkg.in/yaml.v3"
	"os"
)

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

func getDefaultConfig() *Config {
	return &Config{
		WAL: WALConfig{
			SegmentSize: 512,
		},
		MemTable: MemTableConfig{
			MaxSize:   1024,
			Structure: "SkipList",
			Instances: 1,
		},
		SSTable: SSTableConfig{
			SavePath:      ".",
			SummeryDegree: 5,
			IndexDegree:   5,
			Compression:   true,
		},
		LMS: LMSConfig{
			MaxLevel: 4,
		},
		Cache: CacheConfig{
			MaxSize: 1024,
		},
	}
}

var config = getDefaultConfig()

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
	err = yaml.Unmarshal(file, &config)
	if err != nil {
		return config
	}

	return config
}
