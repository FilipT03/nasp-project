package util

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

var defaultConfig *Config = nil

func GetDefaultConfig() *Config {
	if defaultConfig == nil {
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
	return defaultConfig
}

func LoadConfig(path string) *Config {
	file, err := os.ReadFile(path)
	if err != nil {
		return GetDefaultConfig()
	}
	var config Config
	err = yaml.Unmarshal(file, &config)
	if err != nil {
		return GetDefaultConfig()
	}
	return &config
}
