package util

import (
	"log"
	"os"
	"reflect"

	"github.com/go-playground/validator/v10"
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
	SegmentSize   uint64 `yaml:"segmentSize" validate:"gte=1"`
	BufferSize    int    `yaml:"bufferSize" validate:"gte=1"`
	WALFolderPath string `yaml:"walFolderPath"`
}

type MemtableConfig struct {
	MaxSize   int            `yaml:"maxSize" validate:"gte=1"`
	Structure string         `yaml:"structure" validate:"oneof=SkipList HashMap BTree"`
	Instances int            `yaml:"instances" validate:"gte=1"`
	SkipList  SkipListConfig `yaml:"SkipList"`
	BTree     BTreeConfig    `yaml:"BTree"`
}

type BTreeConfig struct {
	MinSize int `yaml:"minSize" validate:"gte=1"`
}

type SkipListConfig struct {
	MaxHeight int `yaml:"maxHeight" validate:"gte=1"`
}

type SSTableConfig struct {
	SavePath            string  `yaml:"savePath"`
	SingleFile          bool    `yaml:"singleFile"`
	SummaryDegree       int     `yaml:"summaryDegree" validate:"gte=1"`
	IndexDegree         int     `yaml:"indexDegree" validate:"gte=1"`
	Compression         bool    `yaml:"compression"`
	FilterPrecision     float64 `yaml:"filterPrecision" validate:"float_between"`
	MerkleTreeChunkSize int64   `yaml:"merkleTreeChunkSize" validate:"gte=1"`
	CompressionFilename string  `yaml:"compressionFilename"`
}

type LSMTreeConfig struct {
	MaxLevel            int              `yaml:"maxLevel" validate:"gte=1"`
	CompactionAlgorithm string           `yaml:"compactionAlgorithm" validate:"oneof=Size-Tiered Leveled"`
	SizeTiered          SizeTieredConfig `yaml:"SizeTiered"`
	Leveled             LeveledConfig    `yaml:"Leveled"`
}

type SizeTieredConfig struct {
	MaxLsmNodesPerLevel int `yaml:"maxLsmNodesPerLevel" validate:"gte=1"`
}

type LeveledConfig struct {
	DataBlockSize           int64 `yaml:"dataBlockSize" validate:"gte=1"`
	FirstLevelTotalDataSize int64 `yaml:"firstLevelTotalDataSize" validate:"gte=1"`
	FanoutSize              int8  `yaml:"fanoutSize" validate:"gt=1"`
}

type CacheConfig struct {
	MaxSize uint64 `yaml:"maxSize" validate:"gte=1"`
}

type TokenBucketConfig struct {
	MaxTokenSize int64 `yaml:"maxTokenSize" validate:"gte=1"`
	Interval     int64 `yaml:"interval" validate:"gte=1"`
}

var config = &Config{
	WAL: WALConfig{
		SegmentSize:   1048576,
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
		CompressionFilename: "CompressionInfo.bin",
	},
	LSMTree: LSMTreeConfig{
		MaxLevel:            4,
		CompactionAlgorithm: "Size-Tiered",
		SizeTiered: SizeTieredConfig{
			MaxLsmNodesPerLevel: 8,
		},
		Leveled: LeveledConfig{
			DataBlockSize:           160_000,   // 160 Kb
			FirstLevelTotalDataSize: 1_000_000, // 1 Mb
			FanoutSize:              10,
		},
	},
	Cache: CacheConfig{
		MaxSize: 1024,
	},
	TokenBucket: TokenBucketConfig{
		MaxTokenSize: 1024,
		Interval:     60,
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
		log.Println("warning: The configuration file is invalid. Using the default configuration.")
		return config
	}
	loadedConfig := Config{}
	_ = yaml.Unmarshal(file, &loadedConfig)

	validate := validator.New(validator.WithRequiredStructEnabled())
	_ = validate.RegisterValidation("float_between", validateFloatBetween)
	err = validate.Struct(loadedConfig)

	if err != nil {
		log.Println("warning: The configuration file is invalid. Using the default configuration.")
	} else {
		config = &loadedConfig
	}

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

// validateFloatBetween checks if a floating-point number is within the range (0, 1).
func validateFloatBetween(fl validator.FieldLevel) bool {
	minValue := 0.0
	maxValue := 1.0

	v := fl.Field()
	if v.Kind() == reflect.Float64 {
		value := v.Float()
		return value > minValue && value < maxValue
	}

	return false
}
