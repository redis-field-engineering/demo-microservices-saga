package types

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Host          string         `yaml:"host"`
	Port          int            `yaml:"port"`
	Password      string         `yaml:"password"`
	Microservices []Microservice `yaml:"microservices"`
}

type Microservice struct {
	Name          string  `yaml:"name"`
	Input         string  `yaml:"input"`
	Output        string  `yaml:"output"`
	ErrorRate     float32 `yaml:"error_rate"`
	BatchSize     int32   `yaml:"batch_size"`
	SaveBatchSize int32   `yaml:"save_batch_size"`
	BlockMS       int32   `yaml:"block_ms"`
	ProcMin       int     `yaml:"min_proc_ms"`
	ProcMax       int     `yaml:"max_proc_ms"`
}

func (c *Config) GetConf(cf string) *Config {
	yamlFile, err := ioutil.ReadFile(cf)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return c
}
