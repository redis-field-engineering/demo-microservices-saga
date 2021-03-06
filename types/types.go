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
	Name   string `yaml:"name"`
	Input  string `yaml:"input"`
	Output string `yaml:"output"`
	Order  int    `yaml:"order"`
}

func (c *Config) GetConf(cf string) *Config {
	log.Printf("Shit")
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
