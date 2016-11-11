package cnf

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"sync"
)

type Config struct {
	Term string
	Port string
	Path string
}

var (
	config     *Config
	configLock = new(sync.RWMutex)
)

func LoadConfig() {
	file, err := ioutil.ReadFile("conf.json")
	if err != nil {
		log.Fatal("open config: ", err)
	}

	temp := new(Config)
	if err = json.Unmarshal(file, temp); err != nil {
		log.Fatal("parse config: ", err)
	}
	configLock.Lock()
	config = temp
	configLock.Unlock()
}

func GetConfig() *Config {
	configLock.RLock()
	defer configLock.RUnlock()
	return config
}
