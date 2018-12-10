package commands

import (
	"github.com/go-redis/redis"
	"time"
	"io/ioutil"
	"encoding/json"
	"gopkg.in/yaml.v2"
	"github.com/pkg/errors"
	"fmt"
	"os"
)

func Set(rds *redis.Client, key, value string, expires time.Duration) {
	rds.Set(key, value, expires)
}

type setItem struct {
	Key     string        `json:"key" yaml:"key"`
	Value   string        `json:"value" yaml:"value"`
	Expires time.Duration `json:"expires" yaml:"expires"`
}

func parseItems(fileName string, fmt string) (items *[]setItem, err error) {
	b, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	items = &[]setItem{}
	if fmt == "yaml" {
		err = yaml.Unmarshal(b, &items)
	} else {
		err = json.Unmarshal(b, &items)
	}
	if err != nil {
		items = nil
	}

	return items, err
}

func loadCsvItems(fileName string) (items *[]setItem, err error) {
	//TODO: implement this function
	return nil, errors.New("not implement")
}

func SetFromFile(rds *redis.Client, fn string, fileType string) {
	var items *[]setItem
	var err error
	switch fileType {
	case "csv":
		items, err = loadCsvItems(fn)
	default:
		items, err = parseItems(fn, fileType)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v", err)
		return
	}
	for _, item := range *items {
		fmt.Printf("set %s %d\n", item.Key, item.Expires)
		Set(rds, item.Key, item.Value, item.Expires)
	}
}

func Load2File(rds *redis.Client, keys *[]string, out string) {
	objs := make([]setItem, 0)
	for _, key := range *keys {
		v := rds.Get(key)
		objs = append(objs, setItem{Key: key, Value: v.Val(), Expires: 0})
	}

	b, err := json.Marshal(objs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load error: %v", err)
		return
	}
	err = ioutil.WriteFile(out, b, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "write error: %v", err)
		return
	}
}
