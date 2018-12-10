package main

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/skiloop/rediscli/commands"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
)

var (
	app    = kingpin.New("rediscli", "A redis command line application.")
	_      = app.HelpFlag.Short('h')
	server = app.Flag("server", "Server address.").Short('s').Default("redis://localhost/0").String()

	// set key command
	setKeyCmd     = app.Command("set", "set key").Alias("s")
	setKeyName    = setKeyCmd.Arg("key", "key name").String()
	setKeyValue   = setKeyCmd.Arg("value", "value name").String()
	setKeyExpires = setKeyCmd.Arg("expires", "expire time").Duration()

	// set key from file command
	importKeyCmd   = app.Command("import", "import key from file(SET command)").Alias("i")
	importFile     = importKeyCmd.Arg("file", "import file name").String()
	importFileType = importKeyCmd.Arg("filetype", "file type, like json, yaml or csv").String()

	// list command
	exportKeyCmd     = app.Command("export", "export a list of keys").Alias("e")
	exportKeyOutFile = exportKeyCmd.Arg("output", "output file").Required().String()
	exportKeys       = exportKeyCmd.Arg("key", "key names to exports").Required().Strings()
)

func newClientFromURL(url *string) *redis.Client {
	opts, err := redis.ParseURL(*url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fail to parse url: %s", url)
		return nil
	}
	return redis.NewClient(opts)
}

func setKey() {
	cli := newClientFromURL(server)
	if cli == nil {
		fmt.Fprintln(os.Stderr, "server none")
		return
	}
	defer cli.Close()
	commands.Set(cli, *setKeyName, *setKeyValue, *setKeyExpires)
}

func exportKey() {
	cli := newClientFromURL(server)
	if cli == nil {
		fmt.Fprintln(os.Stderr, "server none")
		return
	}
	defer cli.Close()
	commands.Load2File(cli, exportKeys, *exportKeyOutFile)
}

func setKeysFromFile() {
	cli := newClientFromURL(server)
	if cli == nil {
		fmt.Fprintln(os.Stderr, "server none")
		return
	}
	defer cli.Close()
	commands.SetFromFile(cli, *importFile, *importFileType)
}

func main() {
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case setKeyCmd.FullCommand():
		setKey()
	case exportKeyCmd.FullCommand():
		exportKey()
	case importKeyCmd.FullCommand():
		setKeysFromFile()
	default:
		flag.Usage()
	}
}
