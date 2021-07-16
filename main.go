package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	flag "github.com/spf13/pflag"
)

func main() {
	config, err := ParseConfig()
	if err != nil {
		log.Panicln("Could not parse command line arguments", err.Error())
	}
	resolveFile(config)
	var lf io.WriteCloser
	if config.logFile == "-" || config.logFile == "stderr" {
		lf = os.Stderr
	} else {
		lf, err = os.OpenFile(config.logFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0755)
		if err != nil {
			log.Println("Could not open logfile, will fall back to logging to stderr")
			lf = os.Stderr
		}
	}
	defer lf.Close()
	log.Default().SetOutput(lf)
	log.Default().SetFlags(log.LUTC | log.LstdFlags | log.Lmsgprefix | log.Lshortfile)
	log.Default().SetPrefix("[redump]")

	client, err := NewRedisClient(config)
	if err != nil {
		log.Fatalf("Could not created redis client %s", err.Error())
	}
	defer client.Close()
	log.Printf("Redis connection established")

	if config.Dump {
		dump(config, client)
	} else if config.Restore {
		restore(config, client)
	}
}

type Config struct {
	Host     string
	Port     int
	Database int
	Auth     string
	Dump     bool
	Restore  bool
	DumpFile string
	Append   bool
	Verbose  bool

	logFile string
}

func ParseConfig() (*Config, error) {

	hostPtr := flag.StringP("host", "H", "localhost", "Redis host to connect to")
	portPtr := flag.IntP("port", "p", 6379, "Redis port to connect to")
	authPtr := flag.StringP("auth", "a", "", "Redis authorization password")
	dbPtr := flag.IntP("database", "d", 0, "The database id of Redis to dump/restore")

	dumpPtr := flag.BoolP("dump", "D", false, "Dump the redis data into the dump file")
	restorePtr := flag.BoolP("restore", "R", false, "Restore the redis database from dump file")
	dumpfFilePtr := flag.StringP("file", "f", "./redump-${host}-${port}-${db}.dump",
		"The dump file where the dump will be written to")
	appendPtr := flag.Bool("append", false, "Whether to append to dump file or to overwrite it")
	verbosePtr := flag.BoolP("verbose", "V", false, "Verbose logging to logfile")
	logfilePtr := flag.String("log", "stderr", "The log file to write to")

	flag.Parse()

	config := Config{
		Host:     *hostPtr,
		Port:     *portPtr,
		Database: *dbPtr,
		Auth:     *authPtr,
		Dump:     *dumpPtr,
		Restore:  *restorePtr,
		DumpFile: *dumpfFilePtr,
		Append:   *appendPtr,
		Verbose:  *verbosePtr,
		logFile:  *logfilePtr,
	}
	return validate(&config)
}

func validate(config *Config) (*Config, error) {

	if config.Dump == false && config.Restore == false {
		return nil, fmt.Errorf("Both dump and restore action is false, please provide one action")
	}
	if config.Dump == true && config.Restore == true {
		return nil, fmt.Errorf("Both dump and restore actions are true, please set only one")
	}
	if config.Database < 0 || config.Database > 16 {
		return nil, fmt.Errorf("Redis supports db id from 0 to 16")
	}
	if config.Port < 0 || config.Port > 65535 {
		return nil, fmt.Errorf("Invalid port number: %d", config.Port)
	}
	if len(config.DumpFile) == 0 {
		return nil, fmt.Errorf("Dump file not provided")
	}
	return config, nil
}

func resolveFile(conf *Config) {
	df := conf.DumpFile
	df = strings.Replace(df, "${host}", conf.Host, -1)
	df = strings.Replace(df, "${port}", strconv.Itoa(conf.Port), -1)
	df = strings.Replace(df, "${db}", strconv.Itoa(conf.Database), -1)
	conf.DumpFile = df
}

func dump(config *Config, client RedisClient) {
	dumper := NewRedisDumper(config, client)
	defer dumper.Close()
	log.Printf("Starting the dump")
	ctr := dumper.Dump()
	log.Printf("%d keys have been dumped to file", ctr)
}

func restore(config *Config, client RedisClient) {
	restorer := NewRedisRestorer(config, client)
	defer restorer.Close()
	log.Printf("Starting the restore")
	ctr := restorer.Restore()
	log.Printf("%d keys have been restored from file", ctr)
}
