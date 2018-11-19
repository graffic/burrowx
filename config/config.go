package config

import (
	"encoding/json"
	"log"
	"os"
)

// InfluxDB Configuration
type InfluxDB struct {
	Db          string            `json:"db"`
	Enable      bool              `json:"enable"`
	Hosts       string            `json:"hosts"`
	Pwd         string            `json:"pwd"`
	Username    string            `json:"username"`
	Measurement string            `json:"measurement"`
	ExtraTags   map[string]string `json:"extra_tags"`
}

// Config parameters for burrowx
type Config struct {
	General struct {
		ClientID       string `json:"clientId"`
		GroupBlacklist string `json:"groupBlacklist"`
		Logconfig      string `json:"logconfig"`
		Pidfile        string `json:"pidfile"`

		TopicFilter string `json:"topicFilter"`
	} `json:"general"`

	InfluxDB InfluxDB `json:"influxdb"`

	Kafka map[string]*struct {
		Brokers       string `json:"brokers"`
		Zookeepers    string `json:"zookeepers"`
		ClientProfile string `json:"ClientProfile"`
		OffsetsTopic  string `gcfg:"offsetsTopic"`

		Sasl struct {
			Username string
			Password string
		}
	} `json:"kafka"`

	Zookeeper struct {
		Hosts    string `json:"hosts"`
		LockPath string `json:"lock-path"`
		Timeout  int    `json:"timeout"`
	} `json:"zookeeper"`

	ClientProfile map[string]*Profile `json:"ClientProfile"`
}

// Profile for kafka client
type Profile struct {
	ClientID        string `json:"clientId"`
	TLS             bool   `json:"tls"`
	TLSNoVerify     bool   `json:"tlsNoverify"`
	TLSCertFilePath string `json:"tlsCertfilepath"`
	TLSKeyFilePath  string `json:"tlsKeyfilepath"`
	TLSCAFilePath   string `json:"tlsCafilepath"`
}

// ReadConfig from a string to a Config structure
func ReadConfig(cfgFile string) *Config {
	var cfg Config
	f, err := os.OpenFile(cfgFile, os.O_RDONLY, 0660)
	errAndExit(err)
	err = json.NewDecoder(f).Decode(&cfg)
	errAndExit(err)

	cfg.init()
	return &cfg
}

func (cfg *Config) init() {
	if cfg.ClientProfile == nil {
		cfg.ClientProfile = make(map[string]*Profile)
	}
	if _, ok := cfg.ClientProfile["default"]; !ok {
		cfg.ClientProfile["default"] = &Profile{
			ClientID: cfg.General.ClientID,
			TLS:      false,
		}
	}

	if cfg.InfluxDB.Measurement == "" {
		cfg.InfluxDB.Measurement = "consumer_metrics"
	}

	for _, k := range cfg.Kafka {
		if k.OffsetsTopic == "" {
			k.OffsetsTopic = "__consumer_offsets"
		}
		if k.ClientProfile == "" {
			k.ClientProfile = "default"
		}
	}
}

func errAndExit(err error) {
	if err != nil {
		log.Fatalf("Failed to parse json data: %s", err)
		os.Exit(1)
	}
}
