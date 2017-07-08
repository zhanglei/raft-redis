package redis

type Config struct {
	proto   string
	host    string
	port    int
	handler interface{}
	snapDir string
	walDir  string
}

func DefaultConfig() *Config {
	return &Config{
		proto:   "tcp",
		host:    "127.0.0.1",
		port:    6399,
	//	handler: NewDefaultHandler(),
		snapDir:"/home/wida/data/snap/",
		walDir:"/home/wida/data/wal/",
	}
}

func (c *Config) Port(p int) *Config {
	c.port = p
	return c
}

func (c *Config) Host(h string) *Config {
	c.host = h
	return c
}


func (c *Config) Proto(p string) *Config {
	c.proto = p
	return c
}
func (c *Config) SnapDir(s string) *Config {
	c.snapDir = s
	return c
}
func (c *Config) WalDir(w string) *Config {
	c.walDir = w
	return c
}

func (c *Config) Handler(h interface{}) *Config {
	c.handler = h
	return c
}
