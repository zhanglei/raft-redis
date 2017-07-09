package store

import "net"

type Conn struct {
	Conn     net.Conn
	Addr     string
}