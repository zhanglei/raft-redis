// go-redis-server is a helper library for building server software capable of speaking the redis protocol.
// This could be an alternate implementation of redis, a custom proxy to redis,
// or even a completely different backend capable of "masquerading" its API as a redis database.

package server

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"reflect"
	"time"
	"flag"
	"strings"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/raft/raftpb"
)

var (
	Conns = make( map[string]chan interface{})
	proposeC = make(chan string)
	confChangeC = make(chan raftpb.ConfChange)
	commitC = make(chan *string)
	errorC = make(chan error)
	snapshotterReady = make(chan *snap.Snapshotter, 1)
)

type Server struct {
	Addr         string // TCP address to listen on, ":6389" if empty
	MonitorChans []chan string
	methods      map[string]HandlerFn
	confChangeC chan<- raftpb.ConfChange
}

func (srv *Server) ListenAndServe() error {
	addr := srv.Addr
	if addr == "" {
		addr = ":6389"
	}
	l, e := net.Listen("tcp", addr)
	if e != nil {
		return e
	}
	return srv.Serve(l)
}

// Serve accepts incoming connections on the Listener l, creating a
// new service goroutine for each.  The service goroutines read requests and
// then call srv.Handler to reply to them.
func (srv *Server) Serve(l net.Listener) error {
	defer l.Close()
	srv.MonitorChans = []chan string{}
	for {
		rw, err := l.Accept()
		if err != nil {
			return err
		}
		go srv.ServeClient(rw)
	}
}

// Serve starts a new redis session, using `conn` as a transport.
// It reads commands using the redis protocol, passes them to `handler`,
// and returns the result.
func (srv *Server) ServeClient(conn net.Conn) (err error) {
	//clientChan := make(chan struct{})
	var clientAddr string
	switch co := conn.(type) {
	case *net.UnixConn:
		f, err := conn.(*net.UnixConn).File()
		if err != nil {
			return err
		}
		clientAddr = f.Name()
	default:
		clientAddr = co.RemoteAddr().String()
	}
	c := fmt.Sprintf("%s",clientAddr)

	//Conns[c] = make(chan interface{})
	defer func() {
		if err != nil {
			fmt.Fprintf(conn, "-%s\n", err)
		}
	//	close(Conns[c])
	//	delete(Conns,c)
		conn.Close()
	}()
	// Read on `conn` in order to detect client disconnect
	go func() {
		// Close chan in order to trigger eventual selects

		defer func() {
		//	 close(clientChan)
			 Debugf("Client disconnected")
		}()

		// FIXME: move conn within the request.
		if false {
			io.Copy(ioutil.Discard, conn)
		}
	}()

	for {
		request, err := parseRequest(conn)
		if err != nil {
			return err
		}
		request.Host = clientAddr
		//request.ClientChan = clientChan
		request.Conn = c
		reply, err := srv.Apply(request)
		if err != nil {
			return err
		}
		if _, err = reply.WriteTo(conn); err != nil {
			return err
		}
	}
	return nil
}

func NewServer(c *Config) (*Server, error) {
	srv := &Server{
		MonitorChans: []chan string{},
		methods:      make(map[string]HandlerFn),
	}

	srv.Addr = fmt.Sprintf("%s:%d", c.Host, c.Port)

	if c.Handler == nil {

		time.Sleep(1*time.Second)
		c.Handler = _Storage.Redis
	}

	rh := reflect.TypeOf(c.Handler)
	for i := 0; i < rh.NumMethod(); i++ {
		method := rh.Method(i)
		if method.Name[0] > 'a' && method.Name[0] < 'z' {
			continue
		}
		handlerFn, err := srv.createHandlerFn(c.Handler, &method.Func)
		if err != nil {
			return nil, err
		}
		srv.Register(method.Name, handlerFn)
	}
	return srv, nil
}

func Main()  {
	cluster := flag.String("cluster", "http://127.0.0.1:12379", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 6389, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	dataDir := flag.String("data-dir","data/","store databases")
	flag.Parse()
	defer close(proposeC)
	defer close(confChangeC)
	_Storage = &Storage{proposeC: proposeC, Redis: NewDatabase()}
	NewRaftNode(*id, strings.Split(*cluster, ","), strings.TrimRight(*dataDir,"/"),*join)

	go func() {
		server, err := NewServer(DefaultConfig(*kvport))
		if err != nil {
			panic(err)
		}
		go func() {
			if err := server.ListenAndServe(); err != nil {
				panic(err)
			}
		}()
	}()
	Run()
	if err, ok := <-errorC; ok {
		panic(err)
	}
}