package gorpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/rpcxio/libkv/store"
	"github.com/sirupsen/logrus"
	"github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/share"
	"net"
	"sync"
	"time"
)

type ClientConfig struct {
	RunMode 				string			`json:"-" validate:"required,oneof=debug test release"`
	ReadTimeout           	time.Duration	`json:"read_timeout" validate:"required,gte=1"`
	WriteTimeout          	time.Duration	`json:"write_timeout" validate:"required,gte=1"`
	Registry				*Registry 		`json:"-"`
	Token                   *TokenConfig	`json:"token"`
	Tls     				*TlsConfig 		`json:"-"`
	TlsInsecureSkipVerify   bool			`json:"tls_insecure_skip_verify"`
	ProcessLog				bool			`json:"processLog"`
}

var (
	_clientConfig ClientConfig
	clientMap sync.Map
	p2pClientMap sync.Map
)

func SetClient(config ClientConfig, logger logrus.FieldLogger) {
	if logger != nil {
		ClientLogger.log = logger
	}
	_clientConfig = config
}

/**
 * 创建rpc调用客户端，基于Etcd服务发现
 */
func CreateClient(serviceName, serverName string) (client.XClient, error) {
	if c, ok := clientMap.Load(serviceName); ok {
		return c.(client.XClient), nil
	}
	option := createClientOption(serverName)
	discovery, err := newDiscovery(serviceName)
	if err != nil {
		return nil, err
	}
	xclient := client.NewXClient(serviceName, client.Failover, client.RoundRobin, discovery, option)
	if _clientConfig.ProcessLog {
		xclient.GetPlugins().Add(ClientLogger)
	}
	xclient.GetPlugins().Add(&clientCloser{serviceName: serviceName})
	clientMap.Store(serviceName, xclient)
	return xclient, nil
}

/**
 * 创建rpc调用客户端，基于服务发现
 */
func CreateP2pClient(serviceName, serverName string, addrs []string) (client.XClient, error) {
	if c, ok := p2pClientMap.Load(serviceName); ok {
		return c.(client.XClient), nil
	}
	option := createClientOption(serverName)
	discovery, err := newP2PDiscovery(addrs)
	if err != nil {
		return nil, err
	}
	xclient := client.NewXClient(serviceName, client.Failover, client.RoundRobin, discovery, option)
	xclient.GetPlugins().Add(ClientLogger)
	xclient.GetPlugins().Add(&clientCloser{serviceName: serviceName})
	p2pClientMap.Store(serviceName, xclient)
	return xclient, nil
}

func createClientOption(serverName string) client.Option {
	option := client.DefaultOption
	option.Heartbeat = true
	option.HeartbeatInterval = time.Second
	option.ConnectTimeout = _clientConfig.ReadTimeout
	option.IdleTimeout = _clientConfig.ReadTimeout
	if _clientConfig.Tls != nil {
		//cert, err := tls.LoadX509KeyPair(_clientConfig.ServerPemPath, _clientConfig.ServerKeyPath)
		cert, err := tls.X509KeyPair([]byte(_clientConfig.Tls.ServerCert), []byte(_clientConfig.Tls.ServerKey))
		if err != nil {
			log.Errorf("unable to read cert.pem and cert.key : %s", err.Error())
			return option
		}
		certPool := x509.NewCertPool()
		ok := certPool.AppendCertsFromPEM([]byte(_clientConfig.Tls.CACert))
		if !ok {
			log.Errorf("failed to parse root certificate : %s", err.Error())
			return option
		}
		option.TLSConfig = &tls.Config{
			RootCAs:            certPool,
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: _clientConfig.TlsInsecureSkipVerify,
			ServerName: _clientConfig.Tls.CACommonName + "." + serverName,
		}
	}
	return option
}

func newP2PDiscovery(addrs []string) (client.ServiceDiscovery, error) {
	if addrs == nil {
		err := fmt.Errorf("failed to create service Peer2Peer discovery : addrs is nil")
		log.Error(err)
		return nil, err
	}
	if len(addrs) == 1 {
		return client.NewPeer2PeerDiscovery("tcp@" +addrs[0], "")
	}
	var clientkvs []*client.KVPair
	for _, addr := range addrs {
		clientkvs = append(clientkvs, &client.KVPair{Key: addr})
	}
	return client.NewMultipleServersDiscovery(clientkvs)
}

func newDiscovery(serviceName string) (client.ServiceDiscovery, error) {
	var options = &store.Config{
		ClientTLS:         nil,
		TLS:               nil,
		ConnectionTimeout: 0,
		Bucket:            "",
		PersistConnection: false,
		Username:          _clientConfig.Registry.UserName,
		Password:          _clientConfig.Registry.Password,
	}
	return client.NewZookeeperDiscovery(_clientConfig.Registry.BasePath, serviceName, _clientConfig.Registry.Servers, options)
}

/**
 * client call
 */
func ClientCall(client client.XClient, ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error {
	if ctx, err := clientAuth(client, ctx); err != nil {
		return err
	} else {
		return client.Call(ctx, serviceMethod, args, reply)
	}
}

/**
 * client go
 */
func ClientGo(client client.XClient, ctx context.Context, serviceMethod string, args interface{}, reply interface{}, done chan *client.Call) (*client.Call, error) {
	if ctx, err := clientAuth(client, ctx); err != nil {
		return nil, err
	} else {
		return client.Go(ctx, serviceMethod, args, reply, done)
	}
}

/**
 * call
 */
func Call(serviceName, serverName string, ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error {
	client, err := CreateClient(serviceName, serverName)
	if err != nil {
		return err
	}
	return ClientCall(client, ctx, serviceMethod, args, reply)
}

/**
 * go
 */
func Go(serviceName, serverName string, ctx context.Context, serviceMethod string, args interface{}, reply interface{}, done chan *client.Call) (*client.Call, error) {
	client, err := CreateClient(serviceName, serverName)
	if err != nil {
		return nil, err
	}
	return ClientGo(client, ctx, serviceMethod, args, reply, done)
}

/**
 * p2p call
 */
func P2pCall(serviceName, serverName string, addrs []string, ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error {
	client, err := CreateP2pClient(serviceName, serverName, addrs)
	if err != nil {
		return err
	}
	return ClientCall(client, ctx, serviceMethod, args, reply)
}

/**
 * p2p go
 */
func P2pGo(serviceName, serverName string, addrs []string, ctx context.Context, serviceMethod string, args interface{}, reply interface{}, done chan *client.Call) (*client.Call, error) {
	client, err := CreateP2pClient(serviceName, serverName, addrs)
	if err != nil {
		return nil, err
	}
	return ClientGo(client, ctx, serviceMethod, args, reply, done)
}

type clientCloser struct {
	serviceName string
}
// ClientConnectionClosePlugin is invoked when the connection is closing.
func (this *clientCloser) ClientConnectionClose(net.Conn) error {
	if _, ok := clientMap.Load(this.serviceName); ok {
		clientMap.Delete(this.serviceName)
	}
	return nil
}

/**
 * client auth
 */
func clientAuth(client client.XClient, ctx context.Context) (context.Context, error) {
	if ctx == nil {
		return nil, errors.New("rpc client call error: context is nil")
	}
	trace := GetTrace(ctx)
	if trace == nil {
		return ctx, errors.New("rpc client call error: trace in context is nil")
	}
	if _clientConfig.Token != nil {
		var expiresTime = (_clientConfig.ReadTimeout + _clientConfig.WriteTimeout) * 2
		if token, err := generateToken(trace, _clientConfig.Token.TokenSecret, _clientConfig.Token.TokenIssuer, expiresTime); err != nil {
			return ctx, err
		} else {
			client.Auth(token)
		}
	}

	ctx = context.WithValue(ctx, share.ReqMetaDataKey, make(map[string]string))
	return ctx, nil
}