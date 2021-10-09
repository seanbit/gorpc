package gorpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/rcrowley/go-metrics"
	"github.com/rpcxio/libkv/store"
	"github.com/seanbit/gokit/validate"
	"github.com/sirupsen/logrus"
	rpcxLog "github.com/smallnest/rpcx/log"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/server"
	"github.com/smallnest/rpcx/serverplugin"
	"math"
	"os"
	"os/signal"
	"time"
)

type RpcConfig struct {
	RunMode 				string			`json:"-" validate:"required,oneof=debug test release"`
	RpcPort               	int				`json:"-"`
	RpcPerSecondConnIdle  	int64			`json:"rpc_per_second_conn_idle" validate:"required,gte=1"`
	ReadTimeout           	time.Duration	`json:"read_timeout" validate:"required,gte=1"`
	WriteTimeout          	time.Duration	`json:"write_timeout" validate:"required,gte=1"`
	Whitelist     			[]string 		`json:"whitelist"`
	Registry				*Registry 		`json:"-"`
	Token                   *TokenConfig	`json:"token"`
	Tls     				*TlsConfig 		`json:"-"`
	TlsInsecureSkipVerify   bool			`json:"tls_insecure_skip_verify"`
	ProcessLog				bool			`json:"processLog"`
}

type TokenConfig struct {
	TokenSecret      		string        	`json:"token_secret" validate:"required,gte=1"`
	TokenIssuer      		string        	`json:"token_issuer" validate:"required,gte=1"`
}

type TlsConfig struct {
	CACert       			string 			`json:"ca_cert" validate:"required"`
	CACommonName 			string 			`json:"ca_common_name" validate:"required"`
	ServerCert   			string 			`json:"server_cert" validate:"required"`
	ServerKey    			string 			`json:"server_key" validate:"required"`
}

type Registry struct {
	Servers  []string `json:"servers" validate:"required,gte=1,dive,tcp_addr"`
	BasePath string   `json:"basePath" validate:"required,gte=1"`
	UserName string   `json:"userName" validate:"required,gte=1"`
	Password string   `json:"password" validate:"required,gte=1"`
}

type RegService struct {
	Name string
	Rcvr interface{}
	Metadata string
}

var (
	log *logrus.Entry
	_config      RpcConfig
)

/**
 * 启动 服务server
 * registerFunc 服务注册回调函数
 */
func Serve(config RpcConfig, logger logrus.FieldLogger, services []RegService, async bool) {
	if logger == nil {
		logger = logrus.New()
	}
	log = logger.WithField("LogIn", "gorpc")
	rpcxLog.SetLogger(log)

	// config validate
	configValidate(config)
	_config = config

	// client config set
	SetClient(ClientConfig{
		RunMode:               _config.RunMode,
		ReadTimeout:           _config.ReadTimeout,
		WriteTimeout:          _config.WriteTimeout,
		Registry:              _config.Registry,
		Token:                 _config.Token,
		Tls:                   _config.Tls,
		TlsInsecureSkipVerify: _config.TlsInsecureSkipVerify,
		ProcessLog:            _config.ProcessLog,
	}, logger)

	// server
	var s *server.Server
	if config.Tls == nil {
		s = server.NewServer(server.WithReadTimeout(config.ReadTimeout), server.WithWriteTimeout(config.WriteTimeout))
	} else {
		//cert, err := tls.LoadX509KeyPair(_config.ServerPemPath, _config.ServerKeyPath)
		cert, err := tls.X509KeyPair([]byte(config.Tls.ServerCert), []byte(config.Tls.ServerKey))
		if err != nil {
			log.Fatal(err)
			return
		}
		certPool := x509.NewCertPool()
		ok := certPool.AppendCertsFromPEM([]byte(_config.Tls.CACert))
		if !ok {
			panic("failed to parse root certificate")
		}
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    certPool,
		}
		s = server.NewServer(server.WithTLSConfig(tlsConfig), server.WithReadTimeout(config.ReadTimeout), server.WithWriteTimeout(config.WriteTimeout))
	}

	address := fmt.Sprintf(":%d", _config.RpcPort)
	registerPlugins(s, address)
	for _, registry := range services {
		if err := s.RegisterName(registry.Name, registry.Rcvr, registry.Metadata); err != nil {
			log.Fatal(err)
		}
	}
	switch async {
	case true:
		go func() {
			err := s.Serve("tcp", address)
			if err != nil {
				log.Fatalf("server start error : %v", err)
			}
			fmt.Println("server start----------------------------")
		}()
	case false:
		err := s.Serve("tcp", address)
		if err != nil {
			log.Fatalf("server start error : %v", err)
		}
		// signal
		quit := make(chan os.Signal)
		signal.Notify(quit, os.Interrupt)
		<- quit
		log.Warn("Shutdown Server ...")
	}
}

func configValidate(config RpcConfig)  {
	if err := validate.ValidateParameter(config); err != nil {
		log.Fatal(err)
	}
	if config.Registry == nil {
		log.Warn("registry for rpc is nil. you can only call for p2p")
	} else if err := validate.ValidateParameter(config.Registry); err != nil {
		log.Fatal(err)
	}
	if config.Token != nil {
		if err := validate.ValidateParameter(config.Token); err != nil {
			log.Fatal(err)
		}
	}
	if config.Tls != nil {
		if err := validate.ValidateParameter(config.Tls); err != nil {
			log.Fatal(err)
		}
	}
}

func registerPlugins(s *server.Server, address string)  {
	// logger
	if _config.ProcessLog {
		s.Plugins.Add(ServerLogger)
	}
	// token auth
	if _config.Token != nil {
		s.AuthFunc = serverAuth
	}
	// whitelist auth
	if _config.Whitelist != nil {
		var wl = make(map[string]bool)
		for _, ip := range _config.Whitelist {
			wl[ip] = true
		}
		s.Plugins.Add(serverplugin.WhitelistPlugin{
			Whitelist:     wl,
			WhitelistMask: nil,
		})
	}
	// registry register
	if _config.Registry != nil {
		RegisterRegistryPlugin(s, address)
	}
	// rate limit register
	RegisterPluginRateLimit(s)
}

/**
 * 注册插件，Etcd注册中心，服务发现
 */
func RegisterRegistryPlugin(s *server.Server, serviceAddr string)  {
	plugin := &serverplugin.ZooKeeperRegisterPlugin{
		ServiceAddress:   "tcp@" + serviceAddr,
		ZooKeeperServers: _config.Registry.Servers,
		BasePath:         _config.Registry.BasePath,
		Metrics:          metrics.NewRegistry(),
		UpdateInterval:   time.Minute,
		Options: &store.Config{
			ClientTLS:         nil,
			TLS:               nil,
			ConnectionTimeout: 0,
			Bucket:            "",
			PersistConnection: false,
			Username:          _config.Registry.UserName,
			Password:          _config.Registry.Password,
		},
	}
	err := plugin.Start()
	if err != nil {
		log.Fatal(err)
	}
	s.Plugins.Add(plugin)
}

/**
 * 注册插件，限流器，限制客户端连接数
 */
func RegisterPluginRateLimit(s *server.Server)  {
	var fillSpeed float64 = 1.0 / float64(_config.RpcPerSecondConnIdle)
	fillInterval := time.Duration(fillSpeed * math.Pow(10, 9))
	plugin := serverplugin.NewRateLimitingPlugin(fillInterval, _config.RpcPerSecondConnIdle)
	s.Plugins.Add(plugin)
}

/**
 * server token auth
 */
func serverAuth(ctx context.Context, req *protocol.Message, token string) error {
	if trace, err := parseToken(token, _config.Token.TokenSecret, _config.Token.TokenIssuer); err != nil {
		return err
	} else if trace.TraceId <= 0 {
		return errors.New("serving rpc resp error: invalid traceId in context")
	}
	return nil
}


