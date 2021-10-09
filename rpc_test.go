package gorpc

import (
	"context"
	"flag"
	"fmt"
	"github.com/seanbit/gokit/foundation"
	"github.com/sirupsen/logrus"
	"github.com/smallnest/rpcx/server"
	"io/ioutil"
	"reflect"
	"testing"
	"time"
)

type UserInfo struct {
	UserId int						`json:"user_id" validate:"required,min=1"`
	UserName string					`json:"user_name" validate:"required,eorp"`
}

type UserAddParameter struct {
	UserName string					`json:"user_name" validate:"required,eorp"`
	Password string					`json:"password" validate:"required,gte=1"`
}

type userServiceImpl struct {
}
var UserService = &userServiceImpl{}

func (this *userServiceImpl) UserAdd(ctx context.Context, parameter *UserAddParameter, user *UserInfo) error {
	//if parameter.UserName != "asd" {
	//	return foundation.NewError(errors.New("asd"), 11001, "")
	//}
	*user = UserInfo{
		UserId:   1230090123,
		UserName: parameter.UserName,
	}
	//user.UserId = 1230090123
	//user.UserName = parameter.UserName
	return nil
}

func TestRpcServer(t *testing.T) {
	//ServerCertBytes, _ := ioutil.ReadFile("/Users/sean/Desktop/Go/secret/webkit/server1.pem")
	//ServerKeyBytes, _ := ioutil.ReadFile("/Users/sean/Desktop/Go/secret/webkit/server1.key")
	//CACertBytes, _ := ioutil.ReadFile("/Users/sean/Desktop/Go/secret/webkit/ca.pem")
	//_rpc_testing = true
	Serve(RpcConfig{
		RunMode:              "debug",
		RpcPort:              9901,
		RpcPerSecondConnIdle: 500,
		ReadTimeout:          60 * time.Second,
		WriteTimeout:         60 * time.Second,
		Token: &TokenConfig{
			TokenSecret:          "asdasd",
			TokenIssuer:          "zhsa",
		},
		Tls: nil,
		//Tls: &TlsConfig{
		//	ServerCert:		  string(ServerCertBytes),
		//	ServerKey:		  string(ServerKeyBytes),
		//	CACert:			  string(CACertBytes),
		//	CACommonName:			  "ex.sean",
		//},
		Registry: &Registry{
			UserName: "root",
			Password: "etcd.user.root.pwd",
			BasePath: "com/sean/gorpc/rpc",
			Servers:  []string{"192.168.0.7:2181"},
		},
	}, nil, []RegService{RegService{
			Name:     "User",
			Rcvr:     new(userServiceImpl),
			Metadata: "",
		}}, false)

	var user = new(UserInfo)
	client, err := CreateClient("User", "")
	if err != nil {
		t.Error(err)
		return
	}
	ctx := TraceContext(context.Background(), 1200011101, 101, "asd", "user")
	if err := ClientCall(client, ctx, "UserAdd", &UserAddParameter{
		UserName: "asd",
		Password: "Aa123456",
	},user); err != nil {
		fmt.Printf("err---%s", err.Error())
	} else  {
		fmt.Printf("user--%+v", user)
	}
}

func BenchmarkRpcClientCall(b *testing.B) {
	SetClient(ClientConfig{
		RunMode:              "debug",
		ReadTimeout:          60 * time.Second,
		WriteTimeout:         60 * time.Second,
		Token: &TokenConfig{
			TokenSecret:          "asdasd",
			TokenIssuer:          "zhsa",
		},
		Tls: nil,
		//Tls: &TlsConfig{
		//	ServerCert:		  string(ServerCertBytes),
		//	ServerKey:		  string(ServerKeyBytes),
		//	CACert:			  string(CACertBytes),
		//	CACommonName:			  "ex.sean",
		//},
		Registry: &Registry{
			UserName: "root",
			Password: "etcd.user.root.pwd",
			BasePath: "com/sean/gorpc/rpc",
			Servers:  []string{"192.168.0.7:2181"},
		},
	}, nil)

	for i := 0; i < b.N; i++ {
		var user = new(UserInfo)
		client, err := CreateClient("User", "")
		if err != nil {
			b.Error(err)
		}
		ctx := TraceContext(context.Background(), 100000000000 + uint64(i), uint64(i), fmt.Sprintf("user%d", i), "user")
		if err := ClientCall(client, ctx, "UserAdd", &UserAddParameter{
			UserName: "asd",
			Password: "Aa123456",
		},user); err != nil {
			b.Error(err)
			//fmt.Printf("err---%s", err.Error())
		} else  {
			//fmt.Printf("user--%+v", user)
		}
	}
}

func TestP2pCall(t *testing.T) {
	var user = new(UserInfo)
	ctx := TraceContext(context.Background(), 101, 11001, "testuser", "testrole")
	if err := P2pCall("User", "", []string{"192.168.1.21:9901"}, ctx, "UserAdd",  &UserAddParameter{
		UserName: "1237757@qq.com",
		Password: "Aa123456",
	}, user); err != nil {
		fmt.Printf("err---%s", err.Error())
	} else  {
		fmt.Printf("user--%+v", user)
	}
}

func TestCall(t *testing.T) {
	ServerCertBytes, _ := ioutil.ReadFile("/Users/sean/Desktop/Go/secret/webkit/server2.pem")
	ServerKeyBytes, _ := ioutil.ReadFile("/Users/sean/Desktop/Go/secret/webkit/server2.key")
	CACertBytes, _ := ioutil.ReadFile("/Users/sean/Desktop/Go/secret/webkit/ca.pem")
	//_rpc_testing = true
	Serve(RpcConfig{
		RunMode:              "debug",
		RpcPort:              9903,
		RpcPerSecondConnIdle: 500,
		ReadTimeout:          60 * time.Second,
		WriteTimeout:         60 * time.Second,
		Token: &TokenConfig{
			TokenSecret:          "asdasd",
			TokenIssuer:          "zhsa",
		},
		Tls: &TlsConfig{
			ServerCert:		  string(ServerCertBytes),
			ServerKey:		  string(ServerKeyBytes),
			CACert:			  string(CACertBytes),
			CACommonName:			  "ex.sean",
		},
		Registry: &Registry{
			UserName: "root",
			Password: "etcd.user.root.pwd",
			BasePath: "sean.tech/webkit/serving/rpc",
			Servers:  []string{"127.0.0.1:2379"},
		},
	},
		nil, []RegService{RegService{
			Name:     "User",
			Rcvr:     new(userServiceImpl),
			Metadata: "",
		}}, true)
	var user = new(UserInfo)
	ctx := TraceContext(context.Background(), 101, 11001, "testuser", "testrole")
	if err := Call("User", "", ctx, "UserAdd", &UserAddParameter{
		UserName: "1237757@qq.com",
		Password: "Aa123456",
	}, user); err != nil {
		fmt.Printf("err-------:%+v\n", err)
		fmt.Printf("err type-------:%+v\n", reflect.TypeOf(err))
		if e, ok := foundation.ParseError(err); ok {
			fmt.Println(e.Code())
		}
	} else  {
		fmt.Printf("user--%+v\n", user)
	}
}


func TestP2pRpcServer(t *testing.T) {
	//ServerCertBytes, _ := ioutil.ReadFile("/Users/sean/Desktop/Go/secret/webkit/server1.pem")
	//ServerKeyBytes, _ := ioutil.ReadFile("/Users/sean/Desktop/Go/secret/webkit/server1.key")
	//CACertBytes, _ := ioutil.ReadFile("/Users/sean/Desktop/Go/secret/webkit/ca.pem")
	//_rpc_testing = true
	Serve(RpcConfig{
		RunMode:              "debug",
		RpcPort:              9901,
		RpcPerSecondConnIdle: 500,
		ReadTimeout:          60 * time.Second,
		WriteTimeout:         60 * time.Second,
		Token: &TokenConfig{
			TokenSecret:          "asdasd",
			TokenIssuer:          "zhsa",
		},
		Tls: nil,
		//Tls: &TlsConfig{
		//	ServerCert:		  string(ServerCertBytes),
		//	ServerKey:		  string(ServerKeyBytes),
		//	CACert:			  string(CACertBytes),
		//	CACommonName:			  "ex.sean",
		//},
	}, logrus.New(), []RegService{RegService{
		Name:     "User",
		Rcvr:     new(userServiceImpl),
		Metadata: "",
	}}, false)

	select {

	}
}

func TestP2pClientCall(t *testing.T) {
	SetClient(ClientConfig{
		RunMode:               "debug",
		ReadTimeout:           60 * time.Second,
		WriteTimeout:          60 * time.Second,
		Registry:              nil,
		Token:                 &TokenConfig{
			TokenSecret:          "asdasd",
			TokenIssuer:          "zhsa",
		},
		Tls:                   nil,
		TlsInsecureSkipVerify: false,
		ProcessLog:            false,
	}, logrus.New())

	var user = new(UserInfo)
	client, err := CreateP2pClient("User", "", []string{"127.0.0.1:9901"})
	if err != nil {
		t.Error(err)
		return
	}
	ctx := TraceContext(context.Background(), 1200011101, 101, "asd", "user")
	if err := ClientCall(client, ctx, "UserAdd", &UserAddParameter{
		UserName: "asd",
		Password: "Aa123456",
	},user); err != nil {
		fmt.Printf("err---%s", err.Error())
	} else  {
		fmt.Printf("user--%+v", user)
	}
}






func TestParameter(t *testing.T) {
	var user = new(UserInfo)
	if err := UserService.UserAdd(context.Background(), &UserAddParameter{
		UserName: "1237757@qq.com",
		Password: "Aa123456",
	}, user); err != nil {
		fmt.Printf("err---%s", err.Error())
	} else  {
		fmt.Printf("user--%+v", user)
	}
}




type Args struct {
	A int
	B int
}

type Reply struct {
	C int
}

type Arith int

func (t *Arith) Mul(ctx context.Context, args *Args, reply *Reply) error {
	reply.C = args.A * args.B
	return nil
}

func TestRpcst(t *testing.T) {
	flag.Parse()
	s := server.NewServer()
	addRegistryPlugin(s)

	s.RegisterName("Arith", new(Arith), "")

	go func() {
		s.Serve("tcp", ":9003")
	}()

	//d := client.NewZookeeperDiscovery("/gorpc/services")
	//xclient := client.NewXClient("Arith", client.Failtry, client.RandomSelect, d, client.DefaultOption)
	//defer xclient.Close()

	//args := &Args{
	//	A: 10,
	//	B: 20,
	//}
	//
	//for i := 0; i < 100; i++ {
	//	reply := &Reply{}
	//	err := xclient.Call(context.Background(), "Mul", args, reply)
	//	if err != nil {
	//		log.Fatalf("failed to call: %v", err)
	//	}
	//
	//	log.Printf("%d * %d = %d", args.A, args.B, reply.C)
	//}
}

func addRegistryPlugin(s *server.Server) {
	//r := client.InprocessClient
	//s.Plugins.Add(r)
}

func TestSuc(t *testing.T) {
	fmt.Println("success")
}