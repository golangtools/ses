package ses

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"log"
	"strings"
	"time"
)

type EtcdConfig struct {
	User      string
	Password  string
	Endpoints []string
}

func NewEtcdConfig(user, password string, endpoints []string) *EtcdConfig {
	return &EtcdConfig{
		User:      user,
		Password:  password,
		Endpoints: endpoints,
	}
}

type Config struct {
	Name string        //the service name
	TTL  time.Duration //the service  heartbeat ttl
	Dir  string        //the service base dir on etcd
	Etcd *EtcdConfig
}

func NewConfig(name string, ttl time.Duration, dir string, etcdConfig *EtcdConfig) *Config {
	return &Config{
		Name: name,
		TTL:  ttl,
		Dir:  dir,
		Etcd: etcdConfig,
	}
}

type ServiceInfo struct {
	Name    string
	Address string
	ExtInfo []byte
}

type Service struct {
	address string
	config  *Config
	extInfo []byte //extend info
	cli     *clientv3.Client
	leaseId clientv3.LeaseID
	running bool
}

func NewService(address string, config *Config, extInfo []byte) *Service {
	return &Service{
		address: address,
		config:  config,
		extInfo: extInfo,
	}
}

func (s *Service) Register() error {
	if s.running {
		return nil
	}

	if s.config == nil {
		return errors.New("config can not nil")
	}

	if s.config.Etcd == nil {
		return errors.New("etcd config can not nil")
	}

	if s.config.Name == "" {
		return errors.New("service name can not empty")
	}

	if s.config.TTL < time.Second*3 {
		return errors.New("the service ttl can not less than 3 sec")
	}

	if s.config.Dir == "" {
		return errors.New("the service base dir can not empty")
	}

	if s.config.Dir[len(s.config.Dir)-1] != '/' {
		s.config.Dir += "/"
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   s.config.Etcd.Endpoints,
		DialTimeout: 5 * time.Second,
		Username:    s.config.Etcd.User,
		Password:    s.config.Etcd.Password,
	})
	if err != nil {
		return err
	}
	s.cli = cli

	opt, err := cli.Grant(context.TODO(), 5)
	if err != nil {
		return err
	}
	s.leaseId = opt.ID

	key := s.config.Dir + s.config.Name
	data, err := json.Marshal(&ServiceInfo{Name: s.config.Name, Address: s.address, ExtInfo: s.extInfo})
	if err != nil {
		return err
	}

	_, err = cli.Put(context.TODO(), key, string(data), clientv3.WithLease(opt.ID))
	if err != nil {
		return err
	}

	s.running = true
	go s.heartbeat()
	return nil
}

func (s *Service) UnRegister() error {
	s.running = false
	s.cli.Close()
	return nil
}

func (s *Service) UpdateExtInfo(ext []byte) error {
	s.extInfo = ext

	key := s.config.Dir + s.config.Name
	data, err := json.Marshal(&ServiceInfo{Name: s.config.Name, Address: s.address, ExtInfo: s.extInfo})
	if err != nil {
		return err
	}

	_, err = s.cli.Put(context.TODO(), key, string(data), clientv3.WithLease(s.leaseId))
	if err != nil {
		if strings.Index(err.Error(), "lease not found") != -1 {
			opt, err := s.cli.Grant(context.TODO(), 5)
			if err != nil {
				log.Println("grant error", err)
			}
			s.leaseId = opt.ID
			_, err = s.cli.Put(context.TODO(), key, string(data), clientv3.WithLease(s.leaseId))
			if err != nil {
				log.Println("UpdateExtInfo error", err)
			}
			return err
		}
		return err
	}
	return nil
}

func (s *Service) heartbeat() {
	tk := time.NewTicker(s.config.TTL - time.Second)
	for s.running {
		<-tk.C
		_, err := s.cli.KeepAliveOnce(context.TODO(), s.leaseId)
		if err != nil {
			log.Println("KeepAliveOnce error", err)
			if strings.Index(err.Error(), "lease not found") != -1 {
				opt, err := s.cli.Grant(context.TODO(), 5)
				if err != nil {
					log.Println("grant error", err)
				}
				s.leaseId = opt.ID
				data, _ := json.Marshal(&ServiceInfo{Name: s.config.Name, Address: s.address, ExtInfo: s.extInfo})
				s.UpdateExtInfo(data)
			}
		}
	}
}
