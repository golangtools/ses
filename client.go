package ses

import (
	"context"
	"encoding/json"
	"errors"
	"go.etcd.io/etcd/clientv3"
	"log"
	"time"
)

type Handler func(sv *ServiceInfo)

type Client struct {
	dir                  string
	config               *EtcdConfig
	cli                  *clientv3.Client
	cancel               context.CancelFunc
	handleServiceOffline Handler
	handleServiceOnline  Handler
}

func NewClient(dir string, config *EtcdConfig) *Client {
	cl := &Client{dir: dir, config: config}
	return cl
}

func (c *Client) SetOnlineHandler(h Handler) {
	c.handleServiceOnline = h
}

func (c *Client) SetOfflineHandler(h Handler) {
	c.handleServiceOffline = h
}

func (c *Client) CancelWatch() {
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
}

func (c *Client) Watch() error {
	if c.config == nil {
		return errors.New("config can not nil")
	}

	if c.dir == "" {
		return errors.New("the service base dir can not empty")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   c.config.Endpoints,
		DialTimeout: 5 * time.Second,
		Username:    c.config.User,
		Password:    c.config.Password,
	})
	if err != nil {
		return err
	}
	c.cli = cli

	mp := make(map[string]*ServiceInfo)

	ret, err := cli.Get(context.Background(), c.dir, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, v := range ret.Kvs {
		var sev *ServiceInfo
		err := json.Unmarshal(v.Value, &sev)
		if err != nil {
			return err
		}
		mp[string(v.Key)] = sev
		if c.handleServiceOnline != nil {
			c.handleServiceOnline(sev)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	wc := cli.Watch(ctx, c.dir, clientv3.WithPrefix())

	go func() {
		for {
			select {
			case resp := <-wc:
				for _, e := range resp.Events {
					if e.Type == clientv3.EventTypeDelete {
						if c.handleServiceOffline != nil {
							key := string(e.Kv.Key)
							sev := mp[key]
							if sev != nil {
								c.handleServiceOffline(sev)
								delete(mp, key)
							}
						}
					} else {
						var sev *ServiceInfo
						err := json.Unmarshal(e.Kv.Value, &sev)
						if err != nil {
							log.Println("Unmarshal e.Kv.Value", err, string(e.Kv.Value))
							continue
						}
						mp[string(e.Kv.Key)] = sev
						if c.handleServiceOnline != nil {
							c.handleServiceOnline(sev)
						}
					}
				}
			}
		}
	}()
	return nil
}
