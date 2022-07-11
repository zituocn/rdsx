package goredis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/zituocn/gow/lib/logy"
)

var (
	dbs           map[string]*redis.Client
	defaultDBName string
	ctx           = context.Background()
)

// RedisConfig redis config struct
type RedisConfig struct {
	Name     string //name
	DB       int    //redis db
	Host     string //redis host
	Port     int    //redis host
	Username string //redis username if need
	Password string //redis password
	Pool     int    //pool size
}

// InitDefaultDB init a rdb to map
func InitDefaultDB(db *RedisConfig) (err error) {
	if db == nil {
		err = fmt.Errorf("[redis] connection configuration error")
		return
	}
	defaultDBName = db.Name
	dbs = make(map[string]*redis.Client, 1)
	newRedis(db)
	return
}

// GetRDB returns a *redis.Client
func GetRDB() *redis.Client {
	m, ok := dbs[defaultDBName]
	if !ok {
		logy.Panic("[redis] not initialized, please refer to the instructions for use")
	}
	return m
}

// InitDB init multiple rdb to map
func InitDB(list []*RedisConfig) (err error) {
	if len(list) == 0 {
		err = fmt.Errorf("[redis] connection configuration error")
		return
	}
	dbs = make(map[string]*redis.Client, len(list))
	for _, item := range list {
		newRedis(item)
	}

	return
}

// GetRDBByName get rdb by name
func GetRDBByName(name string) *redis.Client {
	m, ok := dbs[name]
	if !ok {
		logy.Panic("[redis] not initialized, please refer to the instructions for use")
	}
	return m
}

/*
private
*/

func (m *RedisConfig) string() string {
	return fmt.Sprintf("redis://%s:%s@%s:%d/%d", m.Name, m.Password, m.Host, m.Port, m.DB)
}

// newRedis use redisConfig make dbs
func newRedis(rc *RedisConfig) {
	var (
		rdb *redis.Client
	)
	if rc.Host == "" || rc.Port == 0 || rc.Name == "" {
		logy.Panicf("[redis]-[%s] wrong configuration information", rc.Name)
		return
	}
	if rc.DB < 0 {
		rc.DB = 0
	}
	if rc.Pool < 0 {
		rc.Pool = 10
	}
	opt := &redis.Options{
		Addr:         fmt.Sprintf("%s:%d", rc.Host, rc.Port),
		Username:     rc.Username,
		Password:     rc.Password,
		DB:           rc.DB,
		PoolSize:     rc.Pool,
		IdleTimeout:  30 * time.Second,
		DialTimeout:  5 * time.Second,
		MaxRetries:   -1,
		MinIdleConns: 10,
	}

	rdb = redis.NewClient(opt)

	// COMMAND ping
	for _, err := rdb.Ping(ctx).Result(); err != nil; {
		logy.Errorf("[redis]-%s connecting error: %s", rc.string(), err.Error())
		time.Sleep(5 * time.Second)
	}

	dbs[rc.Name] = rdb
}
