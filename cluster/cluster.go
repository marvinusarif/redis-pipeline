package cluster

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

const totalSlots = 16384

type ClusterInterface interface {
	GetMasterIPs() (masterIPs []string)
	GetMasterFromSlaveIP(slaveIP string) (masterIP string, err error)
	GetSlaveIPs() (slaveIPs []string)
	GetConnByAddr(addr string) (conn redis.Conn, isReadOnly bool, err error)
	GetNodeIPBySlot(slot int, readOnly bool) (nodeIP string, err error)
	updateClusterRegistry(redirectionError *RedirError)
	Slot(key string) int
	HandleError(err error)
}

type Cluster struct {
	mu                *sync.RWMutex
	maxConn           int
	dialOption        []redis.DialOption
	CreatePool        func(host string, maxConn int, dialOption ...redis.DialOption) *redis.Pool
	masterBySlaveIPs  map[string]string
	slaveByMasterIPs  map[string][]string
	nodes             map[string]bool
	poolByAddr        map[string]*redis.Pool
	addrSlots         [][]string
	isUpdatingCluster bool
}

type SlotMap struct {
	lowerBoundSlot, upperBoundSlot int
	nodes                          []string
}

func New(allIPs []string, maxConn int, createPool func(host string, maxConn int, dialOption ...redis.DialOption) *redis.Pool, dialOption ...redis.DialOption) ClusterInterface {
	cluster := &Cluster{
		mu:                &sync.RWMutex{},
		maxConn:           maxConn,
		dialOption:        dialOption,
		CreatePool:        createPool,
		masterBySlaveIPs:  make(map[string]string),
		slaveByMasterIPs:  make(map[string][]string),
		poolByAddr:        make(map[string]*redis.Pool),
		nodes:             make(map[string]bool),
		addrSlots:         make([][]string, totalSlots),
		isUpdatingCluster: false,
	}
	//initialize node with masters
	for _, nodeIP := range allIPs {
		//set all nodes as master, we don't know the master/slave yet
		cluster.slaveByMasterIPs[nodeIP] = nil
		//set all nodes to up, we don't know yet
		cluster.nodes[nodeIP] = true
	}

	if err := cluster.InitClusterRegistry(); err != nil {
		log.Println(err)
		panic("cannot init redis cluster")
	}
	return cluster
}

//getClusterSlots ...
func (c *Cluster) getClusterSlotMaps(addr string) (slotMaps []SlotMap, err error) {
	conn, _, err := c.GetConnByAddr(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	reply, err := redis.Values(conn.Do("CLUSTER", "SLOTS"))
	if err != nil {
		return nil, err
	}
	slotMaps = make([]SlotMap, 0, len(reply))
	for len(reply) > 0 {
		var slotRange []interface{}
		reply, err = redis.Scan(reply, &slotRange)
		if err != nil {
			return nil, err
		}

		var LBSlot, UBSlot int
		slotRange, err = redis.Scan(slotRange, &LBSlot, &UBSlot)
		if err != nil {
			return nil, err
		}
		slotMap := SlotMap{
			lowerBoundSlot: LBSlot,
			upperBoundSlot: UBSlot,
			nodes:          nil,
		}
		//give address of masters and slaves
		for len(slotRange) > 0 {
			var nodes []interface{}
			slotRange, err = redis.Scan(slotRange, &nodes)
			if err != nil {
				return nil, err
			}
			var addr string
			var port int
			if _, err = redis.Scan(nodes, &addr, &port); err != nil {
				return nil, err
			}
			slotMap.nodes = append(slotMap.nodes, fmt.Sprintf("%s:%d", addr, port))
		}
		slotMaps = append(slotMaps, slotMap)
	}
	return slotMaps, nil
}

func (c *Cluster) getOrCreatePool(host string) (pool *redis.Pool) {
	c.mu.Lock()
	pool = c.poolByAddr[host]
	if pool == nil {
		//release Lock to increase performance
		c.mu.Unlock()
		newPool := c.CreatePool(host, c.maxConn, c.dialOption...)
		c.mu.Lock()
		if pool = c.poolByAddr[host]; pool == nil {
			c.poolByAddr[host] = newPool
			pool = newPool
		} else {
			//close pool if not used
			defer pool.Close()
		}
	}
	c.mu.Unlock()
	return pool
}

func (c *Cluster) GetNodeIPBySlot(slot int, readOnly bool) (nodeIP string, err error) {
	c.mu.Lock()
	nodes := c.addrSlots[slot]
	c.mu.Unlock()
	//nodes is array of [master node, slave, slave...]
	numNodes := len(nodes)
	if numNodes == 0 {
		return "", fmt.Errorf("no node for slot [%d]", slot)
	}
	//if not readonly then return master
	if !readOnly {
		nodeIP = nodes[0]
	} else {
		//if readonly return random slave
		if numNodes == 1 {
			nodeIP = nodes[0]
		} else if numNodes == 2 {
			nodeIP = nodes[1]
		} else {
			idx := int(time.Now().UnixNano()) % numNodes
			if idx == 0 {
				idx = 1
			}
			nodeIP = nodes[idx]
		}
	}
	return nodeIP, err
}

// getConnFromAddr ...
func (c *Cluster) GetConnByAddr(addr string) (conn redis.Conn, isReadOnly bool, err error) {
	// check if node is alive
	c.mu.RLock()
	if _, ok := c.nodes[addr]; !ok {
		c.mu.RUnlock()
		return nil, false, fmt.Errorf("node %s is down", addr)
	}
	c.mu.RUnlock()

	isReadOnly = c.isNodeReadOnly(addr)
	pool := c.getOrCreatePool(addr)
	conn = pool.Get()
	err = conn.Err()
	if err == nil && isReadOnly {
		_, err = conn.Do("READONLY")
	}
	if err != nil {
		c.updateClusterRegistry(nil)
	}
	return conn, isReadOnly, conn.Err()
}

func (c *Cluster) InitClusterRegistry() (err error) {
	c.mu.Lock()
	c.isUpdatingCluster = true
	c.mu.Unlock()
	return c.initClusterRegistry()
}

func (c *Cluster) initClusterRegistry() error {
	allIPs := c.GetSlaveIPs()
	allIPs = append(allIPs, c.GetMasterIPs()...)
	for _, nodeIP := range allIPs {
		slotMaps, err := c.getClusterSlotMaps(nodeIP)
		if err == nil {
			c.mu.Lock()
			//reinit nodes, masters and slaves
			c.nodes, c.masterBySlaveIPs, c.slaveByMasterIPs = make(map[string]bool), make(map[string]string), make(map[string][]string)
			for _, slotMap := range slotMaps {
				for i, node := range slotMap.nodes {
					if i == 0 {
						if len(slotMap.nodes) > 1 {
							c.slaveByMasterIPs[node] = slotMap.nodes[1:]
						} else {
							c.slaveByMasterIPs[node] = nil
						}
					} else {
						c.masterBySlaveIPs[node] = slotMap.nodes[0]
					}
					if ok := c.nodes[node]; !ok {
						c.nodes[node] = true
					}
				}
				//map slot map to addresSlots
				for i := slotMap.lowerBoundSlot; i <= slotMap.upperBoundSlot; i++ {
					c.addrSlots[i] = slotMap.nodes
				}
			}

			//remove all connection from clusters
			for masterIP, slaveIPs := range c.slaveByMasterIPs {
				if p := c.poolByAddr[masterIP]; p != nil {
					p.Close()
					delete(c.poolByAddr, masterIP)
				}
				for _, slaveIP := range slaveIPs {
					if p := c.poolByAddr[slaveIP]; p != nil {
						p.Close()
						delete(c.poolByAddr, slaveIP)
					}
				}
			}
			//set false on Refresh
			c.isUpdatingCluster = false
			c.mu.Unlock()
			// go c.printNodes()
			// go c.printNodesAvailability()
			// go c.printSlots()
			return nil
		}
	}
	c.mu.Lock()
	c.isUpdatingCluster = false
	c.mu.Unlock()
	return fmt.Errorf("all nodes is down")
}

func (c *Cluster) updateClusterRegistry(redirectionError *RedirError) {
	c.mu.Lock()
	//help temporarily when slot has moved
	if redirectionError != nil {
		if current := c.addrSlots[redirectionError.NewSlot]; len(current) == 0 || current[0] != redirectionError.Addr {
			c.addrSlots[redirectionError.NewSlot] = []string{redirectionError.Addr}
		}
	}
	if !c.isUpdatingCluster {
		c.isUpdatingCluster = true
		go c.initClusterRegistry()
	}
	c.mu.Unlock()
}

func (c *Cluster) GetMasterIPs() (masterIPs []string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for masterIP := range c.slaveByMasterIPs {
		masterIPs = append(masterIPs, masterIP)
	}
	return masterIPs
}

func (c *Cluster) GetSlaveIPs() (slaveIPs []string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for slaveIP := range c.masterBySlaveIPs {
		slaveIPs = append(slaveIPs, slaveIP)
	}
	return slaveIPs
}

// Stats returns the current statistics for all pools. Keys are node's addresses.
func (c *Cluster) Stats() map[string]redis.PoolStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	stats := make(map[string]redis.PoolStats, len(c.poolByAddr))
	for address, pool := range c.poolByAddr {
		stats[address] = pool.Stats()
	}
	return stats
}

func (c *Cluster) isNodeReadOnly(nodeIP string) (isReadOnly bool) {
	isReadOnly = true
	c.mu.RLock()
	for masterIP := range c.slaveByMasterIPs {
		if nodeIP == masterIP {
			isReadOnly = false
			break
		}
	}
	c.mu.RUnlock()
	return isReadOnly
}

func (c *Cluster) GetMasterFromSlaveIP(slaveIP string) (masterIP string, err error) {
	c.mu.RLock()
	masterIP, ok := c.masterBySlaveIPs[slaveIP]
	c.mu.RUnlock()
	if !ok {
		return "", errors.New("master IP by slave IP not found")
	}
	return masterIP, nil
}
func (c *Cluster) printSlots() {
	c.mu.RLock()
	addrSlots := c.addrSlots
	c.mu.RUnlock()
	for i, ips := range addrSlots {
		fmt.Printf("%d node : %v", i, ips)
	}
}

func (c *Cluster) printNodesAvailability() {
	fmt.Println("Cluster Availability")
	c.mu.RLock()
	nodes := c.nodes
	c.mu.RUnlock()
	for node := range nodes {
		fmt.Println(node)
	}
}
func (c *Cluster) printNodes() {
	fmt.Println("Reinit Cluster Registry")
	fmt.Println("master IPs :", c.GetMasterIPs())
	fmt.Println("slave IPs:", c.GetSlaveIPs())
}

func (c *Cluster) HandleError(err error) {
	redisError := ParseRedir(err)
	c.updateClusterRegistry(redisError)
}
