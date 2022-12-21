package db

import (
	"fmt"
	"sync"
	"time"

	"github.com/alrusov/loadavg"
)

//----------------------------------------------------------------------------------------------------------------------------//

type (
	Stat struct {
		mutex  *sync.Mutex
		period time.Duration
		Stat   map[string]*connStat `json:"stat,omitempty"`
	}

	connStat struct {
		Query *funcStat `json:"query,omitempty"`
		Exec  *funcStat `json:"exec,omitempty"`
	}

	funcStat struct {
		InProgress      int64         `json:"inProgress,omitempty"`
		TotalRequests   int64         `json:"totalRequests,omitempty"`
		TotalDuration   time.Duration `json:"totalDuration,omitempty"`
		DurationAvg     int64         `json:"durationAvg,omitempty"`     // Load average по времени обработки
		LastRequestsAvg float64       `json:"lastRequestsAvg,omitempty"` // Load average по запросам
		LastDurationAvg int64         `json:"lastDurationAvg,omitempty"` // Load average по времени обработки

		requestsLA *loadavg.LoadAvg
		durationLA *loadavg.LoadAvg
	}
)

var (
	stat = &Stat{
		mutex:  new(sync.Mutex),
		period: time.Second * 60,
		Stat:   make(map[string]*connStat, 8),
	}
)

//----------------------------------------------------------------------------------------------------------------------------//

func SetStatPeriod(newPeriod time.Duration) (oldPeriod time.Duration) {
	oldPeriod, stat.period = stat.period, newPeriod
	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func GetStat() *Stat {
	stat.mutex.Lock()
	defer stat.mutex.Unlock()

	for _, s := range stat.Stat {
		s.Query.update()
		s.Exec.update()
	}
	return stat.cloneVals()
}

//----------------------------------------------------------------------------------------------------------------------------//

func newStat(name string) (err error) {
	stat.mutex.Lock()
	defer stat.mutex.Unlock()

	_, exists := stat.Stat[name]
	if exists {
		err = fmt.Errorf(`stat for "%s" already exists`, name)
		return
	}

	stat.Stat[name] = newConnStat()

	return
}

func newConnStat() *connStat {
	return &connStat{
		Query: newFuncStat(),
		Exec:  newFuncStat(),
	}
}

func newFuncStat() *funcStat {
	return &funcStat{
		requestsLA: loadavg.Init(stat.period),
		durationLA: loadavg.Init(stat.period),
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

func (src *Stat) cloneVals() (dst *Stat) {
	// уже залочено
	dst = &Stat{
		mutex:  nil,
		period: 0,
		Stat:   make(map[string]*connStat, len(src.Stat)),
	}

	for name, srcConn := range src.Stat {
		q := *srcConn.Query
		e := *srcConn.Exec
		dst.Stat[name] = &connStat{
			Query: &q,
			Exec:  &e,
		}
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) statBegin(isExec bool) {
	stat.mutex.Lock()
	defer stat.mutex.Unlock()

	c := db.getConnStat()
	if c == nil {
		return
	}

	if isExec {
		c.Exec.begin()
	} else {
		c.Query.begin()
	}
}

func (db *DB) statEnd(isExec bool, duration time.Duration) {
	stat.mutex.Lock()
	defer stat.mutex.Unlock()

	c := db.getConnStat()
	if c == nil {
		return
	}

	if isExec {
		c.Exec.end(duration)
	} else {
		c.Query.end(duration)
	}
}

func (db *DB) getConnStat() (c *connStat) {
	// already locked!

	c, exists := stat.Stat[db.Name]
	if !exists {
		c = nil
		return
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (f *funcStat) begin() {
	// already locked!

	f.InProgress++
	f.TotalRequests++
}

func (f *funcStat) end(duration time.Duration) {
	// уже залочено!

	f.InProgress--
	f.TotalDuration += duration

	f.requestsLA.Add(1)
	f.durationLA.Add(float64(duration))
}

func (f *funcStat) update() {
	// уже залочено!

	if f.TotalRequests > 0 {
		f.DurationAvg = int64(float64(f.TotalDuration) / float64(f.TotalRequests))
	}

	f.LastRequestsAvg = f.requestsLA.Value()
	f.LastDurationAvg = int64(f.durationLA.Value())
}

//----------------------------------------------------------------------------------------------------------------------------//
