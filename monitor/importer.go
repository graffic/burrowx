package monitor

import (
	"time"

	client "github.com/influxdata/influxdb/client/v2"

	log "github.com/cihub/seelog"
	"github.com/sundy-li/burrowx/config"
)

type Importer struct {
	msgs chan *ConsumerFullOffset
	cfg  *config.InfluxDB

	threshold  int
	maxTimeGap int64
	influxdb   client.Client
	stopped    chan struct{}
}

func NewImporter(cfg *config.InfluxDB) (i *Importer, err error) {
	i = &Importer{
		msgs:       make(chan *ConsumerFullOffset, 1000),
		cfg:        cfg,
		threshold:  10,
		maxTimeGap: 10,
		stopped:    make(chan struct{}),
	}
	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     cfg.Hosts,
		Username: cfg.Username,
		Password: cfg.Pwd,
	})
	if err != nil {
		return
	}
	i.influxdb = c
	return
}

func (i *Importer) start() {
	_, err := i.runCmd("create database " + i.cfg.Db)
	if err != nil {
		panic(err)
	}
	go func() {
		bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  i.cfg.Db,
			Precision: "s",
		})
		lastCommit := time.Now().Unix()
		for msg := range i.msgs {
			tags := map[string]string{
				"topic":          msg.Topic,
				"consumer_group": msg.Group,
				"cluster":        msg.Cluster,
			}

			for k, v := range i.cfg.ExtraTags {
				tags[k] = v
			}

			//offset is the sql keyword, so we use offsize
			fields := map[string]interface{}{
				"offsize": msg.Offset,
				"logsize": msg.MaxOffset,
				"lag":     msg.MaxOffset - msg.Offset,
			}

			tm := time.Unix(msg.Timestamp/1000, 0)
			pt, err := client.NewPoint(i.cfg.Measurement, tags, fields, tm)
			if err != nil {
				log.Error("error in add point ", err.Error())
				continue
			}
			bp.AddPoint(pt)
			if len(bp.Points()) > i.threshold || time.Now().Unix()-lastCommit >= i.maxTimeGap {
				err := i.influxdb.Write(bp)
				bp, _ = client.NewBatchPoints(client.BatchPointsConfig{
					Database:  i.cfg.Db,
					Precision: "s",
				})
				lastCommit = time.Now().Unix()
				if err != nil {
					log.Error("error in insert points ", err.Error())
					continue
				}
			}
		}
		i.stopped <- struct{}{}
	}()

}

func (i *Importer) saveMsg(msg *ConsumerFullOffset) {
	i.msgs <- msg
}

func (i *Importer) stop() {
	close(i.msgs)
	<-i.stopped
}

// runCmd method is for influxb querys
func (i *Importer) runCmd(cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: i.cfg.Db,
	}
	if response, err := i.influxdb.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil

}
