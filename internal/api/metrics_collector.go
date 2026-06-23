package api

import (
	"time"

	"github.com/digitalentity/replistore/internal/fuse"
	"github.com/prometheus/client_golang/prometheus"
)

// replFSCollector exposes live RepliStore state as Prometheus metrics. Every
// value is read from the underlying structures at scrape time, so gauges always
// reflect the current instant rather than a cached snapshot.
type replFSCollector struct {
	srv *Server

	up           *prometheus.Desc
	buildInfo    *prometheus.Desc
	uptime       *prometheus.Desc
	backendUp    *prometheus.Desc
	backendFree  *prometheus.Desc
	backendTotal *prometheus.Desc

	clusterCurrent  *prometheus.Desc
	clusterExpected *prometheus.Desc
	clusterLocks    *prometheus.Desc

	rawTotal       *prometheus.Desc
	rawUsed        *prometheus.Desc
	rawFree        *prometheus.Desc
	amortizedTotal *prometheus.Desc
	amortizedUsed  *prometheus.Desc
	amortizedFree  *prometheus.Desc
	replFactor     *prometheus.Desc

	cacheNodes  *prometheus.Desc
	cacheDirs   *prometheus.Desc
	cacheHits   *prometheus.Desc
	cacheMisses *prometheus.Desc

	repairScrubActive  *prometheus.Desc
	repairScrubDur     *prometheus.Desc
	repairDegraded     *prometheus.Desc
	repairDivergent    *prometheus.Desc
	repairActiveRepair *prometheus.Desc

	descs []*prometheus.Desc
}

func newReplFSCollector(srv *Server) *replFSCollector {
	const ns = "replistore"
	backendLabels := []string{"backend", "type"}

	c := &replFSCollector{
		srv: srv,
		up: prometheus.NewDesc(ns+"_up",
			"Whether the RepliStore instance is up (always 1 when scrapeable).", nil, nil),
		buildInfo: prometheus.NewDesc(ns+"_build_info",
			"Build information; the value is always 1.", []string{"version"}, nil),
		uptime: prometheus.NewDesc(ns+"_uptime_seconds",
			"Seconds since the instance started.", nil, nil),
		backendUp: prometheus.NewDesc(ns+"_backend_up",
			"Whether a backend is healthy (1) or down (0).", backendLabels, nil),
		backendFree: prometheus.NewDesc(ns+"_backend_free_bytes",
			"Free space on a backend in bytes.", backendLabels, nil),
		backendTotal: prometheus.NewDesc(ns+"_backend_total_bytes",
			"Total space on a backend in bytes.", backendLabels, nil),

		clusterCurrent: prometheus.NewDesc(ns+"_cluster_size_current",
			"Current number of nodes in the cluster (peers plus self).", nil, nil),
		clusterExpected: prometheus.NewDesc(ns+"_cluster_size_expected",
			"Expected number of nodes in the cluster.", nil, nil),
		clusterLocks: prometheus.NewDesc(ns+"_cluster_active_locks",
			"Number of active cluster locks.", nil, nil),

		rawTotal: prometheus.NewDesc(ns+"_cluster_raw_total_bytes",
			"Raw total space across all backends in bytes.", nil, nil),
		rawUsed: prometheus.NewDesc(ns+"_cluster_raw_used_bytes",
			"Raw used space across all backends in bytes.", nil, nil),
		rawFree: prometheus.NewDesc(ns+"_cluster_raw_free_bytes",
			"Raw free space across all backends in bytes.", nil, nil),
		amortizedTotal: prometheus.NewDesc(ns+"_cluster_amortized_total_bytes",
			"Replication-factor-adjusted total space in bytes.", nil, nil),
		amortizedUsed: prometheus.NewDesc(ns+"_cluster_amortized_used_bytes",
			"Logical used space in bytes.", nil, nil),
		amortizedFree: prometheus.NewDesc(ns+"_cluster_amortized_free_bytes",
			"Replication-factor-adjusted free space in bytes.", nil, nil),
		replFactor: prometheus.NewDesc(ns+"_replication_factor",
			"Configured replication factor.", nil, nil),

		cacheNodes: prometheus.NewDesc(ns+"_cache_nodes",
			"Number of nodes currently held in the metadata cache.", nil, nil),
		cacheDirs: prometheus.NewDesc(ns+"_cache_directories_indexed",
			"Number of directories fully indexed in the cache.", nil, nil),
		cacheHits: prometheus.NewDesc(ns+"_cache_hits_total",
			"Total metadata cache hits.", nil, nil),
		cacheMisses: prometheus.NewDesc(ns+"_cache_misses_total",
			"Total metadata cache misses.", nil, nil),

		repairScrubActive: prometheus.NewDesc(ns+"_repair_scrub_active",
			"Whether a scrub is currently running (1) or not (0).", nil, nil),
		repairScrubDur: prometheus.NewDesc(ns+"_repair_last_scrub_duration_seconds",
			"Duration of the last completed scrub in seconds.", nil, nil),
		repairDegraded: prometheus.NewDesc(ns+"_repair_degraded_files",
			"Number of files with fewer than the desired number of replicas.", nil, nil),
		repairDivergent: prometheus.NewDesc(ns+"_repair_divergent_files",
			"Number of files whose replicas have diverged.", nil, nil),
		repairActiveRepair: prometheus.NewDesc(ns+"_repair_active_repairs",
			"Number of repairs currently in progress.", nil, nil),
	}

	c.descs = []*prometheus.Desc{
		c.up, c.buildInfo, c.uptime,
		c.backendUp, c.backendFree, c.backendTotal,
		c.clusterCurrent, c.clusterExpected, c.clusterLocks,
		c.rawTotal, c.rawUsed, c.rawFree,
		c.amortizedTotal, c.amortizedUsed, c.amortizedFree, c.replFactor,
		c.cacheNodes, c.cacheDirs, c.cacheHits, c.cacheMisses,
		c.repairScrubActive, c.repairScrubDur, c.repairDegraded,
		c.repairDivergent, c.repairActiveRepair,
	}

	return c
}

// Describe implements prometheus.Collector by emitting descriptors directly.
// It must NOT use prometheus.DescribeByCollect, which would run Collect at
// registration time (during server startup) and block on live backend network
// queries (#3).
func (c *replFSCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.descs {
		ch <- d
	}
}

// Collect implements prometheus.Collector, reading all values live.
func (c *replFSCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1)
	ch <- prometheus.MustNewConstMetric(c.buildInfo, prometheus.GaugeValue, 1, c.srv.version)
	ch <- prometheus.MustNewConstMetric(c.uptime, prometheus.GaugeValue, time.Since(c.srv.startTime).Seconds())

	c.collectBackends(ch)
	c.collectCluster(ch)
	c.collectCache(ch)
	c.collectRepair(ch)
}

func (c *replFSCollector) collectBackends(ch chan<- prometheus.Metric) {
	fs := c.srv.replFS
	for name, b := range fs.Backends {
		btype := b.GetType()
		healthy := true
		if fs.HealthMonitor != nil {
			healthy = fs.HealthMonitor.IsHealthy(name)
		}

		up := 0.0
		if healthy {
			up = 1
		}
		ch <- prometheus.MustNewConstMetric(c.backendUp, prometheus.GaugeValue, up, name, btype)

		if healthy {
			if free, err := b.GetFreeSpace(); err == nil {
				ch <- prometheus.MustNewConstMetric(c.backendFree, prometheus.GaugeValue, float64(free), name, btype)
			}
			if total, err := b.GetTotalSpace(); err == nil {
				ch <- prometheus.MustNewConstMetric(c.backendTotal, prometheus.GaugeValue, float64(total), name, btype)
			}
		}
	}
}

func (c *replFSCollector) collectCluster(ch chan<- prometheus.Metric) {
	fs := c.srv.replFS

	expected := 1
	currentSize := 1
	if fs.Discovery != nil {
		currentSize = len(fs.Discovery.GetPeersStatus()) + 1
	}
	if fs.LockManager != nil {
		expected = fs.LockManager.ExpectedClusterSize
		ch <- prometheus.MustNewConstMetric(c.clusterLocks, prometheus.GaugeValue,
			float64(len(fs.LockManager.GetActiveLocks())))
	}
	ch <- prometheus.MustNewConstMetric(c.clusterCurrent, prometheus.GaugeValue, float64(currentSize))
	ch <- prometheus.MustNewConstMetric(c.clusterExpected, prometheus.GaugeValue, float64(expected))

	var rawTotal, rawFree uint64
	for name, b := range fs.Backends {
		// Skip backends known to be down: querying an offline SMB share would
		// block the scrape (#2).
		if fs.HealthMonitor != nil && !fs.HealthMonitor.IsHealthy(name) {
			continue
		}
		// Require both calls to succeed before adding either, so rawFree can
		// never exceed rawTotal and underflow rawUsed (#1).
		total, terr := b.GetTotalSpace()
		free, ferr := b.GetFreeSpace()
		if terr != nil || ferr != nil {
			continue
		}
		rawTotal += total
		rawFree += free
	}
	// Defensive: a backend reporting free > total must not underflow.
	var rawUsed uint64
	if rawTotal >= rawFree {
		rawUsed = rawTotal - rawFree
	}

	rf := fs.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}
	logicalUsed := max(0, fs.Cache.GetLogicalUsedSpace())

	ch <- prometheus.MustNewConstMetric(c.rawTotal, prometheus.GaugeValue, float64(rawTotal))
	ch <- prometheus.MustNewConstMetric(c.rawUsed, prometheus.GaugeValue, float64(rawUsed))
	ch <- prometheus.MustNewConstMetric(c.rawFree, prometheus.GaugeValue, float64(rawFree))
	ch <- prometheus.MustNewConstMetric(c.amortizedTotal, prometheus.GaugeValue, float64(rawTotal/uint64(rf)))
	ch <- prometheus.MustNewConstMetric(c.amortizedUsed, prometheus.GaugeValue, float64(logicalUsed))
	ch <- prometheus.MustNewConstMetric(c.amortizedFree, prometheus.GaugeValue, float64(rawFree/uint64(rf)))
	ch <- prometheus.MustNewConstMetric(c.replFactor, prometheus.GaugeValue, float64(rf))
}

func (c *replFSCollector) collectCache(ch chan<- prometheus.Metric) {
	cache := c.srv.replFS.Cache
	totalNodes, dirsIndexed := cache.GetStats()
	ch <- prometheus.MustNewConstMetric(c.cacheNodes, prometheus.GaugeValue, float64(totalNodes))
	ch <- prometheus.MustNewConstMetric(c.cacheDirs, prometheus.GaugeValue, float64(dirsIndexed))
	ch <- prometheus.MustNewConstMetric(c.cacheHits, prometheus.CounterValue, float64(cache.Hits.Load()))
	ch <- prometheus.MustNewConstMetric(c.cacheMisses, prometheus.CounterValue, float64(cache.Misses.Load()))
}

// collectRepair always emits the repair series. With no repair manager the
// values are zero rather than absent, so alerting can distinguish "healthy" from
// "not scraped".
func (c *replFSCollector) collectRepair(ch chan<- prometheus.Metric) {
	var st fuse.RepairStatus
	if c.srv.repairMgr != nil {
		st = c.srv.repairMgr.GetStatus()
	}

	scrub := 0.0
	if st.ScrubActive {
		scrub = 1
	}
	ch <- prometheus.MustNewConstMetric(c.repairScrubActive, prometheus.GaugeValue, scrub)
	ch <- prometheus.MustNewConstMetric(c.repairScrubDur, prometheus.GaugeValue, st.LastScrubDurationSeconds)
	ch <- prometheus.MustNewConstMetric(c.repairDegraded, prometheus.GaugeValue, float64(st.DegradedFilesCount))
	ch <- prometheus.MustNewConstMetric(c.repairDivergent, prometheus.GaugeValue, float64(st.DivergentFilesCount))
	ch <- prometheus.MustNewConstMetric(c.repairActiveRepair, prometheus.GaugeValue, float64(len(st.ActiveRepairs)))
}
