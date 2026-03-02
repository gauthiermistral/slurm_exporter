package collector

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sckyzo/slurm_exporter/internal/logger"
)

/*
AccountsData executes the squeue command to retrieve job information by account.
Expected squeue output format: "%A|%a|%T|%C|%b" (Job ID|Account|State|CPUs|TRES).
*/
func AccountsData(logger *logger.Logger) ([]byte, error) {
	return Execute(logger, "squeue", []string{"-a", "-r", "-h", "-o", "%A|%a|%T|%C|%b"})
}

type JobMetrics struct {
	pending      float64
	running      float64
	running_cpus float64
	suspended    float64
	running_gpus float64
}

/*
ParseAccountsMetrics parses the output of the squeue command for account-specific job metrics.
It expects input in the format: "JobID|Account|State|CPUs|TRES".
*/
func ParseAccountsMetrics(input []byte) map[string]*JobMetrics {
	accounts := make(map[string]*JobMetrics)
	lines := strings.Split(string(input), "\n")
	for _, line := range lines {
		if strings.Contains(line, "|") {
			fields := strings.Split(line, "|")
			account := fields[1]
			_, key := accounts[account]
			if !key {
				accounts[account] = &JobMetrics{0, 0, 0, 0, 0}
			}
			state := fields[2]
			state = strings.ToLower(state)
			cpus, _ := strconv.ParseFloat(fields[3], 64)
			pending := regexp.MustCompile(`^pending`)
			running := regexp.MustCompile(`^running`)
			suspended := regexp.MustCompile(`^suspended`)
			switch {
			case pending.MatchString(state):
				accounts[account].pending++
			case running.MatchString(state):
				accounts[account].running++
				accounts[account].running_cpus += cpus
				// Parse GPU count from TRES field (format: "gres/gpu=N" or "gres/gpu:type=N")
				if len(fields) > 4 {
					tres := fields[4]
					gpus := parseGPUsFromTRES(tres)
					accounts[account].running_gpus += gpus
				}
			case suspended.MatchString(state):
				accounts[account].suspended++
			}
		}
	}
	return accounts
}

/*
parseGPUsFromTRES extracts the GPU count from TRES string.
Expected formats:
  - "gres/gpu:4" (simple format)
  - "gres/gpu:a100:2" (with GPU type)
  - "gres/gpu=4" (alternative format)
  - "gres/gpu:a100=2" (mixed format)
  - "billing=10,cpu=8,gres/gpu:4,mem=32G,node=1" (full TRES)
*/
func parseGPUsFromTRES(tres string) float64 {
	// Match gres/gpu followed by colon or equals and a number
	// Handles: gres/gpu:4, gres/gpu:a100:2, gres/gpu=4, gres/gpu:a100=2
	re := regexp.MustCompile(`gres/gpu[^,\s]*[:\=](\d+)`)
	matches := re.FindStringSubmatch(tres)
	if len(matches) > 1 {
		count, err := strconv.ParseFloat(matches[1], 64)
		if err == nil {
			return count
		}
	}
	return 0
}

type AccountsCollector struct {
	pending      *prometheus.Desc
	running      *prometheus.Desc
	running_cpus *prometheus.Desc
	suspended    *prometheus.Desc
	running_gpus *prometheus.Desc
	logger       *logger.Logger
}

func NewAccountsCollector(logger *logger.Logger) *AccountsCollector {
	labels := []string{"account"}
	return &AccountsCollector{
		pending:      prometheus.NewDesc("slurm_account_jobs_pending", "Pending jobs for account", labels, nil),
		running:      prometheus.NewDesc("slurm_account_jobs_running", "Running jobs for account", labels, nil),
		running_cpus: prometheus.NewDesc("slurm_account_cpus_running", "Running cpus for account", labels, nil),
		suspended:    prometheus.NewDesc("slurm_account_jobs_suspended", "Suspended jobs for account", labels, nil),
		running_gpus: prometheus.NewDesc("slurm_account_gpus_running", "Running GPUs for account", labels, nil),
		logger:       logger,
	}
}

func (ac *AccountsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- ac.pending
	ch <- ac.running
	ch <- ac.running_cpus
	ch <- ac.suspended
	ch <- ac.running_gpus
}

func (ac *AccountsCollector) Collect(ch chan<- prometheus.Metric) {
	data, err := AccountsData(ac.logger)
	if err != nil {
		ac.logger.Error("Failed to get accounts data", "err", err)
		return
	}
	am := ParseAccountsMetrics(data)
	for a := range am {
		if am[a].pending > 0 {
			ch <- prometheus.MustNewConstMetric(ac.pending, prometheus.GaugeValue, am[a].pending, a)
		}
		if am[a].running > 0 {
			ch <- prometheus.MustNewConstMetric(ac.running, prometheus.GaugeValue, am[a].running, a)
		}
		if am[a].running_cpus > 0 {
			ch <- prometheus.MustNewConstMetric(ac.running_cpus, prometheus.GaugeValue, am[a].running_cpus, a)
		}
		if am[a].running_gpus > 0 {
			ch <- prometheus.MustNewConstMetric(ac.running_gpus, prometheus.GaugeValue, am[a].running_gpus, a)
		}
		if am[a].suspended > 0 {
			ch <- prometheus.MustNewConstMetric(ac.suspended, prometheus.GaugeValue, am[a].suspended, a)
		}
	}
}
