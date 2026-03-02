package collector

import (
	"regexp"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sckyzo/slurm_exporter/internal/logger"
)

// ReservationNodesMetrics holds node state counts per reservation
type ReservationNodesMetrics struct {
	alloc map[string]float64 // key: "pod|rack"
	idle  map[string]float64
	mix   map[string]float64
	down  map[string]float64
	drain map[string]float64
	other map[string]float64
}

/*
parseNodeLocation extracts pod and rack from node name.
Expected format: pX-rYY-ctZZ (e.g., p1-r01-ct01)
Returns: pod, rack
*/
func parseNodeLocation(nodeName string) (string, string) {
	// Match pattern: pX-rYY-...
	re := regexp.MustCompile(`^(p\d+)-(r\d+)-`)
	matches := re.FindStringSubmatch(nodeName)
	if len(matches) >= 3 {
		return matches[1], matches[2]
	}
	return "unknown", "unknown"
}

/*
ParseReservationNodesMetrics parses scontrol node output to get reservation and state.
Expected input: multi-line scontrol show nodes output
*/
func ParseReservationNodesMetrics(input []byte) map[string]*ReservationNodesMetrics {
	reservations := make(map[string]*ReservationNodesMetrics)
	lines := strings.Split(string(input), "\n")

	var currentNodeName string
	var currentReservation string
	var currentState string
	var hasData bool

	nodeNameRe := regexp.MustCompile(`^NodeName=(\S+)`)
	resvRe := regexp.MustCompile(`ReservationName=(\S+)`)
	stateRe := regexp.MustCompile(`State=(\S+)`)

	// Helper function to process current node
	processNode := func() {
		if currentReservation != "" && currentState != "" && currentNodeName != "" {
			// Initialize reservation if it doesn't exist
			if reservations[currentReservation] == nil {
				reservations[currentReservation] = &ReservationNodesMetrics{
					alloc: make(map[string]float64),
					idle:  make(map[string]float64),
					mix:   make(map[string]float64),
					down:  make(map[string]float64),
					drain: make(map[string]float64),
					other: make(map[string]float64),
				}
			}

			// Extract pod and rack from node name
			pod, rack := parseNodeLocation(currentNodeName)
			key := pod + "|" + rack

			// Categorize state
			state := strings.ToLower(currentState)
			allocRe := regexp.MustCompile(`^alloc`)
			idleRe := regexp.MustCompile(`^idle`)
			mixRe := regexp.MustCompile(`^mix`)
			downRe := regexp.MustCompile(`^down`)
			drainRe := regexp.MustCompile(`^drain`)

			switch {
			case allocRe.MatchString(state):
				reservations[currentReservation].alloc[key]++
			case idleRe.MatchString(state):
				reservations[currentReservation].idle[key]++
			case mixRe.MatchString(state):
				reservations[currentReservation].mix[key]++
			case downRe.MatchString(state):
				reservations[currentReservation].down[key]++
			case drainRe.MatchString(state):
				reservations[currentReservation].drain[key]++
			default:
				reservations[currentReservation].other[key]++
			}
		}
	}

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// When we hit a new node, process the previous one
		if strings.HasPrefix(line, "NodeName=") {
			if hasData {
				processNode()
			}
			// Extract node name
			if matches := nodeNameRe.FindStringSubmatch(line); len(matches) > 1 {
				currentNodeName = matches[1]
			}
			// Reset for new node
			currentReservation = ""
			currentState = ""
			hasData = true
		}

		// Extract reservation name
		if matches := resvRe.FindStringSubmatch(line); len(matches) > 1 {
			currentReservation = matches[1]
		}

		// Extract state
		if matches := stateRe.FindStringSubmatch(line); len(matches) > 1 {
			currentState = matches[1]
		}
	}

	// Process the last node
	if hasData {
		processNode()
	}

	return reservations
}

/*
ReservationNodesData executes scontrol to get all nodes with their reservation info.
*/
func ReservationNodesData(logger *logger.Logger) ([]byte, error) {
	return Execute(logger, "scontrol", []string{"show", "nodes", "-o"})
}

/*
ReservationNodesGetMetrics retrieves and parses node metrics by reservation.
*/
func ReservationNodesGetMetrics(logger *logger.Logger) (map[string]*ReservationNodesMetrics, error) {
	data, err := ReservationNodesData(logger)
	if err != nil {
		return nil, err
	}
	return ParseReservationNodesMetrics(data), nil
}

// NewReservationNodesCollector creates a new reservation nodes metrics collector
func NewReservationNodesCollector(logger *logger.Logger) *ReservationNodesCollector {
	labels := []string{"reservation", "pod", "rack"}
	return &ReservationNodesCollector{
		alloc:  prometheus.NewDesc("slurm_reservation_nodes_alloc", "Allocated nodes in reservation", labels, nil),
		idle:   prometheus.NewDesc("slurm_reservation_nodes_idle", "Idle nodes in reservation", labels, nil),
		mix:    prometheus.NewDesc("slurm_reservation_nodes_mix", "Mixed nodes in reservation", labels, nil),
		down:   prometheus.NewDesc("slurm_reservation_nodes_down", "Down nodes in reservation", labels, nil),
		drain:  prometheus.NewDesc("slurm_reservation_nodes_drain", "Drained nodes in reservation", labels, nil),
		other:  prometheus.NewDesc("slurm_reservation_nodes_other", "Other state nodes in reservation", labels, nil),
		logger: logger,
	}
}

// ReservationNodesCollector implements the Prometheus Collector interface
type ReservationNodesCollector struct {
	alloc  *prometheus.Desc
	idle   *prometheus.Desc
	mix    *prometheus.Desc
	down   *prometheus.Desc
	drain  *prometheus.Desc
	other  *prometheus.Desc
	logger *logger.Logger
}

// Describe sends the descriptors of each metric over to the provided channel
func (rnc *ReservationNodesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- rnc.alloc
	ch <- rnc.idle
	ch <- rnc.mix
	ch <- rnc.down
	ch <- rnc.drain
	ch <- rnc.other
}

// Collect fetches the node metrics by reservation and sends them to Prometheus
func (rnc *ReservationNodesCollector) Collect(ch chan<- prometheus.Metric) {
	metrics, err := ReservationNodesGetMetrics(rnc.logger)
	if err != nil {
		rnc.logger.Error("Failed to get reservation nodes metrics", "err", err)
		return
	}

	for reservation, rm := range metrics {
		// Emit metrics for each pod|rack combination
		for key, count := range rm.alloc {
			if count > 0 {
				parts := strings.Split(key, "|")
				if len(parts) == 2 {
					ch <- prometheus.MustNewConstMetric(rnc.alloc, prometheus.GaugeValue, count, reservation, parts[0], parts[1])
				}
			}
		}
		for key, count := range rm.idle {
			if count > 0 {
				parts := strings.Split(key, "|")
				if len(parts) == 2 {
					ch <- prometheus.MustNewConstMetric(rnc.idle, prometheus.GaugeValue, count, reservation, parts[0], parts[1])
				}
			}
		}
		for key, count := range rm.mix {
			if count > 0 {
				parts := strings.Split(key, "|")
				if len(parts) == 2 {
					ch <- prometheus.MustNewConstMetric(rnc.mix, prometheus.GaugeValue, count, reservation, parts[0], parts[1])
				}
			}
		}
		for key, count := range rm.down {
			if count > 0 {
				parts := strings.Split(key, "|")
				if len(parts) == 2 {
					ch <- prometheus.MustNewConstMetric(rnc.down, prometheus.GaugeValue, count, reservation, parts[0], parts[1])
				}
			}
		}
		for key, count := range rm.drain {
			if count > 0 {
				parts := strings.Split(key, "|")
				if len(parts) == 2 {
					ch <- prometheus.MustNewConstMetric(rnc.drain, prometheus.GaugeValue, count, reservation, parts[0], parts[1])
				}
			}
		}
		for key, count := range rm.other {
			if count > 0 {
				parts := strings.Split(key, "|")
				if len(parts) == 2 {
					ch <- prometheus.MustNewConstMetric(rnc.other, prometheus.GaugeValue, count, reservation, parts[0], parts[1])
				}
			}
		}
	}
}
