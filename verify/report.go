package main

const (
	stageCollect      = "collect"
	stageDecode       = "decode"
	stageReEncode     = "reencode"
	stageSecondDecode = "second-decode"
	stageSemantic     = "semantic-compare"
	stageAOF          = "aof"
)

const (
	statusPass         = "pass"          // fully lossless round-trip
	statusExpectedLoss = "expected-loss" // structured, semantically safe representation loss
	statusBlocker      = "blocker"       // stage error or semantic mismatch
	statusNegativeOK   = "negative-ok"   // declared failure observed exactly
)

// fixtureResult is one per discovered .rdb file.
type fixtureResult struct {
	Path     string     `json:"path"`
	Version  string     `json:"rdbVersion"`
	Types    []string   `json:"dataTypes"`
	Features []string   `json:"features"`
	Status   string     `json:"status"`
	Stage    string     `json:"stage,omitempty"`
	Error    string     `json:"error,omitempty"`
	Losses   []Loss     `json:"expectedLoss,omitempty"`
	AOF      *aofResult `json:"aof,omitempty"`
	Manifest bool       `json:"declaredNegative,omitempty"`
	Negative string     `json:"negativeMatch,omitempty"`
}

// report is the structured machine-readable gate output.
type report struct {
	FixtureCount int             `json:"fixtureCount"`
	Pass         int             `json:"pass"`
	ExpectedLoss int             `json:"expectedLoss"`
	Blocker      int             `json:"blocker"`
	NegativeOK   int             `json:"negativeOk"`
	Results      []fixtureResult `json:"results"`
}
