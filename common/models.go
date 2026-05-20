package common

type ClientData struct {
    ClientID       string  `json:"client_id"`
    Age            int     `json:"age"`
    Income         float64 `json:"income"`
    EmploymentType string  `json:"employment_type"`
    CreditHistory  string  `json:"credit_history"`
}

type IncomeAnalysis struct {
    StabilityScore float64 `json:"stability_score"`
    DebtToIncome   float64 `json:"dti"`
    ApprovedAmount float64 `json:"approved_amount"`
}

type RiskAssessment struct {
    RiskScore float64  `json:"risk_score"`
    RiskLevel string   `json:"risk_level"`
    Factors   []string `json:"factors"`
}

type Decision struct {
    Approved bool    `json:"approved"`
    Amount   float64 `json:"amount"`
    Interest float64 `json:"interest"`
    Reason   string  `json:"reason"`
}

type ScoringResult struct {
    Client      ClientData      `json:"client"`
    Income      IncomeAnalysis  `json:"income"`
    Risk        RiskAssessment  `json:"risk"`
    Decision    Decision        `json:"decision"`
    Explanation string          `json:"explanation,omitempty"`
}

type AuctionRequest struct {
    TaskID string      `json:"task_id"`
    Data   interface{} `json:"data"`
}

type Bid struct {
    AgentID string  `json:"agent_id"`
    Cost    float64 `json:"cost"`
    Load    float64 `json:"load"`
}