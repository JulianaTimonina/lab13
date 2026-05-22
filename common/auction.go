package common

import "errors"

// BidScore итоговая метрика для выбора победителя (меньше — лучше).
// Учитывает стоимость, загрузку, навык и доступность.
func BidScore(b Bid) float64 {
	return b.Cost + b.Load*0.4 - b.Skill*0.25 - b.Availability*0.35
}

// SelectBestBid выбирает ставку с наилучшим (минимальным) score.
func SelectBestBid(bids []Bid) (Bid, error) {
	if len(bids) == 0 {
		return Bid{}, errors.New("no bids received")
	}
	best := bids[0]
	bestScore := BidScore(best)
	for _, b := range bids[1:] {
		if s := BidScore(b); s < bestScore {
			best = b
			bestScore = s
		}
	}
	return best, nil
}
