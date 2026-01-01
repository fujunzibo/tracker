package dao

import "time"

type PageResult[T any] struct {
	Total int64 `json:"total"`
	Items []T   `json:"items"`
}

type Point struct {
	Time   int64   `json:"time"`
	Value  float64 `json:"value"`
	Metric string  `json:"metric,omitempty"`
}

type Metric struct {
	Time   time.Time `json:"time"`
	Metric string    `json:"metric"`
	Value  string    `json:"value"`
}
