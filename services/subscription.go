package services

type SubscriptionId = int64

type Subscription struct {
	Id            SubscriptionId
	Started       bool
	StartOffset   int64
	EndOffset     int64
	OffsetIsIndex bool
}
