package distributed

import (
	"reflect"
	"strings"

	eh "github.com/looplab/eventhorizon"
)

type RoutingStrategy interface {
	GetRoutingKey(eh.Command) RoutingKey
	GetTopicPattern() TopicPattern
}
type RoutingKey string

type TopicPattern string
type StaticRoutingStrategy struct {
	domain string
}

// return package.structname
func (rs *StaticRoutingStrategy) GetRoutingKey(command eh.Command) RoutingKey {
	return RoutingKey(strings.Split(reflect.TypeOf(command).String(), "*")[1])
}

func (rs *StaticRoutingStrategy) GetTopicPattern() TopicPattern {
	if rs.domain == "" {
		panic("the domain of RoutingStrategy must not be null")
	}
	return TopicPattern(rs.domain + ".#")
}
