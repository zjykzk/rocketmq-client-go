package client

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/route"
)

type mockProducer struct {
	group string
}

func (mp *mockProducer) Group() string {
	return mp.group
}

func (mp *mockProducer) PublishTopics() []string {
	return nil
}

func (mp *mockProducer) UpdateTopicPublish(topic string, router *route.TopicRouter) {}

func (mp *mockProducer) NeedUpdateTopicPublish(topic string) bool {
	return false
}

func TestProducerColl(t *testing.T) {
	group := "g1"
	pc := producerColl{eles: make(map[string]producer)}
	prev, suc := pc.putIfAbsent(group, &mockProducer{group})
	assert.Nil(t, prev)
	assert.True(t, suc)

	prev, suc = pc.putIfAbsent(group, &mockProducer{group})
	assert.NotNil(t, prev)
	assert.False(t, suc)

	assert.Equal(t, []producer{&mockProducer{group}}, pc.coll())
	assert.True(t, pc.contains(group))
	assert.Equal(t, 1, pc.size())

	pc.delete(group)
	assert.False(t, pc.contains(group))
	assert.Equal(t, 0, pc.size())
}

type mockConsumer struct {
	group string
}

func (mc *mockConsumer) Group() string {
	return mc.group
}
func (mc *mockConsumer) SubscribeTopics() []string {
	return nil
}
func (mc *mockConsumer) UpdateTopicSubscribe(topic string, router *route.TopicRouter) {
	return
}
func (mc *mockConsumer) NeedUpdateTopicSubscribe(topic string) bool {
	return false
}
func (mc *mockConsumer) ConsumeFromWhere() string {
	return ""
}
func (mc *mockConsumer) Model() string {
	return ""
}
func (mc *mockConsumer) Type() string {
	return ""
}
func (mc *mockConsumer) UnitMode() bool {
	return false
}
func (mc *mockConsumer) Subscriptions() []*Data {
	return nil
}
func (mc *mockConsumer) ReblanceQueue() {}
func (mc *mockConsumer) RunningInfo() RunningInfo {
	return RunningInfo{}
}

func TestConsumer(t *testing.T) {
	group := "g1"
	mc := &mockConsumer{group: group}
	cc := consumerColl{eles: make(map[string]consumer)}
	prev, suc := cc.putIfAbsent(group, mc)
	assert.Nil(t, prev)
	assert.True(t, suc)

	prev, suc = cc.putIfAbsent(group, mc)
	assert.NotNil(t, prev)
	assert.False(t, suc)

	assert.True(t, cc.contains(group))
	assert.Equal(t, mc, cc.get(group))
	assert.Equal(t, 1, cc.size())

	cc.delete(group)
	assert.False(t, cc.contains(group))
	assert.Equal(t, 0, cc.size())
}

type mockAdmin struct {
	group string
}

func (a *mockAdmin) Group() string {
	return a.group
}

func TestAdmin(t *testing.T) {
	group := "g1"
	ma := &mockAdmin{group: group}
	ac := adminColl{eles: make(map[string]admin)}
	prev, suc := ac.putIfAbsent(group, ma)
	assert.Nil(t, prev)
	assert.True(t, suc)

	prev, suc = ac.putIfAbsent(group, ma)
	assert.NotNil(t, prev)
	assert.False(t, suc)

	assert.Equal(t, 1, ac.size())

	ac.delete(group)
	assert.Equal(t, 0, ac.size())
}
