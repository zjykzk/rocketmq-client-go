package client

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/route"
)

type fakeProducer struct {
	group string
}

func (mp *fakeProducer) Group() string {
	return mp.group
}

func (mp *fakeProducer) PublishTopics() []string {
	return nil
}

func (mp *fakeProducer) UpdateTopicPublish(topic string, router *route.TopicRouter) {}

func (mp *fakeProducer) NeedUpdateTopicPublish(topic string) bool {
	return false
}

func TestProducerColl(t *testing.T) {
	group := "g1"
	pc := producerColl{producers: make(map[string]Producer)}
	prev, suc := pc.putIfAbsent(group, &fakeProducer{group})
	assert.Nil(t, prev)
	assert.True(t, suc)

	prev, suc = pc.putIfAbsent(group, &fakeProducer{group})
	assert.NotNil(t, prev)
	assert.False(t, suc)

	assert.Equal(t, []Producer{&fakeProducer{group}}, pc.coll())
	assert.True(t, pc.contains(group))
	assert.Equal(t, 1, pc.size())

	pc.delete(group)
	assert.False(t, pc.contains(group))
	assert.Equal(t, 0, pc.size())
}

type fakeConsumer struct {
	group string

	resetTopicOfOffset string
	resetOffsetErr     error
	runResetOffset     bool
}

func (mc *fakeConsumer) Group() string {
	return mc.group
}
func (mc *fakeConsumer) SubscribeTopics() []string {
	return nil
}
func (mc *fakeConsumer) UpdateTopicSubscribe(topic string, router *route.TopicRouter) {
	return
}
func (mc *fakeConsumer) NeedUpdateTopicSubscribe(topic string) bool {
	return false
}
func (mc *fakeConsumer) ConsumeFromWhere() string {
	return ""
}
func (mc *fakeConsumer) Model() string {
	return ""
}
func (mc *fakeConsumer) Type() string {
	return ""
}
func (mc *fakeConsumer) UnitMode() bool {
	return false
}
func (mc *fakeConsumer) Subscriptions() []*SubscribeData {
	return nil
}
func (mc *fakeConsumer) ReblanceQueue() {}
func (mc *fakeConsumer) RunningInfo() RunningInfo {
	return RunningInfo{}
}
func (mc *fakeConsumer) ResetOffset(topic string, offsets map[message.Queue]int64) error {
	mc.resetTopicOfOffset = topic
	mc.runResetOffset = true
	return mc.resetOffsetErr
}
func (mc *fakeConsumer) ConsumeMessageDirectly(
	msg *message.Ext, broker string,
) (
	r ConsumeMessageDirectlyResult, err error,
) {
	// TODO
	return
}

func TestConsumer(t *testing.T) {
	group := "g1"
	mc := &fakeConsumer{group: group}
	cc := consumerColl{eles: make(map[string]Consumer)}
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

type fakeAdmin struct {
	group string
}

func (a *fakeAdmin) Group() string {
	return a.group
}

func TestAdmin(t *testing.T) {
	group := "g1"
	ma := &fakeAdmin{group: group}
	ac := adminColl{eles: make(map[string]Admin)}
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
