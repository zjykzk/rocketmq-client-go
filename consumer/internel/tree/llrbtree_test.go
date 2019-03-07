package tree

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakeTree string

func (m fakeTree) CompareTo(m1 Key) int {
	return strings.Compare(string(m), string(m1.(fakeTree)))
}

func TestTree(t *testing.T) {
	tree := &LLRBTree{}

	defer clearGraph()

	// remove unpresent key
	tree.Remove(fakeTree("1"))

	// Put & Get
	tree.Put(fakeTree("1"), 1)
	tree.Put(fakeTree("2"), 2)
	tree.Put(fakeTree("6"), 6)
	tree.Put(fakeTree("7"), 7)
	tree.Put(fakeTree("8"), 8)
	tree.Put(fakeTree("9"), 9)
	tree.Put(fakeTree("3"), 3)
	tree.Put(fakeTree("4"), 4)
	assert.Equal(t, 8, tree.Size())

	graph := ""
	graph += tree.nodeAndEdges()

	v, ok := tree.Get(fakeTree("1"))
	assert.Equal(t, 1, v.(int))
	assert.True(t, ok)

	// get unpresent key
	_, ok = tree.Get(fakeTree("_"))
	assert.False(t, ok)

	// update value
	old := tree.Put(fakeTree("2"), 4)
	assert.Equal(t, 2, old.(int))
	v, ok = tree.Get(fakeTree("2"))
	assert.Equal(t, 4, v.(int))
	assert.True(t, ok)

	// remove smallest
	v = tree.Remove(fakeTree("1"))
	assert.Equal(t, 1, v.(int))
	v, ok = tree.Get(fakeTree("1"))
	assert.False(t, ok)
	graph += tree.nodeAndEdges()
	assert.Equal(t, 7, tree.Size())

	tree.Put(fakeTree("1"), 1)

	// remove greatest
	v = tree.Remove(fakeTree("8"))
	assert.Equal(t, 8, v.(int))
	v, ok = tree.Get(fakeTree("8"))
	assert.False(t, ok)

	tree.Put(fakeTree("5"), 5)
	tree.Put(fakeTree("50"), 50)
	tree.Put(fakeTree("40"), 40)
	tree.Put(fakeTree("90"), 90)

	// remove key
	v = tree.Remove(fakeTree("2"))
	assert.Equal(t, 4, v.(int))
	v, ok = tree.Get(fakeTree("2"))
	assert.False(t, ok)

	graph += tree.nodeAndEdges()
	// first
	k, v := tree.First()
	assert.Equal(t, fakeTree("1"), k.(fakeTree))
	assert.Equal(t, 1, v.(int))
	// last
	k, v = tree.Last()
	assert.Equal(t, fakeTree("90"), k.(fakeTree))
	assert.Equal(t, 90, v.(int))

	t.Log(graph)
	printGraph(graph)
}

func TestPutAll(t *testing.T) {
	tree := &LLRBTree{}

	defer clearGraph()

	// Put & Get
	tree.Put(fakeTree("1"), 1)
	tree.Put(fakeTree("2"), 2)
	tree.Put(fakeTree("6"), 6)
	tree.Put(fakeTree("7"), 7)
	tree.Put(fakeTree("8"), 8)
	tree.Put(fakeTree("9"), 9)
	tree.Put(fakeTree("3"), 3)
	tree.Put(fakeTree("4"), 4)

	newTree := &LLRBTree{}
	newTree.PutAll(tree)

	checkerTrue := func(i int) {
		v, ok := newTree.Get(fakeTree(strconv.Itoa(i)))
		assert.True(t, ok)
		assert.Equal(t, i, v.(int))
	}
	checkerTrue(1)
	checkerTrue(2)
	checkerTrue(3)
	checkerTrue(4)
	checkerTrue(6)
	checkerTrue(7)
	checkerTrue(8)
	checkerTrue(9)

	checkerFalse := func(i int) {
		_, ok := newTree.Get(fakeTree(strconv.Itoa(i)))
		assert.False(t, ok)
	}
	checkerFalse(5)

	g := newTree.nodeAndEdges() + tree.nodeAndEdges()

	tree.Clear()
	tree.Put(fakeTree("20"), 20)
	newTree.PutAll(tree)
	checkerTrue(20)
	g += newTree.nodeAndEdges()

	tree.Clear()
	newTree.PutAll(tree)

	printGraph(g)
}
