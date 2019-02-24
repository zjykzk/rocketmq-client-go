package tree

import (
	"fmt"
	"os"
	"strings"
)

var nameSufix = 1

func dotName(n *node) string {
	return fmt.Sprintf("no_%v%d", n.key, nameSufix)
}

// dot related functions
func dotNode(n *node) string {
	return fmt.Sprintf(`%v [shape=circle,label="%v:%v"];`, dotName(n), n.key, n.value)
}

func dotNodes(n *node) string {
	if n == nil {
		return ""
	}
	return dotNode(n) + dotNodes(n.left) + dotNodes(n.right)
}

func color(n *node) string {
	if isRed(n) {
		return "red"
	}
	return "black"
}

func dotEdges(n *node) string {
	if n == nil {
		return ""
	}

	s := ""
	if n.left != nil {
		s += fmt.Sprintf("%v->%v[color=%s];", dotName(n), dotName(n.left), color(n.left))
	}
	if n.right != nil {
		s += fmt.Sprintf("%v->%v[color=%s];", dotName(n), dotName(n.right), color(n.right))
	}

	s += dotEdges(n.left)
	s += dotEdges(n.right)
	return s
}

func (t *LLRBTree) nodeAndEdges() string {
	b := &strings.Builder{}

	b.WriteString(dotNodes(t.root))
	b.WriteString(dotEdges(t.root))

	nameSufix++

	return b.String()
}

func printGraph(s string) {
	f, err := os.Create("dot.dot")
	if err != nil {
		fmt.Printf("print graph error:%s\n", err)
		return
	}
	f.WriteString(fmt.Sprintf("digraph G{%s}\n", s))
	f.Close()
}
