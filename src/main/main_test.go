package main_test

import (
	"testing"
	"kademlia"
)

func TestStart(t *testing.T) {
	kademlia.NewKademlia("localhost:7890")
	t.Fail()
}
