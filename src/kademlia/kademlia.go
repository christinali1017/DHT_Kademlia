package kademlia

// Contains the core kademlia type. In addition to core state, this type serves
// as a receiver for the RPC methods, which is required by that package.
//Git Test

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
    "strconv"
    "container/list"
)

const (
	alpha = 3
	b     = 8 * IDBytes
	k     = 20
)


const (
	numberofbuckets  = 8 * IDBytes
	maxbucketsize    = 20
)


// Kademlia type. You can put whatever state you need in this.
type Kademlia struct {
	NodeID ID
    SelfContact Contact
    buckets [IDBytes * 8]*list.List
}


func NewKademlia(laddr string) *Kademlia {
	// TODO: Initialize other state here as you add functionality.
	k := new(Kademlia)
	k.NodeID = NewRandomID()
	for i := 0; i < len(k.buckets); i++{
		k.buckets[i] = list.New();
	}

	// Set up RPC server
	// NOTE: KademliaCore is just a wrapper around Kademlia. This type includes
	// the RPC functions.
	rpc.Register(&KademliaCore{k})
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", laddr)
	if err != nil {
		log.Fatal("Listen: ", err)
	}
	// Run RPC server forever.
	go http.Serve(l, nil)

    // Add self contact
    hostname, port, _ := net.SplitHostPort(l.Addr().String())
    port_int, _ := strconv.Atoi(port)
    ipAddrStrings, err := net.LookupHost(hostname)
    var host net.IP
    for i := 0; i < len(ipAddrStrings); i++ {
        host = net.ParseIP(ipAddrStrings[i])
        if host.To4() != nil {
            break
        }
    }
    k.SelfContact = Contact{k.NodeID, host, uint16(port_int)}
	return k
}

type NotFoundError struct {
	id  ID
	msg string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%x %s", e.id, e.msg)
}

func (k *Kademlia) FindContact(nodeId ID) (*Contact, error) {
	// TODO: Search through contacts, find specified ID
	// Find contact with provided ID
    if nodeId == k.SelfContact.NodeID {
        return &k.SelfContact, nil
    }
    bucket:= k.FindBucketIndex(nodeId)
    res, err := k.FindContactInBucket(nodeId,bucket)
    if err == nil{
    	c := res.Value.(Contact)
    	return &c,nil 
    }
	return nil, &NotFoundError{nodeId, "Not found"}
}

// This is the function to perform the RPC
func (k *Kademlia) DoPing(host net.IP, port uint16) string {
	// TODO: Implement
	ping := new(PingMessage)
	ping.MsgID = NewRandomID()
	ping.Sender = k.SelfContact

	var pong PongMessage
	client, err := rpc.DialHTTP("tcp", string(host) + ":" + string(port))
	err = client.Call("KademliaCore.Ping", ping, pong)

	if err != nil{
		return "PingError"
	}

	k.UpdateContact(pong.Sender)
	client.Close()

	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"


	return "ERR: Not implemented"
}

func (k *Kademlia) DoStore(contact *Contact, key ID, value []byte) string {
	// TODO: Implement
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	return "ERR: Not implemented"
}

func (k *Kademlia) DoFindNode(contact *Contact, searchKey ID) string {
	// TODO: Implement
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	return "ERR: Not implemented"
}

func (k *Kademlia) DoFindValue(contact *Contact, searchKey ID) string {
	// TODO: Implement
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	return "ERR: Not implemented"
}

func (k *Kademlia) LocalFindValue(searchKey ID) string {
	// TODO: Implement
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	return "ERR: Not implemented"
}

func (k *Kademlia) DoIterativeFindNode(id ID) string {
	// For project 2!
	return "ERR: Not implemented"
}
func (k *Kademlia) DoIterativeStore(key ID, value []byte) string {
	// For project 2!
	return "ERR: Not implemented"
}
func (k *Kademlia) DoIterativeFindValue(key ID) string {
	// For project 2!
	return "ERR: Not implemented"
}


/*************************Methods for bucket***************************/

func (k *Kademlia) UpdateContact(contact Contact) string{
	//Find bucket
	bucket := k.FindBucketIndex(contact.NodeID)

    //Find contact, check if conact exist
	res, err := k.FindContactInBucket(contact.NodeID, bucket)

	//if contact has already existed, then move contact to the end of bucket
	if err == nil{
		bucket.MoveToBack(res)
	//if contact is not found
	}else{
		//check if bucket is full, if not, add contact to the end of bucket
		if bucket.Len() < maxbucketsize {
			bucket.PushBack(contact)

		//if bucket id full, ping the least recently contact node.
		}else{
			front := bucket.Front()
			lrc_node := front.Value.(Contact) 
			pingresult := k.DoPing(lrc_node.Host, lrc_node.Port)

			/*if least recent contact respond, ignore the new contact and move the least recent contact to 
			  the end of the bucket
			*/
			if pingresult != "PingError"{
				bucket.MoveToBack(front)

			// if it does not respond, delete it and add the new contact to the end of the bucket
			}else{
				bucket.Remove(front)
				bucket.PushBack(contact)
			}

		}
	


	}
	//todo error handle
	return "ok"
}


func (k *Kademlia) FindBucketIndex(nodeid ID) (*list.List){
	prefixLength := k.NodeID.Xor(nodeid).PrefixLen();
	bucket := k.buckets[prefixLength];
	return bucket
}


func (k *Kademlia) FindContactInBucket(nodeId ID, bucket *list.List) (*list.Element, error){
	for i := bucket.Front(); i != nil; i = i.Next(){
		c := i.Value.(Contact)
		if c.NodeID.Equals(nodeId){
			return i,nil;
		}
	}
	return nil, &NotFoundError{nodeId, "Not found"}
}




/*******************************************history**********************************/

/*
func (k *Kademlia) FindContact(nodeId ID) (*Contact, error) {
	// TODO: Search through contacts, find specified ID
	// Find contact with provided ID
    if nodeId == k.SelfContact.NodeID {
        return &k.SelfContact, nil
    }
    prefixLength := k.FindBucketIndex(nodeId)
    bucket := k.buckets[prefixLength];

	for i := bucket.Front(); i != nil; i = i.Next(){
		c := i.Value.(Contact)
		if c.NodeID.Equals(nodeId){
			return &c,nil;
		}
	}
	return nil, &NotFoundError{nodeId, "Not found"}
}

*/

