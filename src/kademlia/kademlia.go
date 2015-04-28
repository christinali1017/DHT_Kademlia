package kademlia

// Contains the core kademlia type. In addition to core state, this type serves
// as a receiver for the RPC methods, which is required by that package.
//Git Test

import (
	"container/list"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	// "strings"
	// "os"
)

// const (
// 	alpha = 3
// 	b     = 8 * IDBytes
// 	k     = 20
// )
const (
	numberofbuckets = 8 * IDBytes
	maxbucketsize   = 20
	alpha           = 3
)

// Kademlia type. You can put whatever state you need in this.
type Kademlia struct {
	NodeID      ID
	SelfContact Contact
	buckets     [IDBytes * 8]*list.List
	storeMutex  sync.Mutex
	storeMap    map[ID][]byte
}

func NewKademlia(laddr string) *Kademlia {
	// TODO: Initialize other state here as you add functionality.
	k := new(Kademlia)
	k.NodeID = NewRandomID()
	for i := 0; i < len(k.buckets); i++ {
		k.buckets[i] = list.New()
	}

	// make message map
	k.storeMap = make(map[ID][]byte)

	// Set up RPC server
	// NOTE: KademliaCore is just a wrapper around Kademlia. This type includes
	// the RPC functions.
	rpc.Register(&KademliaCore{k})
	rpc.HandleHTTP()

	//GET THE PORT
	// index := strings.Index(laddr, ":")
	// port := laddr[index:]

	l, err := net.Listen("tcp", laddr)
	if err != nil {
		log.Fatal("Listen: ", err)
	}
	// Run RPC server forever.
	go http.Serve(l, nil)
	fmt.Println("laddr is : " + laddr)

	// GET OS HOST
	// name, err := os.Hostname()
	// addrs, err := net.LookupHost(name)

	// fmt.Println("address is : " + addrs[0])

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
	fmt.Println("new : " + host.String())
	// k.SelfContact = Contact{k.NodeID, net.ParseIP(addrs[0]), uint16(port_int)}
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
	// Find contact with provided ID
	if nodeId == k.SelfContact.NodeID {
		return &k.SelfContact, nil
	}
	bucket := k.FindBucket(nodeId)
	if bucket == nil {
		return nil, &NotFoundError{nodeId, "Not found"}
	}
	res, err := k.FindContactInBucket(nodeId, bucket)
	if err == nil {
		c := res.Value.(Contact)
		return &c, nil
	}
	return nil, &NotFoundError{nodeId, "Not found"}
}

// This is the function to perform the RPC
func (k *Kademlia) DoPing(host net.IP, port uint16) string {

	var ping PingMessage
	ping.MsgID = NewRandomID()
	ping.Sender = k.SelfContact

	fmt.Println("***IN DO ping: " + k.SelfContact.Host.String())

	var pong PongMessage
	// client, err := rpc.DialHTTP("tcp", string(host) + ":" + string(port))
	client, err := rpc.DialHTTP("tcp", host.String()+":"+strconv.Itoa(int(port)))
	if err != nil {
		log.Fatal("DialHTTP: ", err)
	}
	err = client.Call("KademliaCore.Ping", ping, &pong)

	if err != nil {
		return "ERR: " + err.Error()
	}

	defer client.Close()
    k.UpdateContact(pong.Sender)

	return "ok"
}

func (k *Kademlia) DoStore(contact *Contact, key ID, value []byte) string {
	//create store request and result
	storeRequest := new(StoreRequest)
	storeRequest.MsgID = NewRandomID()
	storeRequest.Sender = k.SelfContact
	storeRequest.Key = key
	storeRequest.Value = value

	storeResult := new(StoreResult)

	// store
	// rpc.DialHTTP("tcp", host.String() + ":" + strconv.Itoa(int(port)))
	client, err := rpc.DialHTTP("tcp", contact.Host.String()+":"+strconv.Itoa(int(contact.Port)))

	if err != nil {
		return err.Error()
	}

	err = client.Call("KademliaCore.Store", storeRequest, storeResult)

	//check error
	if err != nil {
		return err.Error()
	}
	k.UpdateContact(*contact)
	defer client.Close()
	return "ok"
}

func (k *Kademlia) DoFindNode(contact *Contact, searchKey ID) string {
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"

	client, err := rpc.DialHTTP("tcp", contact.Host.String()+":"+strconv.Itoa(int(contact.Port)))

	if err != nil {

		return err.Error()
	}

	//create find node request and result
	findNodeRequest := new(FindNodeRequest)
	findNodeRequest.Sender = k.SelfContact
	findNodeRequest.MsgID = NewRandomID()
	findNodeRequest.NodeID = searchKey

	findNodeRes := new(FindNodeResult)

	//find node
	err = client.Call("KademliaCore.FindNode", findNodeRequest, findNodeRes)
	if err != nil {
		return err.Error()
	}

	//update contact
	var res string
	// for _, contact := range findNodeRes.Nodes {
	// 	k.UpdateContact(contact)
	// 	res = res + contact.NodeID.AsString() + " "
	// }

	res = k.ContactsToString(findNodeRes.Nodes)
	return "ok, result is: " + res
}

func (k *Kademlia) DoFindValue(contact *Contact, searchKey ID) string {
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	client, err := rpc.DialHTTP("tcp", contact.Host.String()+":"+strconv.Itoa(int(contact.Port)))
	if err != nil {
		return err.Error()
	}

	//create find value request and result
	findValueReq := new(FindValueRequest)
	findValueReq.Sender = k.SelfContact
	findValueReq.MsgID = NewRandomID()
	findValueReq.Key = searchKey

	findValueRes := new(FindValueResult)

	//find value
	err = client.Call("KademliaCore.FindValue", findValueReq, findValueRes)

	if err != nil {
		return err.Error()
	}

	defer client.Close()

	//update contact
	k.UpdateContact(*contact)

	var res string
	//if value if found return value, else return closest contacts
	if findValueRes.Value != nil {
		res = res + hex.EncodeToString(findValueRes.Value[:])
	} else {
		res = res + k.ContactsToString(findValueRes.Nodes)
	}

	return "ok, result is: " + res
}

func (k *Kademlia) LocalFindValue(searchKey ID) string {
	// TODO: Implement
	value, ok := k.storeMap[searchKey]
	if ok {
		return "OK:" + hex.EncodeToString(value[:])
	} else {
		return "ERR: Not implemented"
	}
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
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

///////////////////////////////////////////////////////////////////////////////
// methods for bucket
///////////////////////////////////////////////////////////////////////////////

func (k *Kademlia) UpdateContact(contact Contact) {
	//Find bucket
	fmt.Println("Begin update")

	fmt.Println("*********" + contact.Host.String())
	bucket := k.FindBucket(contact.NodeID)
	if bucket == nil {
		return 
	}
	//Find contact, check if conact exist
	res, err := k.FindContactInBucket(contact.NodeID, bucket)

	//if contact has already existed, then move contact to the end of bucket
	if err == nil {
		bucket.MoveToBack(res)
		//if contact is not found
	} else {
		//check if bucket is full, if not, add contact to the end of bucket
		if bucket.Len() < maxbucketsize {
			bucket.PushBack(contact)

			//if bucket id full, ping the least recently contact node.
		} else {
			front := bucket.Front()
			lrc_node := front.Value.(Contact)
			pingresult := k.DoPing(lrc_node.Host, lrc_node.Port)

			/*if least recent contact respond, ignore the new contact and move the least recent contact to
			  the end of the bucket
			*/
			if pingresult == "ok" {
				bucket.MoveToBack(front)

				// if it does not respond, delete it and add the new contact to the end of the bucket
			} else {
				bucket.Remove(front)
				bucket.PushBack(contact)
			}

		}

	}
	fmt.Println("Update contact succeed, nodeid is : " + contact.NodeID.AsString())

}

func (k *Kademlia) FindBucket(nodeid ID) *list.List {
	prefixLength := k.NodeID.Xor(nodeid).PrefixLen()
	bucketIndex := (IDBytes * 8) - prefixLength 
	fmt.Println("find PrefixLen: ")
	fmt.Println(prefixLength)
	fmt.Println("find bucket index: ")
	fmt.Println(bucketIndex)

	//if ping yourself, then the distance would be 160, and it will ran out of index
	if bucketIndex > (IDBytes * 8 - 1){
		bucketIndex = (IDBytes * 8 - 1)
	}
	
	bucket := k.buckets[bucketIndex]
	return bucket
}

func (k *Kademlia) FindContactInBucket(nodeId ID, bucket *list.List) (*list.Element, error) {
	for i := bucket.Front(); i != nil; i = i.Next() {
		c := i.Value.(Contact)
		if c.NodeID.Equals(nodeId) {
			return i, nil
		}
	}
	return nil, &NotFoundError{nodeId, "Not found"}
}

func (k *Kademlia) FindClosestContacts(searchKey ID) []Contact {

	result := make([]Contact, 0)
	targetBucket := k.FindBucket(searchKey)
	if targetBucket == nil {
		fmt.Println("bucket is nil")
		return nil
	}
	for i := targetBucket.Front(); i != nil; i = i.Next() {
		result = append(result, i.Value.(Contact))
	}
	//bucket := k.FindBucket(searchKey)

	return result
}

//Convert contacts to string
func (k *Kademlia) ContactsToString(contacts []Contact) string {
	var res string
	for _, contact := range contacts {
		res = res + "{NodeID: " + contact.NodeID.AsString() + ", Host: " + contact.Host.String() + ", Port: " + strconv.Itoa(int(contact.Port)) + "},"
	}
	return res[:len(res)]
}

///////////////////////////////////////////////////////////////////////////////
// questions
///////////////////////////////////////////////////////////////////////////////

// 1. When store, if the key has already exist, should we replace the original value?
