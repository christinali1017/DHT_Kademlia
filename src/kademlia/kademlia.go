package kademlia

// Contains the core kademlia type. In addition to core state, this type serves
// as a receiver for the RPC methods, which is required by that package.
//Git Test

import (
	"container/list"
	// "encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"sync"
	// "os"
)

// const (
// 	ALPHA = 3
// 	b     = 8 * IDBytes
// 	k     = 20
// )
const (
	TOTAL_BUCKETS   = 8 * IDBytes
	MAX_BUCKET_SIZE = 20
	ALPHA           = 3
	TIME_INTERVAL   = 0.3 * time.Second
)

// Kademlia type. You can put whatever state you need in this.
type Kademlia struct {
	NodeID      ID
	SelfContact Contact
	buckets     [IDBytes * 8]*list.List
	storeMutex  sync.RWMutex
	storeMap    map[ID][]byte
}

type ContactDistance struct {
	SelfContact Contact
	Distance    ID
}

type ByDistance []ContactDistance

type Stopper struct {
	stopMutex sync.RWMutex
	stopType  int
	//0: No stop 
	//1: Have accumulate k active
	//2: No improve in shortlist
	//3: Found value


}

type Valuer struct {
	value  []byte
	NodeID ID
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
	index := strings.Index(laddr, ":")
	port := laddr[index:]

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

	k.UpdateContact(*contact)
	var res string
	if findNodeRes.Nodes != nil {
		for _, contactItem := range findNodeRes.Nodes {
			k.UpdateContact(contactItem)
		}
	}

	res = k.ContactsToString(findNodeRes.Nodes)
	if res == "" {
		return "No Record"
	}
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
	if findValueRes.Nodes != nil {
		for _, contactItem := range findValueRes.Nodes {
			k.UpdateContact(contactItem)
		}
	}

	var res string
	//if value if found return value, else return closest contacts
	if findValueRes.Value != nil {
		res = res + string(findValueRes.Value[:])
	} else {
		res = res + k.ContactsToString(findValueRes.Nodes)
	}
	if res == "" {
		return "No Record"
	}
	return "ok, result is: " + res
}

func (k *Kademlia) LocalFindValue(searchKey ID) string {
	// TODO: Implement
	k.storeMutex.Lock()
	value, ok := k.storeMap[searchKey]
	k.storeMutex.Unlock()
	if ok {
		return "OK:" + string(value[:])
	} else {
		return "ERR: Not implemented"
	}
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
}

func (k *Kademlia) DoIterativeFindNode(id ID) string {
	// For project 2!

	// shortlist := list.New()
	// markedMap := make(map[ID]bool)
	// initalizeContacts := k.closestContacts(id, k.NodeID, ALPHA)
	// // fmt.Println(k.ContactsToString(initalizeContacts))

	// for {
	// 	searchNodes := getUnmarkedNodes(markedMap, shortlist, ALPHA)

	// }

	//sort
	//sort.Sort(ByDistance(contactDistanceList))
	shortlist := IterativeFindNode(id) 
	// shortcontacts := FindClosestContactsBySort(shortlist)
	return k.ContactsToString(shortlist)
}

func (k* Kademlia) IterativeFindNode(id ID) []Contact {


	shortlist = make([]ContactDistance, 0)

	//node that we have seen
	seenMap := make(map[ID]bool)

	//nodes that have not been queried yet
	unqueriedList := list.New()

	//channel for query result
	res := make(chan []Contact, 120)

	//channel for nodes need to be deleted
	deleteNodes := make(chan Contact, 120)

	stopper := new(Stopper)

	//initialize shortlist, seenmap
	contacts := k.FindClosestContacts(id, k.NodeID)[:3]
	for _, contact := range contacts {
		seenMap[contact.NodeID] = true
		unqueriedList.PushBack(contact)
		shortlist = append(shortlist, k.contactToDistanceContact(contact, id)) 
	}

	// alpha query
	for i := 0; i < ALPHA && unqueriedList.Len() > 0 ; i++ {
		//check if end 
		stopper.stopMutex.RLock()
		if stopper.stopType != 0 {
			break;
		}
		stopper.stopMutex.RUnlock()
		
		front := unqueriedList.Front()
		contact := front.Value.(Contact)
		unqueriedList.Remove(front)
		go func() {
			 err := k.rpcQuery(contact, id, res)	
			 if err != nil {
			 	deleteNodes <- contact
			 }
		}()
	}

	//add res to shortlist
	for {
		select {
			case contacts <- res:
				for _, contact := range contacts {
					if _, ok := seenMap[contact.NodeID]; ok == false {
						shortlist = append(shortlist, k.contactToDistanceContact(contact, id))
						unqueriedList.PushBack(contact)
						seenMap[contact.NodeID] = true;
					}
				}
			case contact <- deleteNodes:
				for i := 0; i < len(shortlist); i++ {
					if shortlist[i].NodeID.Equals(Contact.NodeID) {
						shortlist = append(shorlist[:i], shortlist[i+1]...)
						break;
					}
				}
				delete(seenMap, contact.NodeID)
			default:	
		}
	}

	if len(shortlist) < 20 {
		return FindClosestContactsBySort(shortlist)
	}
	return FindClosestContactsBySort(shortlist[:MAX_BUCKET_SIZE+1])
}


func (k *Kademlia) DoIterativeStore(key ID, value []byte) string {
	// For project 2!
	contactList := k.IterativeFindNode(key)
	var result string
	for _, contact := range contactList {
		res := k.DoStore(contact, key, value)
		if res == "ok" {
			result = result + contact.NodeID.AsString()
		}
	}
	return result
}
func (k *Kademlia) DoIterativeFindValue(key ID) string {
	// For project 2!
	shortlist := make([]ContactDistance, 0)

	//node that we have seen
	seenMap := make(map[ID]bool)
	returnedValueMap := make(map[ID]bool)

	//nodes that have not been queried yet
	unqueriedList := list.New()

	//init the stopper
	stopper := new(Stopper)
	stopper.stopType = 0

	contactChan = make(chan []Contact, MAX_BUCKET_SIZE*MAX_BUCKET_SIZE*2)
	valueChan = make(chan Valuer, MAX_BUCKET_SIZE*MAX_BUCKET_SIZE*2)
	deleteChan = make(chan ID, MAX_BUCKET_SIZE*MAX_BUCKET_SIZE*2)

	//initialize
	var returnValue []byte
	contacts := k.FindClosestContacts(id, k.NodeID)[:3]
	for _, contact := range contacts {
		seenMap[contact.NodeID] = true
		unqueriedList.PushBack(contact)
		shortlist = append(shortlist, k.contactToDistanceContact(contact))
	}

	// alpha query
	for i := 0; i < ALPHA && unqueriedList.Len() > 0; i++ {
		//check if end
		stopper.stopMutex.RLock()
		if stopper.stopType != 0 {
			break
		}
		stopper.stopMutex.RUnlock()
		front := unqueriedList.Front()
		unqueriedList.Remove(front)
		contact := front.Value.(Contact)
		go k.iterFindValuQeuery(node, target, contactChan, valuerChan, deleteChan)
	}

	go func() {
		for {
			select {
			case contactList := <-contactChan:
				for _, contact := range contacts {
					if _, ok := seenMap[contact.NodeID]; ok == false {
						shortlist = append(shortlist, k.contactToDistanceContact(contact, id))
						unqueriedList.PushBack(contact)
						seenMap[contact.NodeID] = true
					}
				}
			case valuer := <-valueChan:
				returnValue = valuer.value
				returnedValueMap[valuer.NodeID] = true
				if len(valueChan) == 0{
					stopper.stopMutex.Lock()
					stopper.stopType = 3
					stopper.stopMutex.Unlock()
					return
				}
			case deleteNodeId := <-deleteChan:
				for i := 0; i < len(shortlist); i++ {
					if shortlist[i].NodeID.Equals(deleteNodeId) {
						shortlist = append(shorlist[:i], shortlist[i+1]...)
						break
					}
				}
				delete(seenMap, deleteNodeId)
			default:

			}
		}
	}()

	for {
		stopper.stopMutex.RLock()
		if stopper.stopType != 0 {
			break
		}
		stopper.stopMutex.RUnlock()
		// alpha query
		for i := 0; i < ALPHA && unqueriedList.Len() > 0; i++ {
			//check if end
			stopper.stopMutex.RLock
			if stopper.stopType != 0 {
				break
			}
			stopper.stopMutex.RUnlock()
			front := unqueriedList.Front()
			unqueriedList.Remove(front)
			contact := front.Value.(Contact)
			go k.iterFindValuQeuery(node, target, contactChan, valuerChan, deleteChan)
		}
	}

	return "ERR: Not implemented"
}

//rpc query for iterativefindnode
func (k *Kademlia) rpcQuery(node Contact, searchId ID, res chan []Contact) error {
	client, err := rpc.DialHTTP("tcp", contact.Host.String()+":"+strconv.Itoa(int(contact.Port)))

	if err != nil {
		return err
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
		return err;
	}

	res <- findNodeRes.Nodes

	//update contact
	k.UpdateContact(node)
	for _, contact := range findNodeRes.Nodes {
		k.UpdateContact(contact)
	}

	return err
}
func (k *Kademlia) iterFindValuQeuery(node *Contact, target NodeID, contactChan chan []Contact, valuerChan chan Valuer, deleteChan chan ID) {
	client, errDial := rpc.DialHTTP("tcp", contact.Host.String()+":"+strconv.Itoa(int(contact.Port)))
	if err != nil {
		deleteChan <- node.NodeID
	}

	//create find value request and result
	findValueReq := new(FindValueRequest)
	findValueReq.Sender = k.SelfContact
	findValueReq.MsgID = NewRandomID()
	findValueReq.Key = searchKey

	findValueRes := new(FindValueResult)

	//find value
	errCall = client.Call("KademliaCore.FindValue", findValueReq, findValueRes)

	if err != nil {
		deleteChan <- node.NodeID
	}

	defer client.Close()

	//update contact
	k.UpdateContact(*contact)
	if findValueRes.Nodes != nil {
		for _, contactItem := range findValueRes.Nodes {
			k.UpdateContact(contactItem)
		}
	}
	if findValueRes.Value != nil {
		valuer := new(Valuer)
		valuer.value = findValueRes.Value
		valuer.NodeID = node.NodeID
		valuerChan <- valuer
	} else if findValueRes.Nodes != nil {
		contactChan <- findValueRes.Nodes
	}
}

///////////////////////////////////////////////////////////////////////////////
// methods for bucket
///////////////////////////////////////////////////////////////////////////////

func (k *Kademlia) UpdateContact(contact Contact) {
	//Find bucket
	fmt.Println("Begin update")

	// fmt.Println("*********" + contact.Host.String())
	bucket := k.FindBucket(contact.NodeID)
	if bucket == nil {
		return
	}
	//Find contact, check if conact exist
	res, err := k.FindContactInBucket(contact.NodeID, bucket)

	//if contact has already existed, then move contact to the end of bucket
	if err == nil {
		k.storeMutex.Lock()
		bucket.MoveToBack(res)
		k.storeMutex.Unlock()
		//if contact is not found
	} else {
		//check if bucket is full, if not, add contact to the end of bucket
		k.storeMutex.RLock()
		bucketLen := bucket.Len()
		k.storeMutex.RUnlock()
		if bucketLen < MAX_BUCKET_SIZE {
			k.storeMutex.Lock()
			bucket.PushBack(contact)
			k.storeMutex.Unlock()

			//if bucket id full, ping the least recently contact node.
		} else {
			k.storeMutex.Lock()
			front := bucket.Front()
			k.storeMutex.Unlock()
			lrc_node := front.Value.(Contact)
			pingresult := k.DoPing(lrc_node.Host, lrc_node.Port)

			/*if least recent contact respond, ignore the new contact and move the least recent contact to
			  the end of the bucket
			*/
			if pingresult == "ok" {
				k.storeMutex.Lock()
				bucket.MoveToBack(front)
				k.storeMutex.Unlock()

				// if it does not respond, delete it and add the new contact to the end of the bucket
			} else {
				k.storeMutex.Lock()
				bucket.Remove(front)
				bucket.PushBack(contact)
				k.storeMutex.Unlock()

			}

		}

	}
	fmt.Println("Update contact succeed, nodeid is : " + contact.NodeID.AsString())

}

func (k *Kademlia) FindBucket(nodeid ID) *list.List {
	k.storeMutex.RLock()
	defer k.storeMutex.RUnlock()
	prefixLength := k.NodeID.Xor(nodeid).PrefixLen()
	if prefixLength == 160 {
		return nil
	}
	bucketIndex := (IDBytes * 8) - prefixLength

	//if ping yourself, then the distance would be 160, and it will ran out of index
	if bucketIndex > (IDBytes*8 - 1) {
		bucketIndex = (IDBytes*8 - 1)
	}

	bucket := k.buckets[bucketIndex]
	return bucket
}

func (k *Kademlia) FindContactInBucket(nodeId ID, bucket *list.List) (*list.Element, error) {
	k.storeMutex.RLock()
	defer k.storeMutex.RUnlock()
	for i := bucket.Front(); i != nil; i = i.Next() {
		c := i.Value.(Contact)
		if c.NodeID.Equals(nodeId) {
			return i, nil
		}
	}
	return nil, &NotFoundError{nodeId, "Not found"}
}

func (k *Kademlia) FindClosestContacts(searchKey ID, senderKey ID) []Contact {
	contactDistanceList := k.FindAllKnownContact(searchKey, senderKey)
	result := k.FindClosestContactsBySort(contactDistanceList)
	return result

}

func (k *Kademlia) FindAllKnownContact(searchKey ID, senderKey ID) []ContactDistance {
	k.storeMutex.RLock()
	defer k.storeMutex.RUnlock()
	result := make([]ContactDistance, 0)
	bucketIndex := 0
	for bucketIndex < IDBytes*8 {
		targetBucket := k.buckets[bucketIndex]
		for i := targetBucket.Front(); i != nil; i = i.Next() {
			if !i.Value.(Contact).NodeID.Equals(senderKey) {
				contactD := new(ContactDistance)
				contactD.SelfContact = i.Value.(Contact)
				contactD.Distance = i.Value.(Contact).NodeID.Xor(searchKey)
				result = append(result, *contactD)
			}
		}
		bucketIndex++
	}

	if len(result) == 0 {
		return nil
	}

	return result

}

func (k *Kademlia) FindClosestContactsBySort(contactDistanceList []ContactDistance) []Contact {
	sort.Sort(ByDistance(contactDistanceList))
	result := make([]Contact, 0)
	for _, contactDistanceItem := range contactDistanceList {
		result = append(result, contactDistanceItem.SelfContact)
	}

	if len(result) == 0 {
		return nil
	}

	if len(result) <= MAX_BUCKET_SIZE {
		return result
	}

	result = result[0:MAX_BUCKET_SIZE]

	return result

}

func (a ByDistance) Len() int           { return len(a) }
func (a ByDistance) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByDistance) Less(i, j int) bool { return a[i].Distance.Less(a[j].Distance) }

//Convert contacts to string
func (k *Kademlia) ContactsToString(contacts []Contact) string {
	var res string
	for _, contact := range contacts {
		res = res + "{NodeID: " + contact.NodeID.AsString() + ", Host: " + contact.Host.String() + ", Port: " + strconv.Itoa(int(contact.Port)) + "},"
	}
	return res[:len(res)]
}

func (k *Kademlia) PingWithOutUpdate(host net.IP, port uint16) string {

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
	return "ok"
}

/*================================================================================
Functions for Project 2
================================================================================ */
func (k *Kademlia) closestContacts(searchKey ID, senderKey ID, num int) []Contact {
	contactDistanceList := k.FindAllKnownContact(searchKey, senderKey)
	result := k.FindNClosestContacts(contactDistanceList, num)
	return result
}

func (k *Kademlia) FindNClosestContacts(contactDistanceList []ContactDistance, num int) []Contact {
	sort.Sort(ByDistance(contactDistanceList))
	result := make([]Contact, 0)
	for _, contactDistanceItem := range contactDistanceList {
		result = append(result, contactDistanceItem.SelfContact)
	}

	if len(result) == 0 {
		return nil
	}

	if len(result) <= num {
		return result
	}

	result = result[0:num]

	return result

}
func (k *Kademlia) compareDistance(c1 Contact, c2 Contact, id ID) int {
	distance1 := c1.NodeID.Xor(id)
	distance2 := c2.NodeID.Xor(id)
	return distance1.Compare(distance2)
}

//From shortlist get the unmarked list
func (k *Kademlia) getUnmarkedNodes(markedMap map[ID]bool, shortlist *list.List, num int) []Contact {
	unmarkedNodes := make([]Contact, 0)
	for i := shortlist.Front(); i != nil; i = i.Next() {
		contact := i.Value.(Contact)
		if !markedMap[contact.NodeID] {
			unmarkedNodes.append(contact)
			if len(unmarkedNodes) >= num {
				break
			}
		}
	}
	return unmarkedNodes
}

func (k *Kademlia) contactToDistanceContact(contact Contact, id ID) {
	contactD := new(ContactDistance)
	contactD.SelfContact = contact.Value.(Contact)
	contactD.Distance = contact.Value.(Contact).NodeID.Xor(id)
	return contactD
}

