#ifndef BTREE_IMPLEMENT_H
#define BTREE_IMPLEMENT_H

#include "btree_mgr.h"
#include "buffer_mgr.h"

typedef struct BpTNode {
	int totalKeys;
	bool isLeafNode;
	Value ** keyNode;
	void ** ptr;
	struct BpTNode * internalNode, * next; 
} BpTNode;

typedef struct BPTreeManagement {
	int totalNodes, totalEntries, orderType;
	BpTNode * rootNode, * queueNode;
	DataType keyDataType;
	BM_BufferPool buffer;
	BM_PageHandle pHandler;
} BPTreeManagement;

typedef struct NodeSearchManagement {
	BpTNode * node;
	int keyIValue, totalKeys, order;
} NodeSearchManagement;

typedef struct NodeRecordData {
	RID rid;
} NodeRecordData;

BpTNode * findLeaf(BpTNode * root, Value * key);
NodeRecordData * findRecord(BpTNode * root, Value * key);

void enqueue(BPTreeManagement * treeManager, BpTNode * new_node);
BpTNode * dequeue(BPTreeManagement * treeManager);
int pathForRoot(BpTNode * root, BpTNode * child);

NodeRecordData * makeRecord(RID * rid);
BpTNode * insertIntoLeaf(BPTreeManagement * treeManager, BpTNode * leaf, Value * key, NodeRecordData * pointer);
BpTNode * createNewTree(BPTreeManagement * treeManager, Value * key, NodeRecordData * pointer);
BpTNode * createNode(BPTreeManagement * treeManager);
BpTNode * createLeaf(BPTreeManagement * treeManager);
BpTNode * insertIntoLeafAfterSplitting(BPTreeManagement * treeManager, BpTNode * leaf, Value * key, NodeRecordData * pointer);
BpTNode * insertIntoNode(BPTreeManagement * treeManager, BpTNode * parent, int left_index, Value * key, BpTNode * right);
BpTNode * insertIntoNodeAfterSplitting(BPTreeManagement * treeManager, BpTNode * parent, int left_index, Value * key, BpTNode * right);
BpTNode * insertIntoParent(BPTreeManagement * treeManager, BpTNode * left, Value * key, BpTNode * right);
BpTNode * insertIntoNewRoot(BPTreeManagement * treeManager, BpTNode * left, Value * key, BpTNode * right);
int getLeftIndex(BpTNode * parent, BpTNode * left);

BpTNode * adjustRoot(BpTNode * root);
BpTNode * mergeNodes(BPTreeManagement * treeManager, BpTNode * n, BpTNode * neighbor, int neighbor_index, int k_prime);
BpTNode * redistributeNodes(BpTNode * root, BpTNode * n, BpTNode * neighbor, int neighbor_index, int k_prime_index, int k_prime);
BpTNode * deleteEntry(BPTreeManagement * treeManager, BpTNode * n, Value * key, void * pointer);
BpTNode * delete(BPTreeManagement * treeManager, Value * key);
BpTNode * removeEntryFromNode(BPTreeManagement * treeManager, BpTNode * n, Value * key, BpTNode * pointer);
int getNeighborIndex(BpTNode * n);

bool isLess(Value * key1, Value * key2);
bool isGreater(Value * key1, Value * key2);
bool isEqual(Value * key1, Value * key2);

#endif 
