#ifndef BTREE_HELPER_H
#define BTREE_HELPER_H

#include"btree_mgr.h"
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"

typedef struct BpTNode {
    int totalKeys;
    bool isLeafNode;
    void ** ptr;
    Value ** keyNode;
    struct BpTNode * next;
    struct BpTNode * parent;
} BpTNode;

typedef struct IndexSearchManager {
    int keyIvalue, totalKeys, whichOrder;
    BpTNode * bpTNode;
}IndexSearchManager;

typedef struct BTreeManagement {
    int totalNodes, totalEntries, whichOrder;
    BpTNode * rootNode, * queueNode;
    DataType dataType;
    BM_BufferPool buffer;
    BM_PageHandle pHandler;
} BTreeManagement;

typedef struct NodeRecordDetails {
    RID rid;
} NodeRecordDetails;

#endif