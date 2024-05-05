#include <stdio.h>
#include <stdlib.h>
#include "btree_mgr.h"
#include "tables.h"
#include "dberror.h"
#include "buffer_mgr_stat.h"
#include "btree_helper.h"
#include "dt.h"
#include "string.h"

#include "storage_mgr.h" // Assignment 1 file
#include "buffer_mgr.h" // Assignment 2 file
#include "record_mgr.h" // Assignment 3 file

// Global Variables Declaration and Initialization 
BTreeManagement * treeTracker = NULL;

/* ------------ Function Declarations ------------ */
// init and shutdown index manager
RC initIndexManager (void *mgmtData);
RC shutdownIndexManager ();

/* ------ create, destroy, open, and close an btree index ------ */
RC createBtree (char *idxId, DataType keyType, int n);
RC openBtree (BTreeHandle **tree, char *idxId);
RC closeBtree (BTreeHandle *tree);
RC deleteBtree (char *idxId);

/* ------ access information about a b-tree ------ */
RC getNumNodes (BTreeHandle *tree, int *result);
RC getNumEntries (BTreeHandle *tree, int *result);
RC getKeyType (BTreeHandle *tree, DataType *result);

RC findKey (BTreeHandle *tree, Value *key, RID *result);
RC insertKey (BTreeHandle *tree, Value *key, RID rid);
RC deleteKey (BTreeHandle *tree, Value *key);
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
RC nextEntry (BT_ScanHandle *handle, RID *result);
RC closeTreeScan (BT_ScanHandle *handle);

/* CUSTOM FUNCTION DECLARATION */

NodeRecordDetails * makeRecord(RID * recordID);
BpTNode * createNew(BTreeManagement * bpTreeManager, NodeRecordDetails * details, Value * keys);
BpTNode * createALeafNode(BTreeManagement * bpTree);
BpTNode * createANewLeaf(BTreeManagement * bpTree);

BpTNode * insertIntoLeafAfterSplitting(BTreeManagement * treeManager, BpTNode * leaf, Value * key, NodeRecordDetails * pointer);


bool isEqual(Value * K1, Value * K2);
bool isGreater(Value * K1, Value * K2);
bool isLess(Value * K1, Value * K2);

/* ============ FUNCTION DEFINITIONS ============ */

RC initIndexManager (void *mgmtData) {
    initStorageManager();
    printf("Index manager initialized.");
    return RC_OK;
}

RC shutdownIndexManager () {
    treeTracker = NULL;
    printf("Operation shut down index manager successful.");
    return RC_OK;
}

RC createBtree (char *idxId, DataType keyType, int n) {
    int size = (PAGE_SIZE / sizeof(BpTNode));

    if (n > size) {
        printf("The n has reached maximum nodes with n = %d & maximum nodes = %d, now you cannot add anything.", n, size);
        return RC_BTREE_MEMORY_MAXIMUM;
    }

    SM_FileHandle localfHandler;
    char info[PAGE_SIZE];

    treeTracker = (BTreeManagement *) malloc (sizeof(BTreeManagement));

    treeTracker->totalNodes = 0; 
    treeTracker->totalEntries = 0;
    treeTracker->whichOrder = n + 2;
    treeTracker->dataType = keyType;

    treeTracker->rootNode = NULL;
    treeTracker->queueNode = NULL;

    treeTracker->buffer = * (BM_BufferPool *) malloc (sizeof(BM_BufferPool *));

    // RC createPageFile(char *fileName)
    RC answer = createPageFile(idxId);
    if(answer != RC_OK) {
        return answer;
    }

    // RC openPageFile(char *fileName, SM_FileHandle *fHandle)
    answer = openPageFile(idxId, &localfHandler);
    if (answer != RC_OK) {
        return answer;
    }

    // RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
    answer = writeBlock(0, &localfHandler, info);
    if (answer != RC_OK) {
        return answer;
    }

    // RC closePageFile(SM_FileHandle *fHandle)
    answer = closePageFile(&localfHandler);
    if (answer != RC_OK) {
        return answer;
    }

    return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId) {
    RC answer = RC_OK;

    *tree = (BTreeHandle *) malloc (sizeof(BTreeHandle));
    (*tree)->mgmtData = treeTracker;

    // RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
    answer = initBufferPool(&treeTracker->buffer, idxId, 1000, RS_FIFO, NULL);

    if (answer == RC_OK) {
        printf("\nopen Tree function SUCCESSFUL\n");
        return RC_OK;
    }
    return answer;
}

RC closeBtree (BTreeHandle *tree) {
    BTreeManagement * btreeInfo = (BTreeManagement *) tree->mgmtData;

    // RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
    markDirty(&btreeInfo->buffer, &btreeInfo->pHandler);

    // RC shutdownBufferPool(BM_BufferPool *const bm)
    shutdownBufferPool(&btreeInfo->buffer);

    btreeInfo = NULL;
    tree = NULL;

    free(btreeInfo);
    free(tree);

    return RC_OK;
}

RC deleteBtree (char *idxId) {

    // RC destroyPageFile(char *fileName)
    RC answer = destroyPageFile(idxId);
    
    if(answer != RC_OK) {
        printf("\n Some issue while deleting BTree \n");
        return RC_B_PLUS_TREE_NOT_DELETING;
    }

    printf("B + Tree deleted SUCCESSFULLY");
    
    return RC_OK;
}

/* ============================================================================================================ */

RC getNumNodes (BTreeHandle *tree, int *result) {
    BTreeManagement * bpTreeStruct = (BTreeManagement *) tree->mgmtData;
    * result = bpTreeStruct->totalNodes;
    return RC_OK;
}

RC getNumEntries (BTreeHandle *tree, int *result) {
    BTreeManagement * bpTreeStruct = (BTreeManagement *) tree->mgmtData;
    * result = bpTreeStruct->totalEntries;
    return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result) {
    BTreeManagement * bpTreeStruct = (BTreeManagement *) tree->mgmtData;
    * result = bpTreeStruct->dataType;
    return RC_OK;
}

/* ============================================================================================================ */

RC findKey (BTreeHandle *tree, Value *key, RID *result) {
    BTreeManagement * bpTreeStruct = (BTreeManagement *) tree->mgmtData;

    NodeRecordDetails * details;

    return RC_OK;
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid) {

    return RC_OK;
}

RC deleteKey (BTreeHandle *tree, Value *key) {

    return RC_OK;
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle) {

    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result) {

    return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle) {

    return RC_OK;
}

/* ============================================================================================================ */


/* 1) CUSTOM METHODS FOR B PLUS TREES
   2) REAL B PLUS TREE NODE AND TREE CREATION, SPLITTING, DISTRIBUTION AND DELETION 
*/

NodeRecordDetails * makeRecord(RID * recordID) {
    NodeRecordDetails * details = (NodeRecordDetails *) malloc (sizeof(NodeRecordDetails));
    if (details == NULL) {
        printf("\nNodeRecordDetails initialized and created successfully.\n");
        exit(RC_MAKE_INSERT_ERROR);
    } else {
        details->rid.slot = recordID->slot;
        details->rid.page = recordID->page;
    }

    return details;
}

BpTNode * createNew(BTreeManagement * bpTreeManager, NodeRecordDetails * details, Value * keys) {
    
    BpTNode * internalNode = createALeafNode(bpTreeManager);

    int order = bpTreeManager->whichOrder;

    internalNode->ptr[0] = details;
    internalNode->ptr[order - 1] = NULL;
    internalNode->parent = NULL;
    internalNode->keyNode[0] = keys;
    internalNode->totalKeys++;

    bpTreeManager->totalEntries++;

    return internalNode;
}


BpTNode * insertIntoLeaf(BTreeManagement * bpTreeManager, BpTNode * leaf, Value * key, NodeRecordDetails * pointer) {
    int pointOfInsertion = 0;
    bpTreeManager->totalEntries++;

    while ((pointOfInsertion < leaf->totalKeys) && (isLess(leaf->keyNode[pointOfInsertion], key))) 
        pointOfInsertion++;

    for(int index = leaf->totalKeys; index > pointOfInsertion; index--) {
        leaf->keyNode[index] = leaf->keyNode[index - 1];
        leaf->ptr[index] = leaf->ptr[index - 1];
    }

    leaf->keyNode[pointOfInsertion] = key;
    leaf->ptr[pointOfInsertion] = pointer;
    leaf->totalKeys++;
    return leaf;
}

BpTNode * insertIntoLeafAfterSplitting(BTreeManagement * treeManager, BpTNode * leaf, Value * key, NodeRecordDetails * pointer) {
    
    int pointOfInsertion = 0, splitValue = 0, key, i, j;
    Value ** tempKey;
    void ** tempPtr;
    BpTNode * newLeafNode;

    newLeafNode = createALeafNode(treeManager);
    int bTreeOrder = treeManager->whichOrder;

    tempKey = malloc (bTreeOrder * sizeof(Value));
    if (tempKey == NULL) {
        printf("The keys are allocation is not SUCCESSFUL");
		exit(RC_MAKE_INSERT_KEY_ERROR);
	}

    tempPtr = malloc (bTreeOrder * sizeof(void *));
    if (tempPtr == NULL) {
        printf("The pointer are allocation is not SUCCESSFUL");
        exit(RC_MAKE_INSERT_PTR_ERROR);
    }

    while (pointOfInsertion < (bTreeOrder - 1) && isLess(leaf->keyNode[pointOfInsertion], key))
        pointOfInsertion++;

    for (int indexI = 0, indexJ = 0; indexI < leaf->totalKeys; indexI++, indexJ++) {
        if (indexJ == pointOfInsertion)
            indexJ++;

        tempKey[indexJ] = leaf->keyNode[indexI];
        tempPtr[indexJ] = leaf->ptr[indexI];
    }

    tempKey[pointOfInsertion] = key;
    tempPtr[pointOfInsertion] = pointer;

    leaf->totalKeys = 0;

        


}

BpTNode * createALeafNode(BTreeManagement * bpTree) {
    BpTNode * leafNode = createANewLeaf(bpTree);
    leafNode->isLeafNode = true;
    return leafNode;
}

BpTNode * createANewLeaf(BTreeManagement * bpTree) {
    bpTree->totalNodes++;
    int bTreeOrder = bpTree->whichOrder;

    // memory allocation
    BpTNode * tempNode = malloc(sizeof(BpTNode));
    if (tempNode == NULL) {
        printf("Node cannot be created or memory allocation is made.");
        exit(RC_MAKE_INSERT_ERROR);
    }

    // memory for keynode
    tempNode->keyNode = malloc((bTreeOrder - 1) * sizeof(Value *));
    if (tempNode->keyNode == NULL) {
        printf("keyNode key is made new or new memory allocation is made.");
        exit(RC_MAKE_INSERT_KEY_ERROR);
    }

    // memory for pointer
    tempNode->ptr = malloc((bTreeOrder * sizeof(void *)));
    if(tempNode->ptr == NULL) {
        printf("Ptr is NULL and no memory is allocated");
        exit(RC_MAKE_INSERT_PTR_ERROR);
    }

    tempNode->isLeafNode = false;
    tempNode->totalKeys = 0;
    tempNode->parent = NULL;
    tempNode->next = NULL;
    return tempNode;
}

/* ============================================================================================================ */

bool isLess(Value * K1, Value * K2) {
    switch (K1->dt) {
        case DT_INT:
            if (K1->v.intV < K2->v.intV)
                return TRUE;
            else 
                return FALSE;

            break;
        
        case DT_FLOAT:
            if (K1->v.floatV < K2->v.floatV)
                return TRUE;
            else 
                return FALSE;

            break;

        case DT_STRING:
            if (strcmp(K1->v.stringV, K2->v.stringV) == -1)
                return TRUE;
            else 
                return FALSE;

            break;   

        case DT_BOOL:
            return FALSE;

            break;                       
    }
}

bool isGreater(Value * K1, Value * K2) {
    switch (K1->dt) {
        case DT_INT:
            if (K1->v.intV > K2->v.intV) 
                return TRUE;
            else 
                return FALSE;
            
            break;

        case DT_FLOAT:
            if (K1->v.floatV > K2->v.floatV) 
                return TRUE;
            else 
                return FALSE;
            
            break;

        case DT_STRING:
            if (strcmp(K1->v.stringV, K2->v.stringV) == 1) 
                return TRUE;
            else 
                return FALSE;
            
            break;      

        case DT_BOOL:
            return FALSE;
            break;            
    }
}

bool isEqual(Value * K1, Value * K2) {
    switch (K1->dt) {
        case DT_INT:
            if (K1->v.intV == K2->v.intV) 
                return TRUE;
            else 
                return FALSE;
            
            break;

        case DT_FLOAT:
            if (K1->v.floatV == K2->v.floatV) 
                return TRUE;
            else 
                return FALSE;
            
            break;

        case DT_STRING:
            if (strcmp(K1->v.stringV, K2->v.stringV) == 0) 
                return TRUE;
            else 
                return FALSE;
            
            break;      

        case DT_BOOL:
            if (K1->v.boolV == K2->v.boolV) 
                return TRUE;
            else 
                return FALSE;
            break;              
    }
}
