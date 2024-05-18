#include "dberror.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "tables.h"
#include "btree_implement.h"
#include "stdlib.h"
/* ============== FUNCTION DECLARATIONS STARTED ============== */

/* init and shutdown index manager */
RC initIndexManager (void * mgmtData); 
RC shutdownIndexManager ();

/* create, destroy, open, and close an btree index */
RC createBtree(char *idxId, DataType keyType, int n);
RC openBtree(BTreeHandle **tree, char *idxId);
RC closeBtree(BTreeHandle *tree);
RC deleteBtree(char *fileName);

/* access information about a b-tree */
RC getNumNodes(BTreeHandle *tree, int *result);
RC getNumEntries(BTreeHandle *tree, int *result);
RC getKeyType(BTreeHandle *tree, DataType *result);

/* index access */
RC findKey(BTreeHandle *tree, Value *key, RID *result);
RC insertKey(BTreeHandle *tree, Value *key, RID rid);
RC deleteKey(BTreeHandle *tree, Value *key);
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle);
RC nextEntry(BT_ScanHandle *handle, RID *result);
RC closeTreeScan(BT_ScanHandle *handle);

/* NECESSARY CUSTOM FUNCTIONS */
BpTNode * findLeftmostLeafNode (BpTNode *node);
char * printNode(BPTreeManagement * manager, BpTNode * node, int lastPriority, int presentRank);

/* ============== FUNCTION DECLARATIONS ENDED ============== */

BPTreeManagement * treeTracker = NULL;

RC initIndexManager (void * mgmtData) {
    initStorageManager();
    printf("Index manager initialized.");
    return RC_OK;
}

RC shutdownIndexManager () {
    treeTracker = NULL;
    printf("Operation shut down index manager successful.");
    return RC_OK;
}

RC createBtree(char *idxId, DataType keyType, int n) {
    int capacity = PAGE_SIZE / sizeof(BpTNode);

    SM_FileHandle fHandler;
    char info[PAGE_SIZE] = {0};

    RC status = RC_OK;

    if (n > capacity) {
        printf("\nThe n has reached maximum nodes with n = %d & maximum nodes = %d, now you cannot add anything.\n", n, capacity);
        return RC_ORDER_TOO_HIGH_FOR_PAGE;
    }

    treeTracker = (BPTreeManagement *) malloc(sizeof(BPTreeManagement));
    if (!treeTracker) {
        perror("\nMemory allocation for BPTree Manager FAILED\n");
        return RC_MEMORY_ALLOCATION_MANAGER_ERROR;
    }
    
    treeTracker->keyDataType = keyType;
    treeTracker->orderType = n + 2;  
    treeTracker->totalNodes = 0;
    treeTracker->totalEntries = 0;

    treeTracker->rootNode = NULL;
    treeTracker->queueNode = NULL;

    BM_BufferPool * bufferPoolManager = (BM_BufferPool *) malloc(sizeof(BM_BufferPool));
    if (!bufferPoolManager) {
        free(treeTracker);
        printf("\n Memory Allocation for bufferPoolManager FAILED. \n");
        return RC_MEMORY_ALLOCATION_MANAGER_ERROR;
    }

    treeTracker->buffer = * bufferPoolManager;

    // RC createPageFile(char *fileName)
    status = createPageFile(idxId);
    if (status != RC_OK) {
        return status;
    }

    // RC openPageFile(char *fileName, SM_FileHandle *fHandle)
    status = openPageFile(idxId, &fHandler);
    if (status != RC_OK) {
        return status;
    }

    // RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
    status = writeBlock(0, &fHandler, info);
    if (status != RC_OK) {
        return status;
    }

    // RC closePageFile(SM_FileHandle *fHandle)
    status = closePageFile(&fHandler);
    if (status != RC_OK) {
        return status;
    }

    return RC_OK;
}

RC openBtree(BTreeHandle **tree, char *idxId) {
    *tree = (BTreeHandle *) malloc(sizeof(BTreeHandle));
    if (!*tree) {
        perror("\nFailed to allocate memory for B-tree handle.\n");
        return RC_MEMORY_ALLOCATION_FAIL;
    }

    (*tree)->mgmtData = treeTracker;

    RC initStatus = initBufferPool(&treeTracker->buffer, tree, 1000, RS_FIFO, NULL);
    if (initStatus != RC_OK) {
        *tree = NULL;
        free(*tree);  
        return initStatus;
    }

    return RC_OK;
}

RC closeBtree(BTreeHandle *tree) {
	if (!tree) {
        printf("\n tree is NULL. \n");
        return RC_MANAGER_NULL_ERROR;
    }

	BPTreeManagement * manager = (BPTreeManagement*) tree->mgmtData;
	if (!manager) {
        printf("\nInvalid Exception on tree. Error on closing BTree.\n");
        return RC_MANAGER_NULL_ERROR;
    }

    // RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
	markDirty(&treeTracker->buffer, &treeTracker->pHandler);

    // RC terminateBufferManager(BM_BufferPool *const pool)
	shutdownBufferPool(&treeTracker->buffer);
    
    treeTracker = tree = NULL;

	free(treeTracker);
	free(tree);

	return RC_OK;
}

RC deleteBtree(char *fileName) {
    if (fileName == NULL) {
        printf("\n Tree is NULL. \n");        
        return RC_INVALID_FILENAME;
    }

    // RC destroyPageFile(char *fileName)
    RC opStatus = destroyPageFile(fileName);
    if(opStatus != RC_OK) {
        printf("\n Some issue while deleting BTree \n");
        return RC_B_PLUS_TREE_NOT_DELETING;
    }

    printf("\n B + Tree deleted SUCCESSFULLY.\n");

    return RC_OK;
}

RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
    NodeRecordData *ptr;
    BpTNode *leaf;
    BPTreeManagement * treeManager = (BPTreeManagement *) tree->mgmtData;
    int bTreeOrder = treeManager->orderType;
    
    NodeRecordData * records = findRecord(treeManager->rootNode, key);
    if (records != NULL) {
        printf("The key that is going to be INSERTED is EXISTS already.");
        return RC_ALREADY_EXISTED_KEY;
    } else if (treeManager->rootNode == NULL) {

        ptr = makeRecord(&rid);

         // BpTNode * createNewTree(BPTreeManagement * treeManager, Value* key, NodeRecordData* pointer) {
        treeManager->rootNode = createNewTree(treeManager, key, ptr);
        return RC_OK;
    } else {

        // BpTNode *findLeaf(BpTNode *root, Value *key)
        leaf = findLeaf(treeManager->rootNode, key);
    }

    ptr = makeRecord(&rid);
    if (leaf->totalKeys < (bTreeOrder - 1)) {

        // BpTNode *insertIntoLeaf(BPTreeManagement *treeManager, BpTNode *leaf, Value *key, NodeRecordData *pointer)
        insertIntoLeaf(treeManager, leaf, key, ptr);
     
    } else {

        // BpTNode *insertIntoLeafAfterSplitting(BPTreeManagement *treeManager, BpTNode *leaf, Value *key, NodeRecordData *pointer)
        treeManager->rootNode = insertIntoLeafAfterSplitting(treeManager, leaf, key, ptr);
    }

    return RC_OK;
}

RC findKey(BTreeHandle *tree, Value *key, RID *result) {
    BPTreeManagement * bpTreeStruct = (BPTreeManagement *) tree->mgmtData;
    NodeRecordData * searchedRecords = findRecord(bpTreeStruct->rootNode, key);

    if (searchedRecords == NULL) {
        return RC_IM_KEY_NOT_FOUND;
    } else {
        *result = searchedRecords->rid;
        return RC_OK;
    }
}

RC getNumNodes(BTreeHandle *tree, int *result) {

    if (tree == NULL) {
        printf("\nBTreeHandle is NULL.\n");
        return RC_NULL_FOR_TREE_MANAGER; 
    }

    if (result == NULL) {
        printf("\result is NULL.\n");
        return RC_NULL_FOR_TREE_MANAGER; 
    }

    BPTreeManagement * treeManager = (BPTreeManagement *) tree->mgmtData;
    if (treeManager == NULL) {
        printf("\n Cannot get the Number of nodes. \n");
        return RC_NULL_FOR_TREE_MANAGER;
    }

    *result = treeManager->totalNodes;
    return RC_OK;
}


RC getNumEntries(BTreeHandle *tree, int *result) {
     if (tree == NULL) {
        printf("\nBTreeHandle is NULL.\n");
        return RC_NULL_FOR_TREE_MANAGER; 
    }

    if (result == NULL) {
        printf("\result is NULL.\n");
        return RC_NULL_FOR_TREE_MANAGER; 
    }

    BPTreeManagement * treePManager = (BPTreeManagement *) tree->mgmtData;
    if (treePManager == NULL) {
        printf("\n Cannot get the Number of nodes. \n");
        return RC_NULL_FOR_TREE_MANAGER;
    }

    *result = treePManager->totalEntries;
    return RC_OK;
}


RC getKeyType(BTreeHandle *tree, DataType *result) {
    if (tree == NULL) {
        printf("\nBTreeHandle is NULL.\n");
        return RC_NULL_FOR_TREE_MANAGER; 
    }

    if (result == NULL) {
        printf("\result is NULL.\n");
        return RC_NULL_FOR_TREE_MANAGER; 
    }

    BPTreeManagement * treePManager = (BPTreeManagement *)tree->mgmtData;
    if (treePManager == NULL) {
        printf("\n Cannot get the Number of nodes. \n");
        return RC_NULL_FOR_TREE_MANAGER;
    }

    *result = treePManager->keyDataType;
    return RC_OK;
}

 
RC deleteKey(BTreeHandle *tree, Value *key) {
    if (tree == NULL) {
        printf("\nBTreeHandle is NULL.\n");
        return RC_NULL_FOR_TREE_MANAGER; 
    }

    if (key == NULL) {
        printf("\nkey is NULL.\n");
        return RC_NULL_FOR_TREE_MANAGER; 
    }

    BPTreeManagement * managerData = (BPTreeManagement *) tree->mgmtData;
    if (managerData == NULL) {
        printf("\n NULL - Cannot get the Number of nodes. \n");
        return RC_NULL_FOR_TREE_MANAGER;    
    }

    void *flag = delete(managerData, key); 
    if (flag == NULL) {
        printf("\n NULL - The RC for deletion is NULL. \n");
        return RC_BTREE_DELETE_FAILED; 
    }

    managerData->rootNode = flag;
    return RC_OK;
}

RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
    NodeSearchManagement * searchedMgr = (NodeSearchManagement *) malloc(sizeof(NodeSearchManagement));
    BPTreeManagement *treeData = (BPTreeManagement *) tree->mgmtData;
    BpTNode * leftmost = findLeftmostLeafNode (treeData->rootNode);

    *handle = (BT_ScanHandle *) malloc(sizeof(BT_ScanHandle));

    RC rc = RC_OK;
    if (leftmost == NULL) {
        rc = RC_NO_INFO_TO_SCAN;
    } else {
        searchedMgr->keyIValue = 0;
        searchedMgr->totalKeys = leftmost->totalKeys; 
        searchedMgr->node = leftmost;
        searchedMgr->order = treeData->orderType; 
        (*handle)->mgmtData = searchedMgr;
    }
    
    return rc;
}

BpTNode *findLeftmostLeafNode (BpTNode *node) {
    if(node != NULL && node->isLeafNode == false) 
        return findLeftmostLeafNode (node->ptr[0]);
    return node;
}

RC nextEntry(BT_ScanHandle *handle, RID *result) {
    if (handle == NULL) {
        printf("NULL ERROR: Null pointer for Scan handler structure.\n");
        return RC_ERROR;
    }

    if (result == NULL) {
        printf("NULL ERROR: Null pointer for RID structure .\n");
        return RC_ERROR; 
    }

    NodeSearchManagement * searchStruct = (NodeSearchManagement *) handle->mgmtData;
    if (!searchStruct) {
        printf("NULL EXCEPTION: If search manager structure is null.");
        return RC_IM_NO_MORE_ENTRIES;
    }
    if (!searchStruct->node) {
        printf("NULL EXCEPTION: If search manager struct's nodes is null.");
        return RC_IM_NO_MORE_ENTRIES;
    }

    BpTNode *node = searchStruct->node;
    int keyIth = searchStruct->keyIValue, totalNumberKeys = searchStruct->totalKeys;

    if (keyIth < totalNumberKeys) {
        *result = ((NodeRecordData *) node->ptr[keyIth])->rid;
        searchStruct->keyIValue++;  
        return RC_OK;
    }

    BpTNode *nextNode = node->ptr[searchStruct->order - 1]; 
    if (nextNode) {
        searchStruct->keyIValue = 0;
        searchStruct->totalKeys = nextNode->totalKeys;
        searchStruct->node = nextNode;

        if (searchStruct->totalKeys > 0) {
            searchStruct->keyIValue = 1; 
            *result = ((NodeRecordData *) nextNode->ptr[0])->rid;
            return RC_OK;
        }
    }

    return RC_IM_NO_MORE_ENTRIES;
}

RC closeTreeScan(BT_ScanHandle *handle) {
    if (handle != NULL) {  
        handle->mgmtData = NULL; 
        handle = NULL;
        free(handle);  
        return RC_OK;  
    }
    return RC_ERROR;  
}

char *printTree(BTreeHandle *tree) {
    BPTreeManagement * treeManager = (BPTreeManagement *) tree->mgmtData;
    printf("\nNow priting the B+Tree : \n");

    if (treeManager->rootNode == NULL) {
        printf("\nNo root exists and So, its a empty tree.\n");
        return NULL;
    } else {
        return printNode(treeManager, treeManager->rootNode, -1, 0);
    }
}

char * printNode(BPTreeManagement * manager, BpTNode * node, int lastPriority, int presentRank) {
    int i = 0, tempRank;

    if (node->internalNode != NULL && node == node->internalNode->ptr[0]) {
        tempRank = pathForRoot(manager->rootNode, node);
    } else {
        tempRank = lastPriority;
    }
    
    if (tempRank != lastPriority) {
        lastPriority = tempRank;
    }

    while (i < node->totalKeys) {
        switch (manager->keyDataType) {
            case DT_INT: printf("\n INT TYPE : %d \n", (*node->keyNode[i]).v.intV);
                         break;

            case DT_FLOAT: printf("\n FLOAT TYPE : %.4f \n", (*node->keyNode[i]).v.floatV);
                         break;

            case DT_STRING: printf("\n STRING TYPE : %s \n", (*node->keyNode[i]).v.stringV);
                         break;

            case DT_BOOL: printf("BOOL TYPE : %d ", (*node->keyNode[i]).v.boolV);
                         break;
        }
        i++;
    } 

    if (node->isLeafNode == false) {
        for (int i = 0; i <= node->totalKeys; i++) {
            printNode(manager, node->ptr[i], presentRank + 1, tempRank);
        }
    }

    return NULL;
}