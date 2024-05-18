#include "btree_implement.h"
#include "dt.h"
#include "string.h"
#include "stdlib.h"

void safeAssignParent(BpTNode * child, BpTNode * parent);
Value* createIntValue(int num);

Value * createIntValue(int num) {
    Value * valueNum = (Value *)calloc(1, sizeof(Value));
    if (valueNum == NULL) {
        printf("\n NULL | ERROR : unable to allocate memery for value. check it. \n");
        exit(EXIT_FAILURE);
    }

    valueNum->v.intV = num;
    valueNum->dt = DT_INT;
    return valueNum;
}

NodeRecordData * makeRecord(RID * rid) {
    NodeRecordData * record = (NodeRecordData *) calloc(1, sizeof(NodeRecordData));
    if (record == NULL) {
        printf("\n NULL | ERROR : Memory allocation for records failed. \n");
        exit(RC_INSERT_ERROR);
    } 

    record->rid = *rid;
    return record;
}

BpTNode * createNewTree(BPTreeManagement * treeManager, Value* key, NodeRecordData* pointer) {
    if (treeManager == NULL) {
        printf("\n NULL | ERROR : function Parameter - treeManager is NULL. \n");
        exit(EXIT_FAILURE);
    } if (key == NULL) {
        printf("\n NULL | ERROR : function Parameter - key is NULL. \n");
        exit(EXIT_FAILURE);
    } if (pointer == NULL) {
        printf("\n NULL | ERROR : function Parameter - pointer is NULL. \n");
        exit(EXIT_FAILURE);
    }

    if (treeManager->orderType <= 0) {
        printf("NULL | Error: The orderType is invalid and is <= 0. The value presented here is : %d \n", treeManager->orderType);
        return NULL;
    }

    BpTNode * bpTree = createLeaf(treeManager);
    if (bpTree == NULL) {
        printf("Error: Failed to create a new leaf node in createNewTree.\n");
        return NULL;
    }

    bpTree->ptr[(treeManager->orderType) - 1] = NULL;

    bpTree->keyNode[0] = key;
    bpTree->ptr[0] = pointer;

    bpTree->totalKeys = 1;
    bpTree->internalNode = NULL;

    treeManager->totalEntries++;

    return bpTree;
}

BpTNode * insertIntoLeaf(BPTreeManagement * treeManager, BpTNode * leafNode, Value* key, NodeRecordData* pointer) {
    if (treeManager == NULL) {
        printf("\n NULL | ERROR : function Parameter - treeManager is NULL. \n");
        exit(EXIT_FAILURE);
    } if (key == NULL) {
        printf("\n NULL | ERROR : function Parameter - key is NULL. \n");
        exit(EXIT_FAILURE);
    } if (pointer == NULL) {
        printf("\n NULL | ERROR : function Parameter - pointer is NULL. \n");
        exit(EXIT_FAILURE);
    } if (leafNode == NULL) {
        printf("\n NULL | ERROR : function Parameter - leaf is NULL. \n");
        exit(EXIT_FAILURE);
    } if (leafNode->totalKeys >= (treeManager->orderType - 1)) {
        fprintf(stderr, "Error: Leaf node is full, cannot insert new key.\n");
        return NULL;
    }

    int totalKeys = leafNode->totalKeys;

    while (totalKeys > 0 && isLess(key, leafNode->keyNode[totalKeys - 1]))         
        totalKeys--;


    if (totalKeys < leafNode->totalKeys) {
        int remainingLen = (leafNode->totalKeys - totalKeys);
        memmove(&leafNode->ptr[totalKeys + 1], &leafNode->ptr[totalKeys], sizeof(NodeRecordData*) * remainingLen);
        memmove(&leafNode->keyNode[totalKeys + 1], &leafNode->keyNode[totalKeys], sizeof(Value*) * remainingLen);
    } else {
        // Do nothing ... 
    }

    leafNode->ptr[totalKeys] = pointer;
    leafNode->keyNode [totalKeys] = key;

    leafNode->totalKeys++;
    treeManager->totalEntries++;

    return leafNode;
}

BpTNode * insertIntoLeafAfterSplitting(BPTreeManagement * treeManager, BpTNode * leaf, Value* key, NodeRecordData* pointer) {
  
    if (treeManager == NULL) {
        printf("\n Error: treeManager is NULL. \n");
        exit(EXIT_FAILURE);
    } if (leaf == NULL) {
        printf("\n Error: leaf is NULL. \n");
        exit(EXIT_FAILURE);
    } if (key == NULL) {
        printf("\n Error: key is NULL. \n");
        exit(EXIT_FAILURE);
    } if (pointer == NULL) {
        printf("\n Error: pointer is NULL. \n");
        exit(EXIT_FAILURE);
    }
    
    int ithInsert = 0, orderType = treeManager->orderType;
    BpTNode * newNode = createLeaf(treeManager);
    if (newNode == NULL) {
        printf("Error: Memory Allocation for new leaf node failed.\n");
        exit(EXIT_FAILURE);
    }

    void ** tempPtr = malloc(orderType * sizeof(void*));
    Value ** newKeys = malloc(orderType * sizeof(Value*));
    if (newKeys == NULL) {
        perror("\n Memory allocation for keys failed. \n");
        exit(EXIT_FAILURE);
    }
    if (tempPtr == NULL) {
        perror("Error allocating space for temporary arrays.");
        exit(EXIT_FAILURE);
    }

    bool lessFlag = isLess(leaf->keyNode[ithInsert], key);
    while ((ithInsert < orderType - 1) && (isLess(leaf->keyNode[ithInsert], key))) {
        ithInsert++;
    }

    if (ithInsert > 0) {
        int size1 = ithInsert * sizeof(void *);
        int size2 = ithInsert * sizeof(Value *);
        memcpy(tempPtr, leaf->ptr, size1);
        memcpy(newKeys, leaf->keyNode, size2);
    }

    tempPtr[ithInsert] = pointer;
    newKeys[ithInsert] = key;
    if (ithInsert < leaf->totalKeys) {
        int ValueSize1 = (leaf->totalKeys - ithInsert) * sizeof(Value *);
        int voidSize2 = (leaf->totalKeys - ithInsert) * sizeof(void *);
        memcpy(newKeys + ithInsert + 1, (leaf->keyNode  + ithInsert), ValueSize1);
        memcpy(tempPtr + ithInsert + 1, (leaf->ptr + ithInsert), voidSize2);
    }

    leaf->totalKeys = 0;
    int splitPoint = (orderType - 1) / 2 + ((orderType - 1) % 2);

    int ValueSplitSize = splitPoint * sizeof(Value *);
    int voidSplitSize = splitPoint * sizeof(void *);
    memcpy(leaf->keyNode , newKeys, ValueSplitSize);
    memcpy(leaf->ptr, tempPtr, voidSplitSize);
    leaf->totalKeys = splitPoint;

    int diff = (orderType - splitPoint);
    int diff1Size = diff * sizeof(Value *);
    int diff2Size = diff * sizeof(void *);
    memcpy(newNode->keyNode , newKeys + splitPoint, diff1Size);
    memcpy(newNode->ptr, tempPtr + splitPoint, diff2Size);
    newNode->totalKeys = diff;

    tempPtr = newKeys = NULL;
    free(tempPtr);
    free(newKeys);

    newNode->ptr[orderType - 1] = leaf->ptr[orderType - 1];
    leaf->ptr[orderType - 1] = newNode;

    int oldPtrLeafSize1 = (orderType - 1 - leaf->totalKeys) * sizeof(void *);
    int oldPtrNodeSize2 = (orderType - 1 - newNode->totalKeys) * sizeof(void *);
    memset(leaf->ptr + leaf->totalKeys, 0, oldPtrLeafSize1);
    memset(newNode->ptr + newNode->totalKeys, 0, oldPtrNodeSize2);

    newNode->internalNode = leaf->internalNode;
    treeManager->totalEntries++;

    return insertIntoParent(treeManager, leaf, newNode->keyNode[0], newNode);
}

BpTNode * insertIntoNodeAfterSplitting(BPTreeManagement * treeManager, BpTNode * prevNode, int leftI, Value* key, BpTNode * right) {
   if (treeManager == NULL) {
        printf("\nError: treeManager struct is NULL and cannot be used further\n");
        exit(EXIT_FAILURE);
    } if (prevNode == NULL) {
        printf("\nError: oldNode struct is NULL and cannot be used further\n");
        exit(EXIT_FAILURE);
    } if (key == NULL) {
        printf("\nError: key is NULL and cannot be used further\n");
        exit(EXIT_FAILURE);
    } if (right == NULL) {
        printf("\nError: right Node is NULL and cannot be used further\n");
        exit(EXIT_FAILURE);
    }

    int orderType = treeManager->orderType;
    BpTNode * newBTNode = createNode(treeManager);
    if (newBTNode == NULL) {
        printf("\nError: A new node is not created.\n");
        exit(EXIT_FAILURE);
    }

    BpTNode ** newPtr = malloc((orderType + 1) * sizeof(BpTNode *));
    Value ** newKeys = malloc (orderType * sizeof(Value*));
    if (newPtr == NULL) {
        printf("\nError: Memory allocation for ptrs went wrong.\n");
        exit(EXIT_FAILURE);
    } if (newKeys == NULL) {
        printf("\nError: Memory allocation for keys went wrong.\n");
        exit(EXIT_FAILURE);
    }

    int i = 0, j = 0;
    for (i = 0, j = 0; i < prevNode->totalKeys + 1; i++, j++) {
        if (j == leftI + 1) j++;
        newPtr[j] = prevNode->ptr[i];
    }
    newPtr[leftI + 1] = right;

    for (i = 0, j = 0; i < prevNode->totalKeys; i++, j++) {
        if (j == leftI) j++;
        newKeys[j] = prevNode->keyNode [i];
    }
    newKeys[leftI] = key;

    int splittedIndex = (orderType - 1) / 2 + ((orderType - 1) % 2);
    prevNode->totalKeys = 0;
    i = 0;
    while (i < splittedIndex) {
        prevNode->totalKeys++;
        prevNode->ptr[i] = newPtr[i];
        prevNode->keyNode[i] = newKeys[i];
        i++;
    }
    prevNode->ptr[i] = newPtr[i];

    Value * tempValue = newKeys[splittedIndex];
    i = (splittedIndex + 1);
    j=0; 
    while (i < orderType) {
        newBTNode->keyNode [j] = newKeys[i];
        newBTNode->ptr[j] = newPtr[i];
        newBTNode->totalKeys++;
        i++;j++;
    }
    newBTNode->ptr[j] = newPtr[i];

    newPtr = newKeys = NULL;
    free(newPtr);
    free(newKeys);

    newBTNode->internalNode = prevNode->internalNode;
    i = 0;
    while (i <= newBTNode->totalKeys) {
        BpTNode * child = newBTNode->ptr[i];
        child->internalNode = newBTNode;
        i++;
    }

    treeManager->totalEntries++;
    BpTNode * insertedNode =  insertIntoParent(treeManager, prevNode, tempValue, newBTNode);
    return insertedNode;
}

BpTNode * insertIntoParent(BPTreeManagement * treeManager, BpTNode * left, Value* key, BpTNode * right) {

    if (treeManager == NULL) {
        printf("\nError: tree manager struct is NULL.\n");
        exit(EXIT_FAILURE);
    } if (left == NULL) {
        printf("\nError: left struct is NULL.\n");
        exit(EXIT_FAILURE);
    } if (key == NULL) {
        printf("\nError: key struct is NULL.\n");
        exit(EXIT_FAILURE);
    } if (right == NULL) {
        printf("\nError: right struct is NULL.\n");
        exit(EXIT_FAILURE);
    }

    BpTNode * parent = left->internalNode;
    if (parent == NULL) {
        return insertIntoNewRoot(treeManager, left, key, right);
    }

    int leftIndex = getLeftIndex(parent, left);
    if (parent->totalKeys < (treeManager->orderType - 1)) {
        return insertIntoNode(treeManager, parent, leftIndex, key, right);
    }
    BpTNode * aftSplit = insertIntoNodeAfterSplitting(treeManager, parent, leftIndex, key, right);
    return aftSplit;
}

int getLeftIndex(BpTNode * parent, BpTNode * left) {
    int leftI = 0;

    if (parent == NULL ) {
        printf("Error: The parent struct is NULL.\n");
        return -1;
    } if (left == NULL) {
        printf("Error: The parent struct is NULL.\n");
        return -1;
    }

    for (leftI = 0; ((leftI <= parent->totalKeys) && (parent->ptr[leftI] != left)); ) {
        leftI++;
    }

    if (leftI > parent->totalKeys) {
        printf("\nThe left index is greater than parents total keys in getLeftIndex().\n");
        return -1;
    } else {
        return leftI;
    }
}

BpTNode * insertIntoNode(BPTreeManagement * treeManager, BpTNode * parent, int prevIndex, Value * key, BpTNode * right) {
    if (treeManager == NULL) {
        printf("\nError: The tree Manager struct is NULL.\n");
        exit(EXIT_FAILURE);
    } if (key == NULL) {
        printf("\nError: The key struct is NULL.\n");
        exit(EXIT_FAILURE);
    } if (parent == NULL) {
        printf("\nError: The parent struct is NULL.\n");
        exit(EXIT_FAILURE);
    } if (right == NULL) {
        printf("\nError: The right struct is NULL.\n");
        exit(EXIT_FAILURE);
    }

    if (prevIndex < 0 || prevIndex > parent->totalKeys) {
        printf("\nError: Cannot be inserted because previous index is greater than or less than zero total keys present. \n");
        exit(EXIT_FAILURE);
    }

    int i = parent->totalKeys, createPos = prevIndex + 1;  

    while (i > prevIndex) {
        parent->ptr[i + 1] = parent->ptr[i];
        parent->keyNode[i] = parent->keyNode[i - 1];     
        i--;
    }

    parent->totalKeys++;
    parent->keyNode [prevIndex] = key;   
    parent->ptr[createPos] = right;  
     
    return treeManager->rootNode;   
}

BpTNode * insertIntoNewRoot(BPTreeManagement * treeManager, BpTNode * left, Value* key, BpTNode * right) {
    if (treeManager == NULL) {
        printf("\nError: The tree Manager struct is NULL.\n");
        exit(EXIT_FAILURE);
    } if (key == NULL) {
        printf("\nError: The key struct is NULL.\n");
        exit(EXIT_FAILURE);
    } if (left == NULL) {
        printf("\nError: The left struct is NULL.\n");
        exit(EXIT_FAILURE);
    } if (right == NULL) {
        printf("\nError: The right struct is NULL.\n");
        exit(EXIT_FAILURE);
    }

    // BpTNode *createNode(BPTreeManagement *treeManager)
    BpTNode * root = createNode(treeManager);
    if (root == NULL) {
        printf("\n The new Node is not created properly. \n");
        exit(EXIT_FAILURE);
    }

    root->totalKeys = 1; 
    root->keyNode [0] = key;                  
    root->ptr[0] = left;            
    root->ptr[1] = right;             
                     
    root->internalNode = NULL;                  

    // void safeAssignParent(BpTNode *child, BpTNode *parent)
    safeAssignParent(left, root);         

    // void safeAssignParent(BpTNode *child, BpTNode *parent)
    safeAssignParent(right, root);        

    return root;                          
}

void safeAssignParent(BpTNode * childrenNode, BpTNode * parentNode) {
    if (childrenNode == NULL) {                  
        // Do nothing
    } else {
        childrenNode->internalNode = parentNode;          
    }
}

BpTNode * createNode(BPTreeManagement * treeManager) {
    if (treeManager == NULL) {
        printf("\nError: The tree Manager struct is NULL.\n");
        exit(EXIT_FAILURE);
    }

    treeManager->totalNodes++;

    BpTNode * tempNode = (BpTNode *) malloc(sizeof(BpTNode));
    if (tempNode == NULL) {
        printf("\n Error! The MEMORY ALLOCATION failed for BpTNode. \n");
        return NULL;
    }

    tempNode->keyNode  = (Value **) malloc((treeManager->orderType - 1) * sizeof(Value*));
    if (!tempNode->keyNode ) {
        perror("\n Error! The MEMORY ALLOCATION failed for Value **. \n");
        tempNode = NULL;
        free(tempNode); 
        return NULL;
    }

    tempNode->ptr = (void**) malloc(treeManager->orderType * sizeof(void*));
    if (!tempNode->ptr) {
        printf("\n Error! The MEMORY ALLOCATION failed for void **. \n");
        
        tempNode->keyNode = NULL;
        free(tempNode->keyNode); 

        tempNode = NULL;  
        free(tempNode);  

        return NULL;
    }

    tempNode->internalNode = NULL;     
    tempNode->next = NULL;  
    
    tempNode->isLeafNode = false;   
    tempNode->totalKeys = 0;  
    
    return tempNode;
}

BpTNode * createLeaf(BPTreeManagement * treeManager) {
     if (treeManager == NULL) {
        printf("\nError: The tree Manager struct is NULL.\n");
        exit(EXIT_FAILURE);
    }

    // BpTNode *createNode(BPTreeManagement *treeManager)
    BpTNode * leafNode = createNode(treeManager);
    if (leafNode == NULL) {
        printf("\n The new Node is not created properly. \n");
        exit(EXIT_FAILURE);
    }

    leafNode->isLeafNode = true;
    return leafNode;
}

BpTNode * findLeafHelper(BpTNode * node, Value * key, int index) {
    bool isgreater = isGreater(key, node->keyNode [index]);
    bool isequal = isEqual(key, node->keyNode [index]);

    if (index < node->totalKeys && (isgreater || isequal)) {
        return findLeafHelper(node, key, index + 1);
    } else {
        return findLeaf((BpTNode *)node->ptr[index], key);
    }
}

BpTNode * findLeaf(BpTNode * root, Value * key) {
    if ((root == NULL || root->isLeafNode)) { 
        return root; 
    }  else { 
        return findLeafHelper(root, key, 0); 
    }
}

NodeRecordData * findRecordHelper(BpTNode * node, Value * key, int index) {
    if (node == NULL ) {
        printf("ERROR! The node is NULL.");
        return NULL;
    } if (index >= node->totalKeys) {
        printf("ERROR! Index is greater than total keys");
        return NULL;
    }

    bool isequal = isEqual(node->keyNode[index], key);
       
    if (isequal) {
        return (NodeRecordData *) node->ptr[index];
    } else {
        return findRecordHelper(node, key, index + 1);
    }
}

NodeRecordData * findRecord(BpTNode * root, Value * key) {
    // BpTNode *findLeaf(BpTNode *root, Value *key)
    BpTNode * searchedNde = findLeaf(root, key);
    if (searchedNde == NULL) 
        return NULL;  

    // NodeRecordData *findRecordHelper(BpTNode *node, Value *key, int index)
    return findRecordHelper(searchedNde, key, 0); 
}

int getNeighborIndex(BpTNode * n) {
    if (n == NULL) {
        printf("ERROR: The BpTNode is NULL. \n");
        return -1; 
    } if (n->internalNode == NULL) {
        printf("\n ERROR! The Internal Node is NULL. \n");
        return -1;
    }

    int i = 0;
    while (i <= n->internalNode->totalKeys) {
        if (n == n->internalNode->ptr[i]) {
            return (i == 0) ? -1 : i - 1;
        }
        i++;
    }

    printf(stderr, "ERROR! Node is not available in any of the nodes.\n");
    return -1; 
}

BpTNode * removeEntryFromNode(BPTreeManagement * treeManager, BpTNode * bpTreeNode, Value * key, BpTNode * pointer) {
    if (treeManager == NULL) {
        printf("\nError: The tree Manager struct is NULL.\n");
        exit(EXIT_FAILURE);
    } if (bpTreeNode == NULL) {
        printf("\nError: The bpTreeNode struct is NULL.\n");
        exit(EXIT_FAILURE);
    } if (key == NULL) {
        printf("\nError: The key value struct is NULL.\n");
        exit(EXIT_FAILURE);
    } if (pointer == NULL) {
        printf("\nError: The bpTreeNode-pointer struct is NULL.\n");
        exit(EXIT_FAILURE);
    }

    int i = 0, j;
    while (i < bpTreeNode->totalKeys && !isEqual(bpTreeNode->keyNode [i], key))
        i++;

    j = i + 1;
    while (j < bpTreeNode->totalKeys) {
        bpTreeNode->keyNode [j - 1] = bpTreeNode->keyNode [j];
        j++;
    }

    int TotalPtrs = bpTreeNode->totalKeys + 1;

    if ( bpTreeNode->isLeafNode) {
        TotalPtrs = bpTreeNode->totalKeys;
    }

    for (i = 0; ((i < TotalPtrs) && (bpTreeNode->ptr[i] != pointer)); i++) {
        // Do nothing......
    }

    for (j = i; j < (TotalPtrs - 1); j++) {
        bpTreeNode->ptr[j] = bpTreeNode->ptr[j + 1];
    }

    treeManager->totalEntries--;
    bpTreeNode->totalKeys--;

    int indexUnItr = bpTreeNode->totalKeys + 1;

    if(bpTreeNode->isLeafNode) {
        indexUnItr = bpTreeNode->totalKeys;
    }

    j = indexUnItr;
    while (j < treeManager->orderType) {
        bpTreeNode->ptr[j] = NULL;
        j++;
    }

    return bpTreeNode;
}

BpTNode * adjustRoot(BpTNode * bpTreeNode) {
    if (bpTreeNode == NULL) {
        printf("Error: The bpTreeNode struct is NULL.\n");
        return NULL;
    }

    if (bpTreeNode->totalKeys > 0) 
        return bpTreeNode;
    

    BpTNode * tempNode = NULL;

    if (!bpTreeNode->isLeafNode) {
        tempNode = bpTreeNode->ptr[0];
        tempNode->internalNode = NULL; 
    }

    if (bpTreeNode != NULL) {
        if (bpTreeNode->keyNode != NULL) { 
            bpTreeNode->keyNode  = NULL;
            free(bpTreeNode->keyNode );
        }
        if (bpTreeNode->ptr != NULL) {  
            bpTreeNode->ptr = NULL;
            free(bpTreeNode->ptr);
        }
        bpTreeNode = NULL;
        free(bpTreeNode);  
        bpTreeNode = NULL; 
    }

    return tempNode;  
}

BpTNode * mergeNodes(BPTreeManagement * treeManager, BpTNode * allbptNode, BpTNode * neighbor, int neighbor_index, int value) {
    if (treeManager == NULL) {
        printf("\n Error: The given treeManager struct is Null\n");
        return NULL;
    } if (allbptNode == NULL) {
        printf("\n Error: The given allbptNode struct is Null\n");
        return NULL;
    } if (neighbor == NULL) {
        printf("\n Error: The given neighbor struct is Null\n");
        return NULL;
    }

    int i = 0, j = 0, nthIndex;
    BpTNode * tempNode = NULL;

    if (neighbor_index == -1) {
        tempNode = allbptNode;
        allbptNode = neighbor;
        neighbor = tempNode;
    }
    
    nthIndex = allbptNode->totalKeys;

    int neighbourIndex = neighbor->totalKeys;

    if (!allbptNode->isLeafNode) {

        // Value *createIntValue(int num)
        Value *valueKey = createIntValue(value);  

        neighbor->keyNode[neighbourIndex] = valueKey; 
        neighbor->totalKeys++; 

        for (i = neighbourIndex + 1, j = 0; j < nthIndex; i++, j++) {
            allbptNode->totalKeys--; 

            neighbor->ptr[i] = allbptNode->ptr[j];  
            neighbor->keyNode[i] = allbptNode->keyNode [j];   
              
            neighbor->totalKeys++;   
        }

        neighbor->ptr[i] = allbptNode->ptr[j];

        i = 0; 
        while (i <= neighbor->totalKeys) {
            tempNode = (BpTNode *)neighbor->ptr[i];  
            if (tempNode != NULL) { 
                tempNode->internalNode = neighbor;
            }  
            i++;
        }
    } else {
        for (i = neighbourIndex, j = 0; j < allbptNode->totalKeys; i++, j++) {
            neighbor->ptr[i] = allbptNode->ptr[j]; 

            neighbor->keyNode[i] = allbptNode->keyNode[j];  
            neighbor->totalKeys++;  
        }

        int orderIndex = treeManager->orderType - 1;
        neighbor->ptr[orderIndex] = allbptNode->ptr[orderIndex];
    }

    // Value *createIntValue(int num)
    Value * tempValueSt = createIntValue(value); 

    // BpTNode *deleteEntry(BPTreeManagement *treeManager, BpTNode *n, Value *key, void *pointer)
    treeManager->rootNode = deleteEntry(treeManager, allbptNode->internalNode, tempValueSt, allbptNode);

    if (allbptNode->keyNode == NULL) { 
        free(allbptNode->keyNode );  
    }  
    if (allbptNode->ptr == NULL) 
        free(allbptNode->ptr);

    free(allbptNode);  

    return treeManager->rootNode;  
}

BpTNode * deleteEntry(BPTreeManagement * treeManager, BpTNode * allbptNode, Value* key, void* pointer) {
    if (treeManager == NULL) {
        printf("\n Error: The given treeManager struct is Null\n");
        return NULL;
    } if (allbptNode == NULL) {
        printf("\n Error: The given allbptNode struct is Null\n");
        return NULL;
    } if (key == NULL) {
        printf("\n Error: The given key struct is Null\n");
        return NULL;
    } if (pointer == NULL) {
        printf("\n Error: The given key struct is Null\n");
        return NULL;
    }

    // BpTNode *removeEntryFromNode(BPTreeManagement *treeManager, BpTNode *bpTreeNode, Value *key, BpTNode *pointer)
    allbptNode = removeEntryFromNode(treeManager, allbptNode, key, pointer);

    if (allbptNode == treeManager->rootNode) {  
        // BpTNode *adjustRoot(BpTNode *bpTreeNode)
        return adjustRoot(treeManager->rootNode);
    }

    int minimumKeys = 0;
    if (!allbptNode->isLeafNode) 
        minimumKeys = (treeManager->orderType + 1) / 2 - 1;
    else 
        minimumKeys = (treeManager->orderType - 1 + 1) / 2 ;



    if (allbptNode->totalKeys >= minimumKeys) {
        return treeManager->rootNode;
    }

    // int getNeighborIndex(BpTNode *n)
    int neiIndix = getNeighborIndex(allbptNode);

    int kIndex = 0;
    BpTNode * neighbor = NULL;

    if (neiIndix == -1) {
        kIndex = 0;
        neighbor = allbptNode->internalNode->ptr[1];
    } else {
        kIndex = neiIndix;
        neighbor = allbptNode->internalNode->ptr[neiIndix];
    }

    int k = allbptNode->internalNode->keyNode[kIndex]->v.intV;  

    int orderTypeLen = treeManager->orderType;
    if (!allbptNode->isLeafNode) {
        orderTypeLen = orderTypeLen - 1;
    }

    if (neighbor->totalKeys + allbptNode->totalKeys >= orderTypeLen) {

        // redistributeNodes(BpTNode *root, BpTNode *n, BpTNode *neighbor, int neighbor_index, int k_prime_index, int k_prime)
        return redistributeNodes(treeManager->rootNode, allbptNode, neighbor, neiIndix, kIndex, k);
    } else {

        // mergeNodes(BPTreeManagement *treeManager, BpTNode *allbptNode, BpTNode *neighbor, int neighbor_index, int value)
        return mergeNodes(treeManager, allbptNode, neighbor, neiIndix, k);
    }
}

BpTNode * delete(BPTreeManagement * treeManager, Value * key) {
    if (treeManager == NULL) {
        printf("\n Error: The given treeManager struct is Null\n");
        return NULL;
    } if (key == NULL) {
        printf("\n Error: The given key Value struct is Null\n");
        return NULL;
    } 

    // BpTNode *findLeaf(BpTNode *root, Value *key)
    BpTNode * searchedLeafNode = findLeaf(treeManager->rootNode, key);
    
    // findRecord(BpTNode *root, Value *key)
    BpTNode * searchedRecord = findRecord(treeManager->rootNode, key);
    
    if (searchedRecord == NULL) {
        printf("Error: The searched record is not found (or) not available in the B+Tree\n");
        return treeManager->rootNode; 
    } if (searchedLeafNode == NULL) {
        printf("Error: The searched leafNode is not found (or) not available in the B+Tree\n");
        return treeManager->rootNode; 
    }

    // BpTNode *deleteEntry(BPTreeManagement *treeManager, BpTNode *allbptNode, Value *key, void *pointer)
    BpTNode * tempBTNode = deleteEntry(treeManager, searchedLeafNode, key, searchedRecord);
    treeManager->rootNode = tempBTNode;  

    if (searchedRecord) {
        searchedRecord = NULL;
        free(searchedRecord);
    }

    return tempBTNode; 
}

BpTNode * redistributeNodes(BpTNode * mainBPTree, BpTNode * allBPTree, BpTNode * neighbor, int neighbor_index, int k_prime_index, int k_prime) {
    if (mainBPTree == NULL) {
        printf("\n Error: The given mainBPTree struct is Null\n");
        return NULL;
    } if (allBPTree == NULL) {
        printf("\n Error: The given allBPTree struct is Null\n");
        return NULL;
    } if (neighbor == NULL) {
        printf("\n Error: The given neighbor struct is Null\n");
        return NULL;
    } 

    int i = 0;
    if (neighbor_index != -1) {
        if (!allBPTree->isLeafNode) {
            i = allBPTree->totalKeys;
            while (i > 0) {
                allBPTree->keyNode [i] = allBPTree->keyNode [i - 1];
                allBPTree->ptr[i + 1] = allBPTree->ptr[i];
                i--;
            }

            allBPTree->ptr[1] = allBPTree->ptr[0];
            allBPTree->ptr[0] = neighbor->ptr[neighbor->totalKeys];

            // Value *createIntValue(int num)
            allBPTree->keyNode [0] = createIntValue(k_prime);

            allBPTree->internalNode->keyNode [k_prime_index] = neighbor->keyNode [neighbor->totalKeys - 1];
        } else {
            i = allBPTree->totalKeys;
            while ( i > 0) {
                allBPTree->keyNode [i] = allBPTree->keyNode [i - 1];
                allBPTree->ptr[i] = allBPTree->ptr[i - 1];
                i--;
            }
            allBPTree->ptr[0] = neighbor->ptr[neighbor->totalKeys - 1];
            allBPTree->keyNode [0] = neighbor->keyNode [neighbor->totalKeys - 1];
            allBPTree->internalNode->keyNode [k_prime_index] = allBPTree->keyNode [0];
        }
        neighbor->ptr[neighbor->totalKeys] = NULL;
    } else {
        allBPTree->keyNode [allBPTree->totalKeys] = createIntValue(k_prime);
        allBPTree->ptr[allBPTree->totalKeys + 1] = neighbor->ptr[0];
        allBPTree->internalNode->keyNode [k_prime_index] = neighbor->keyNode [0];
        i = 0;
        while (i < (neighbor->totalKeys - 1)) {
            neighbor->ptr[i] = neighbor->ptr[i + 1];
            neighbor->keyNode [i] = neighbor->keyNode [i + 1];
            i++;
        }
        neighbor->ptr[neighbor->totalKeys - 1] = neighbor->ptr[neighbor->totalKeys];
    }

    neighbor->totalKeys--;
    allBPTree->totalKeys++;

    return mainBPTree;
}

void enqueue_helper(BpTNode * current, BpTNode * tempNode) {
    if (current->next == NULL) {
        current->next = tempNode;
    } else {
        // enqueue_helper(BpTNode *current, BpTNode *tempNode)
        enqueue_helper(current->next, tempNode);
    }
}

void enqueue(BPTreeManagement * treeManager, BpTNode * tempNode) {
    tempNode->next = NULL; 
    if ( treeManager->queueNode != NULL) {

        // void enqueue_helper(BpTNode *current, BpTNode *tempNode)
        enqueue_helper(treeManager->queueNode, tempNode);
    }
    else 
        treeManager->queueNode = tempNode;
}

BpTNode * dequeue_helper(BPTreeManagement * treeManager) {
    if (treeManager == NULL) {
        printf("\n Error: The given treeManager struct is Null\n");
        return NULL;
    } 

    BpTNode * tempNode = treeManager->queueNode;
    if (tempNode == NULL) {
        printf("\n Error: The given treeManager->queueNode variable is NULL. \n");
        return NULL;
    } 

    treeManager->queueNode = tempNode->next;
    tempNode->next = NULL;
    return tempNode;
}

BpTNode * dequeue(BPTreeManagement * treeManager) {
    if (treeManager == NULL) {
        printf("\n Error: The given treeManager struct is Null\n");
        return NULL;
    } 
    if (treeManager->queueNode == NULL) 
        return NULL;

        // BpTNode *dequeue_helper(BPTreeManagement *treeManager)
        return dequeue_helper(treeManager); 
}

int pathForRoot(BpTNode * root, BpTNode * child) {
    if (child == NULL) {
        printf("\n The child BpTNode is NULL.\n");
        return -1;
    }

    if (root == child) 
        return 0;
    return (1 + pathForRoot(root, child->internalNode));
}

bool isLess(Value * K1, Value * K2) {
    if (K1 == NULL) {
        printf("\nError: The K1 is NULL. \n");
        return FALSE; 
    } if (K2 == NULL) {
        printf("\nError: The K2 is NULL. \n");
        return FALSE; 
    }

    switch (K1->dt) {
        case DT_INT:
            if (K1->v.intV < K2->v.intV)
                return TRUE;
            else 
                return FALSE;        
        case DT_FLOAT:
            if (K1->v.floatV < K2->v.floatV)
                return TRUE;
            else 
                return FALSE;
        case DT_STRING:
            if (strcmp(K1->v.stringV, K2->v.stringV) == -1)
                return TRUE;
            else 
                return FALSE;
        case DT_BOOL:
            return FALSE;
    }
    return FALSE;
}

bool isGreater(Value * key1, Value * key2) {
    if (key1 == NULL) {
        printf("\nError: The key1 is NULL. \n");
        return FALSE; 
    } if (key2 == NULL) {
        printf("\nError: The key2 is NULL. \n");
        return FALSE; 
    }

    if (key1->dt != key2->dt) {
        printf("\n In isGreater() .The both datatypes are not same. \n");
        return FALSE;  
    }

    switch (key1->dt) {
        case DT_INT:
            if (key1->v.intV > key2->v.intV) 
                return TRUE;
            else 
                return FALSE;
        
        case DT_FLOAT:
            if (key1->v.floatV > key2->v.floatV) 
                return TRUE;
            else 
                return FALSE;

        case DT_STRING:
            if (strcmp(key1->v.stringV, key2->v.stringV) > 0) 
                return TRUE;
            else 
                return FALSE;
        
        case DT_BOOL:
            return FALSE;

    }
    return FALSE;
}

bool isEqual(Value * key1, Value * key2) {
    if (key1 == NULL) {
        printf("\nError: The key1 is NULL. \n");
        return FALSE; 
    } if (key2 == NULL) {
        printf("\nError: The key2 is NULL. \n");
        return FALSE; 
    }

    if (key1->dt != key2->dt) {
        printf("\n In isEqual(). The both datatypes are not same. \n");
        return FALSE;  
    }
    
    switch (key1->dt) {
        case DT_INT:
            if (key1->v.intV == key2->v.intV) 
                return TRUE;
            else 
                return FALSE;
        
        case DT_FLOAT:
            if (key1->v.floatV == key2->v.floatV) 
                return TRUE;
            else 
                return FALSE;

        case DT_STRING:
            if (strcmp(key1->v.stringV, key2->v.stringV) == 0) 
                return TRUE;
            else 
                return FALSE;
        
        case DT_BOOL:
            if (key1->v.boolV == key1->v.boolV) 
                return TRUE;
            else 
                return FALSE;

    }
    return FALSE;
}
