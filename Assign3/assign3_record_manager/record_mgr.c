#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

const int MAXIMUM_PAGES = 100;
const int SIZE_OF_ATTRIBUTE = 15;

int findFreeSlot(char *data, int recordSize);
RC attributeOffset (Schema *schema, int attributeNumber, int *output) ;

RC initRecordManager (void *mgmtData);
RC shutdownRecordManager ();
RC openTable (RM_TableData *rel, char *name);
RC createTable (char *name, Schema *schema);
int getNumTuples (RM_TableData *rel);
RC deleteTable (char *name);
RC closeTable (RM_TableData *rel);

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record);
RC deleteRecord (RM_TableData *rel, RID id);
RC updateRecord (RM_TableData *rel, Record *record);
RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
RC next (RM_ScanHandle *scan, Record *record);
RC closeScan (RM_ScanHandle *scan);

// dealing with schemas
int getRecordSize (Schema *schema);
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
RC freeSchema (Schema *schema);

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema);
RC freeRecord (Record *record);
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);

typedef struct RecordM {
	BM_BufferPool buffer;
	BM_PageHandle pHandler;
	RID recordId;
	Expr *constraint;
	int tupleCount;
	int freeCount;
	int scanCount;
} RecordManagement;

RecordManagement *manager;

/* ============================== table and manager ========================== */

RC initRecordManager (void *mgmtData) {
	initStorageManager();
	printf("\nInitialization of storage manager is done here\n");
	return RC_OK;
}

RC shutdownRecordManager () {
	manager = NULL;
	free(manager);
	printf("\nShutting down storage manager is done here\n");
	return RC_OK;
}

RC createTable (char *name, Schema *schema) {
	int output;
	char data[PAGE_SIZE];
	char *pHandler = data;
	int i = 0;
	SM_FileHandle fileHandle;

	manager = (RecordManagement *) malloc(sizeof(RecordManagement));

	// initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
	initBufferPool(&manager->buffer, name, MAXIMUM_PAGES, RS_LRU, NULL);

	*(int*)pHandler = 0;
	pHandler = pHandler + sizeof(int);

	*(int*)pHandler = 1;
	pHandler = pHandler + sizeof(int);

	*(int*)pHandler = schema->numAttr;
	pHandler = pHandler + sizeof(int);

	*(int*)pHandler = schema->keySize;
	pHandler = pHandler + sizeof(int);

	while (i < schema->numAttr) {
		strncpy(pHandler, schema->attrNames[i], SIZE_OF_ATTRIBUTE);
		pHandler = pHandler + SIZE_OF_ATTRIBUTE;

		*(int*)pHandler = (int)schema->dataTypes[i];
		pHandler = pHandler + sizeof(int);

		*(int*)pHandler = (int) schema->typeLength[i];
		pHandler = pHandler + sizeof(int);

		i++;
	}

	// createPageFile (char *fileName)
	output = createPageFile(name);
	if (output != RC_OK)
		return output;

	// RC openPageFile (char *fileName, SM_FileHandle *fHandle)
	output = openPageFile(name, &fileHandle);
	if(output != RC_OK)
		return output;

	// writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
	output = writeBlock(0, &fileHandle, data);
	if(output != RC_OK)
		return output;

	// RC closePageFile (SM_FileHandle *fHandle)
	output = closePageFile(&fileHandle);
	if(output != RC_OK)
		return output;

	return RC_OK;
}

RC openTable (RM_TableData *rel, char *name) {
	int count, i = 0;
	Schema *schema;
	SM_PageHandle pHandler;
	
	schema = (Schema*) malloc(sizeof(Schema));

	rel->name = name;
	rel->mgmtData = manager;

	// pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
	pinPage(&manager->buffer, &manager->pHandler, 0);

	pHandler = (char*) manager->pHandler.data;

	manager->tupleCount = *(int*)pHandler;
	pHandler = pHandler + sizeof(int);

	manager->freeCount= *(int*) pHandler;
    pHandler = pHandler + sizeof(int);

    count = *(int*)pHandler;
	pHandler = pHandler + sizeof(int);

	schema->attrNames = (char**) malloc(sizeof(char*) *count);
	schema->typeLength = (int*) malloc(sizeof(int) *count);
	schema->dataTypes = (DataType*) malloc(sizeof(DataType) *count);
	schema->numAttr = count;
	rel->schema = schema;

	i=0;
	while (i < count) {
		schema->attrNames[i]= (char*) malloc(SIZE_OF_ATTRIBUTE);
		i++;
	}

	i=0;
	while (i<schema->numAttr) {
		strncpy(schema->attrNames[i], pHandler, SIZE_OF_ATTRIBUTE);
		pHandler = pHandler + SIZE_OF_ATTRIBUTE;

		schema->dataTypes[i]= *(int*) pHandler;
		pHandler = pHandler + sizeof(int);

		schema->typeLength[i]= *(int*)pHandler;
		pHandler = pHandler + sizeof(int);
		
		i++;
	}

	// rel->schema = schema;

	// RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
	unpinPage(&manager->buffer, &manager->pHandler);

	// RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
	forcePage(&manager->buffer, &manager->pHandler);

	return RC_OK;
}

RC closeTable (RM_TableData *rel) {
	RecordManagement * recordManagement = rel->mgmtData;
	shutdownBufferPool(&recordManagement->buffer);
	return RC_OK;
}

RC deleteTable (char *name) {
	destroyPageFile(name);
	return RC_OK;
}

int getNumTuples (RM_TableData *rel) {
	RecordManagement *recordManagement = rel->mgmtData;
	return recordManagement->tupleCount;
}

/* ====================== HANDLING RECORDS IN A TABLE ====================== */

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record) {
	int recordSize = getRecordSize(rel->schema);
	char *dataPtr, *inSlotPtr;
	RecordManagement *recordManagement = rel->mgmtData;
	RID *rids = &record->id;

	rids->page = recordManagement->freeCount;
	
	// pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
	pinPage(&recordManagement->buffer, &recordManagement->pHandler, rids->page);

	dataPtr = recordManagement->pHandler.data;
	rids->slot = findFreeSlot(dataPtr, recordSize);

	while(rids->slot == -1) {
		unpinPage(&recordManagement->buffer, &recordManagement->pHandler);
		rids->page++;

		pinPage(&recordManagement->buffer, &recordManagement->pHandler, rids->page);
		dataPtr = recordManagement->pHandler.data;

		rids->slot = findFreeSlot(dataPtr, recordSize);
	}

	inSlotPtr = dataPtr;

	// RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
	markDirty(&recordManagement->buffer, &recordManagement->pHandler);

	inSlotPtr = inSlotPtr + (rids->slot * recordSize);
	*inSlotPtr = '+';

	memcpy(++inSlotPtr, (record->data + 1), (recordSize - 1));

	// RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
	unpinPage(&recordManagement->buffer, &recordManagement->pHandler);

	recordManagement->tupleCount++;

	// pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
	pinPage(&recordManagement->buffer, &recordManagement->pHandler, 0);

	return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id) {

	int recSize = getRecordSize(rel->schema);
	RecordManagement * recordManagement = rel->mgmtData;

	// pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
	pinPage(&recordManagement->buffer, &recordManagement->pHandler, id.page);

	recordManagement->freeCount = id.page;

	char *dataPtr = recordManagement->pHandler.data;
	dataPtr = dataPtr + (id.slot * recSize);
	*dataPtr = '-';

	// RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
	markDirty(&recordManagement->buffer, &recordManagement->pHandler);

	// RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
	unpinPage(&recordManagement->buffer, &recordManagement->pHandler);

	return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record) {
	char *dataPtr;
	RecordManagement *recordManagement = rel->mgmtData;
	int recSize = getRecordSize(rel->schema);
	RID rids = record->id;

	// pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
	pinPage(&recordManagement->buffer, &recordManagement->pHandler, record->id.page);

	dataPtr = recordManagement->pHandler.data;
	dataPtr = dataPtr + (rids.slot * recSize);
	*dataPtr = '+';

	memcpy(++dataPtr, (record->data + 1), (recSize - 1));

	// RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
	markDirty(&recordManagement->buffer, &recordManagement->pHandler);

	// RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
	unpinPage(&recordManagement->buffer, &recordManagement->pHandler);

	return RC_OK;
}

extern RC getRecord (RM_TableData *rel, RID id, Record *record) {
	int recSize = getRecordSize(rel->schema);
	RecordManagement *recordManagement = rel->mgmtData;

	// pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
	pinPage(&recordManagement->buffer, &recordManagement->pHandler, id.page);

	char *actualData = recordManagement->pHandler.data;
	actualData = actualData + (id.slot * recSize);

	if(*actualData != '+') {
		return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
	} else {
		record->id = id;

		char *dataPtr = record->data;
		memcpy(++dataPtr, (actualData + 1), (recSize - 1));
	}

	// Unpin the page after the record is retrieved since the page is no longer required to be in memory
	unpinPage(&recordManagement->buffer, &recordManagement->pHandler);

	return RC_OK;
}

/* ======================= SCANS ===================== */

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {

	if(cond == NULL) {
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	RecordManagement * scanManagement;
	RecordManagement * tableManagement;

	// RC openTable(RM_TableData *rel, char *name)
	openTable(rel, "TableScaning");

	scanManagement = (RecordManagement *) malloc (sizeof(RecordManagement));
	// scan->mgmtData = scanManagement;

	scanManagement->constraint = cond;
	scanManagement->recordId.page = 1;
	scanManagement->scanCount = 0;
	scanManagement->recordId.slot = 0;
	scanManagement->tupleCount = SIZE_OF_ATTRIBUTE;

	tableManagement = rel->mgmtData;

	scan->rel = rel;
	scan->mgmtData = scanManagement; 

	return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record) {
	RecordManagement *scanManager = scan->mgmtData;

	// Checking if scan condition (test expression) is present
	if (scanManager->constraint == NULL) {
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	RecordManagement *tableManager = scan->rel->mgmtData;
    Schema *schema = scan->rel->schema;
	char *actualData;
	Value *result = (Value *) malloc(sizeof(Value));
	int scanCount = scanManager->scanCount;
	int tuplesCount = tableManager->tupleCount;
	int recordSize = getRecordSize(schema);
	int totalSlots = PAGE_SIZE / recordSize;
	char *dataPointer = record->data;

	if (tuplesCount == 0)
		return RC_RM_NO_MORE_TUPLES;

	while(scanCount <= tuplesCount) {

		if (scanCount > 0) {
			scanManager->recordId.slot++;

			if(scanManager->recordId.slot >= totalSlots) {
				scanManager->recordId.slot = 0;
				scanManager->recordId.page++;
			}
		} else {
			scanManager->recordId.page = 1;
			scanManager->recordId.slot = 0;
		}

		// pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
		pinPage(&tableManager->buffer, &scanManager->pHandler, scanManager->recordId.page);

		record->id.page = scanManager->recordId.page;
		record->id.slot = scanManager->recordId.slot;

		actualData = scanManager->pHandler.data;
		actualData = actualData + (scanManager->recordId.slot * recordSize);

		*dataPointer = '-';

		scanManager->scanCount++;
		scanCount++;

		memcpy(++dataPointer, (actualData + 1), (recordSize - 1));

		// evalExpr(Record *record, Schema *schema, Expr *expr, Value **result)
		evalExpr(record, schema, scanManager->constraint, &result);

		if(result->v.boolV == TRUE) {

			// unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
			unpinPage(&tableManager->buffer, &scanManager->pHandler);
			
			return RC_OK;
		}
	}

	// unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
	unpinPage(&tableManager->buffer, &scanManager->pHandler);

	scanManager->recordId.page = 1;
	scanManager->scanCount = 0;
	scanManager->recordId.slot = 0;

	return RC_RM_NO_MORE_TUPLES;
}

RC closeScan (RM_ScanHandle *scan) {

	if (scan == NULL) {
		printf("The RM_ScanHandle is null");
		exit(-1);
	}

	RecordManagement * recordManagement = scan->rel->mgmtData;
	RecordManagement * scanManagement = scan->mgmtData;

	if(scanManagement->scanCount > 0) {
		scanManagement->recordId.slot = 0;
		scanManagement->scanCount = 0;
		scanManagement->recordId.page = 1;

		// unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
		unpinPage(&recordManagement->buffer, &scanManagement->pHandler);
	}

	scan->mgmtData = NULL;
	free(scan->mgmtData);

	return RC_OK;
}

/* ============================================ */

// dealing with schemas
int getRecordSize (Schema *schema) {
	int recordSize = 0;

	for (int i = 0;i < schema->numAttr;i++) {
		switch (schema->dataTypes[i]) {
			case DT_INT:
					recordSize = recordSize + sizeof(int);
					break;

			case DT_FLOAT:
					recordSize = recordSize + sizeof(float);
					break;

			case DT_BOOL:
					recordSize = recordSize + sizeof(bool);
					break;

			case DT_STRING:
					recordSize = recordSize + schema->typeLength[i];
					break;
		}
	}

	return ++recordSize;
}

Schema * createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
	Schema * schema = (Schema *) malloc (sizeof(Schema));
	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	schema->keyAttrs = keys;
	schema->keySize = keySize;
	schema->numAttr = numAttr;
	schema->typeLength = typeLength;
	return schema;
}

RC freeSchema (Schema *schema) {
	free(schema);
	return RC_OK;
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema) {
	Record *tempRecord = (Record*) malloc(sizeof(Record));
	int rSize = getRecordSize(schema);

	tempRecord->data= (char*) malloc(rSize);
	tempRecord->id.page = -1;
	tempRecord->id.slot = -1;
	*record = tempRecord;

	char *dataPtr = tempRecord->data;
	*dataPtr = '-';
	*(++dataPtr) = '\0';

	return RC_OK;
}

RC freeRecord (Record *record) {
	free(record);
	return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) {

	int offset = 0;
	int intValue = 0; // for DT_INT
	float floatValue = 0.0f; // for DT_FLOAT
	bool boolValue = true; // for DT_BOOL
	int length = 0;  // DT_STRING
	Value *attributeValue = (Value*) malloc(sizeof(Value));
	char *data;

	attributeOffset(schema, attrNum, &offset);

	data = record->data;
	data = data + offset;

	schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];

	switch(schema->dataTypes[attrNum]) {
		case DT_INT:
					intValue = 0;
					memcpy(&intValue, data, sizeof(int));
					attributeValue->v.intV = intValue;
					attributeValue->dt = DT_INT;
					break;

		case DT_FLOAT:
					memcpy(&floatValue, data, sizeof(float));
					attributeValue->v.floatV = floatValue;
					attributeValue->dt = DT_FLOAT;
					break;

		case DT_BOOL:
					memcpy(&boolValue, data, sizeof(bool));
					attributeValue->v.boolV = boolValue;
					attributeValue->dt = DT_BOOL;
					break;

		case DT_STRING:
				length = schema->typeLength[attrNum];
				attributeValue->v.stringV = (char *) malloc(length + 1);
				strncpy(attributeValue->v.stringV, data, length);
				attributeValue->v.stringV[length] = '\0';
				attributeValue->dt = DT_STRING;
				break;

		default:
			printf("No such datatype under the desired datatype \n");
			break;
	}

	*value = attributeValue;
	return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {
	int offset = 0;

	attributeOffset(schema, attrNum, &offset);

	char *data = record->data;

	data = data + offset;

	switch(schema->dataTypes[attrNum]) {
		case DT_INT:
					*(int *) data = value->v.intV;
					data = data + sizeof(int);
					break;

		case DT_FLOAT:
					*(float *) data = value->v.floatV;
					data = data + sizeof(float);
					break;

		case DT_STRING:
					strncpy(data, value->v.stringV, schema->typeLength[attrNum]);
					data = data + schema->typeLength[attrNum];
					break;

		case DT_BOOL:
					*(bool *) data = value->v.boolV;
					data = data + sizeof(bool);
					break;
		default:
			printf("Serializer not defined for the given datatype. \n");
			break;
	}
	return RC_OK;
}

RC attributeOffset (Schema *schema, int attributeNumber, int *output) {

	*output = 1;

	for(int i = 0; i < attributeNumber; i++) {
		switch (schema->dataTypes[i]) {
			case DT_INT:
				*output = *output + sizeof(int);
				break;

			case DT_FLOAT:
				*output = *output + sizeof(float);
				break;

			case DT_STRING:
				*output = *output + schema->typeLength[i];
				break;

			case DT_BOOL:
				*output = *output + sizeof(bool);
				break;
		}
	}
	return RC_OK;
}

int findFreeSlot(char *data, int recordSize) {
	int totalNoOfSlots = PAGE_SIZE / recordSize;

	for (int i = 0; i < totalNoOfSlots; i++)
		if (data[i * recordSize] != '+')
			return i;
	return -1;
}
