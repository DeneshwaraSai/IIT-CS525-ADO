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
	SM_FileHandle fileHandle;
	
	manager = (RecordManagement *) malloc(sizeof(RecordManagement));

	initBufferPool(&manager->buffer, name, MAXIMUM_PAGES, RS_LRU, NULL);

	*(int*)pHandler = 0; 
	pHandler = pHandler + sizeof(int);
	
	*(int*)pHandler = 1;
	pHandler = pHandler + sizeof(int);

	*(int*)pHandler = schema->numAttr;
	pHandler = pHandler + sizeof(int); 

	*(int*)pHandler = schema->keySize;
	pHandler = pHandler + sizeof(int);
	
	for(int i = 0; i < schema->numAttr; i++) {
		strncpy(pHandler, schema->attrNames[i], SIZE_OF_ATTRIBUTE);
		pHandler = pHandler + SIZE_OF_ATTRIBUTE;

		*(int*)pHandler = (int)schema->dataTypes[i];
		pHandler = pHandler + sizeof(int);

		*(int*)pHandler = (int) schema->typeLength[i];
		pHandler = pHandler + sizeof(int);
    }

	if((output = createPageFile(name)) != RC_OK)
		return output;
		
	if((output = openPageFile(name, &fileHandle)) != RC_OK)
		return output;
		
	if((output = writeBlock(0, &fileHandle, data)) != RC_OK)
		return output;
		
	if((output = closePageFile(&fileHandle)) != RC_OK)
		return output;

	return RC_OK;
}

RC openTable (RM_TableData *rel, char *name) {	
	int count, k;
	SM_PageHandle pHandler;    
	 	
	Schema *schema;
	schema = (Schema*) malloc(sizeof(Schema));
	
	rel->mgmtData = manager;
	rel->name = name;
    
	pinPage(&manager->buffer, &manager->pHandler, 0);
	
	pHandler = (char*) manager->pHandler.data;
	
	manager->tupleCount = *(int*)pHandler;
	pHandler = pHandler + sizeof(int);

	manager->freeCount= *(int*) pHandler;
    pHandler = pHandler + sizeof(int);
	
    count = *(int*)pHandler;
	pHandler = pHandler + sizeof(int);

	schema->numAttr = count;
	schema->attrNames = (char**) malloc(sizeof(char*) *count);
	schema->dataTypes = (DataType*) malloc(sizeof(DataType) *count);
	schema->typeLength = (int*) malloc(sizeof(int) *count);

	for(k = 0; k < count; k++)
		schema->attrNames[k]= (char*) malloc(SIZE_OF_ATTRIBUTE);
      
	for(k = 0; k < schema->numAttr; k++) {
		strncpy(schema->attrNames[k], pHandler, SIZE_OF_ATTRIBUTE);
		pHandler = pHandler + SIZE_OF_ATTRIBUTE;
	   
		schema->dataTypes[k]= *(int*) pHandler;
		pHandler = pHandler + sizeof(int);

		schema->typeLength[k]= *(int*)pHandler;
		pHandler = pHandler + sizeof(int);
	}
	
	rel->schema = schema;	

	// RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
	unpinPage(&manager->buffer, &manager->pHandler);

	// RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
	forcePage(&manager->buffer, &manager->pHandler);

	return RC_OK;
}  

RC closeTable (RM_TableData *rel) {
	RecordManagement *recordManagement = rel->mgmtData;
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
	char *data, *inSlotPtr;
	RecordManagement *recordManagement = rel->mgmtData;	
	RID *recordID = &record->id; 
	int recordSize = getRecordSize(rel->schema);
	
	recordID->page = recordManagement->freeCount;
	pinPage(&recordManagement->buffer, &recordManagement->pHandler, recordID->page);
	
	data = recordManagement->pHandler.data;
	recordID->slot = findFreeSlot(data, recordSize);

	while(recordID->slot == -1) {
		unpinPage(&recordManagement->buffer, &recordManagement->pHandler);	
		recordID->page++;
		
		pinPage(&recordManagement->buffer, &recordManagement->pHandler, recordID->page);
		data = recordManagement->pHandler.data;

		recordID->slot = findFreeSlot(data, recordSize);
	}
	
	inSlotPtr = data;
	
	markDirty(&recordManagement->buffer, &recordManagement->pHandler);
	
	inSlotPtr = inSlotPtr + (recordID->slot * recordSize);
	*inSlotPtr = '+';

	memcpy(++inSlotPtr, record->data + 1, recordSize - 1);

	unpinPage(&recordManagement->buffer, &recordManagement->pHandler);
	recordManagement->tupleCount++;
	pinPage(&recordManagement->buffer, &recordManagement->pHandler, 0);

	return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id) {
	
	RecordManagement * recordManagement = rel->mgmtData;

	pinPage(&recordManagement->buffer, &recordManagement->pHandler, id.page);

	recordManagement->freeCount = id.page;

	char *data = recordManagement->pHandler.data;
	int recSize = getRecordSize(rel->schema);

	data = data + (id.slot * recSize);
	*data = '-';

	// RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
	markDirty(&recordManagement->buffer, &recordManagement->pHandler);

	// RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
	unpinPage(&recordManagement->buffer, &recordManagement->pHandler);
	
	return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record) {

	char *data;
	RecordManagement *recordManagement = rel->mgmtData;
	int recSize = getRecordSize(rel->schema);
	RID id = record->id;

	pinPage(&recordManagement->buffer, &recordManagement->pHandler, record->id.page);

	data = recordManagement->pHandler.data;
	data = data + (id.slot * recSize);
	*data = '+';

	memcpy(++data, (record->data + 1), (recSize - 1));
	
	// RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
	markDirty(&recordManagement->buffer, &recordManagement->pHandler);
	
	// RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
	unpinPage(&recordManagement->buffer, &recordManagement->pHandler);

	return RC_OK;
}


extern RC getRecord (RM_TableData *rel, RID id, Record *record) {
	RecordManagement *recordManagement = rel->mgmtData;
	
	pinPage(&recordManagement->buffer, &recordManagement->pHandler, id.page);

	int recSize = getRecordSize(rel->schema);
	char *actualData = recordManagement->pHandler.data;
	actualData = actualData + (id.slot * recSize);
	
	if(*actualData != '+') {
		return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
	} else {
		record->id = id;

		char *data = record->data;
		memcpy(++data, (actualData + 1), (recSize - 1));
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
	scan->mgmtData = scanManagement;

	scanManagement->recordId.page = 1;
	scanManagement->recordId.slot = 0;
	scanManagement->scanCount = 0;

	scanManagement->constraint = cond;

	tableManagement = rel->mgmtData;
	scanManagement->tupleCount = SIZE_OF_ATTRIBUTE;
	scan->rel = rel;
	
	return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record) {
	RecordManagement *scanManager = scan->mgmtData;
	RecordManagement *tableManager = scan->rel->mgmtData;
    Schema *schema = scan->rel->schema;
	
	// Checking if scan condition (test expression) is present
	if (scanManager->constraint == NULL) {
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	Value *result = (Value *) malloc(sizeof(Value));
   
	char *data;
   	
	int recordSize = getRecordSize(schema);
	int totalSlots = PAGE_SIZE / recordSize;
	int scanCount = scanManager->scanCount;
	int tuplesCount = tableManager->tupleCount;

	if (tuplesCount == 0)
		return RC_RM_NO_MORE_TUPLES;

	while(scanCount <= tuplesCount) {  
		if (scanCount <= 0) {
			scanManager->recordId.page = 1;
			scanManager->recordId.slot = 0;
		} else {
			scanManager->recordId.slot++;

			if(scanManager->recordId.slot >= totalSlots) {
				scanManager->recordId.slot = 0;
				scanManager->recordId.page++;
			}
		}

		pinPage(&tableManager->buffer, &scanManager->pHandler, scanManager->recordId.page);
			
		data = scanManager->pHandler.data;

		data = data + (scanManager->recordId.slot * recordSize);
		
		record->id.page = scanManager->recordId.page;
		record->id.slot = scanManager->recordId.slot;

		char *dataPointer = record->data;

		*dataPointer = '-';
		
		memcpy(++dataPointer, data + 1, recordSize - 1);

		scanManager->scanCount++;
		scanCount++;

		evalExpr(record, schema, scanManager->constraint, &result); 

		if(result->v.boolV == TRUE) {
			unpinPage(&tableManager->buffer, &scanManager->pHandler);
			return RC_OK;
		}
	}
	
	unpinPage(&tableManager->buffer, &scanManager->pHandler);
	
	scanManager->recordId.page = 1;
	scanManager->recordId.slot = 0;
	scanManager->scanCount = 0;
	
	return RC_RM_NO_MORE_TUPLES;
}

RC closeScan (RM_ScanHandle *scan) {

	RecordManagement * recordManagement = scan->rel->mgmtData;
	RecordManagement * scanManagement = scan->mgmtData;

	if(scanManagement->scanCount > 0) {
		unpinPage(&recordManagement->buffer, &scanManagement->pHandler);

		scanManagement->scanCount = 0;
		scanManagement->recordId.page = 1;
		scanManagement->recordId.slot = 0;
	}

	scan->mgmtData = NULL;
	free(scan->mgmtData);

	return RC_OK;
}

/* ============================================ */

// dealing with schemas
int getRecordSize (Schema *schema) {
	int size = 0;
	
	for (int i = 0;i < schema->numAttr;i++) {
		switch (schema->dataTypes[i]) {
			case DT_INT: 
					size = size + sizeof(int);
					break;

			case DT_FLOAT: 
					size = size + sizeof(float);
					break;

			case DT_BOOL: 
					size = size + sizeof(bool);	
					break;

			case DT_STRING: 
					size = size + schema->typeLength[i]; 
					break;
		}
	}

	return ++size;
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
	tempRecord->id.page = tempRecord->id.slot = -1;

	char *data = tempRecord->data;
	*data = '-';
	*(++data) = '\0';

	*record = tempRecord;

	return RC_OK;
}

RC freeRecord (Record *record) {
	free(record);
	return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) {
	
	int offset = 0;
	int intValue = 0; // for DT_INT
	float floatValue; // for DT_FLOAT
	bool boolValue; // for DT_BOOL
	int length;  // DT_STRING
	attributeOffset(schema, attrNum, &offset);

	Value *attributeValue = (Value*) malloc(sizeof(Value));

	char *data = record->data;
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
