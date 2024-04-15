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



/* ==================================== */



// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// #include "record_mgr.h"
// #include "buffer_mgr.h"
// #include "storage_mgr.h"

// // This is custom data structure defined for making the use of Record Manager.
// typedef struct RecordManager
// {
// 	// Buffer Manager's PageHandle for using Buffer Manager to access Page files
// 	BM_PageHandle pageHandle;	// Buffer Manager PageHandle 
// 	// Buffer Manager's Buffer Pool for using Buffer Manager	
// 	BM_BufferPool bufferPool;
// 	// Record ID	
// 	RID recordID;
// 	// This variable defines the condition for scanning the records in the table
// 	Expr *condition;
// 	// This variable stores the total number of tuples in the table
// 	int tuplesCount;
// 	// This variable stores the location of first free page which has empty slots in table
// 	int freePage;
// 	// This variable stores the count of the number of records scanned
// 	int scanCount;
// } RecordManager;

// const int MAX_NUMBER_OF_PAGES = 100;
// const int ATTRIBUTE_SIZE = 15; // Size of the name of the attribute

// RecordManager *recordManager;

// // ******** CUSTOM FUNCTIONS ******** //

// // This function returns a free slot within a page
// int findFreeSlot(char *data, int recordSize)
// {
// 	int i, totalSlots = PAGE_SIZE / recordSize; 

// 	for (i = 0; i < totalSlots; i++)
// 		if (data[i * recordSize] != '+')
// 			return i;
// 	return -1;
// }


// // ******** TABLE AND RECORD MANAGER FUNCTIONS ******** //

// // This function initializes the Record Manager
// extern RC initRecordManager (void *mgmtData)
// {
// 	// Initiliazing Storage Manager
// 	initStorageManager();
// 	return RC_OK;
// }

// // This functions shuts down the Record Manager
// extern RC shutdownRecordManager ()
// {
// 	recordManager = NULL;
// 	free(recordManager);
// 	return RC_OK;
// }

// // This function creates a TABLE with table name "name" having schema specified by "schema"
// extern RC createTable (char *name, Schema *schema)
// {
// 	// Allocating memory space to the record manager custom data structure
// 	recordManager = (RecordManager*) malloc(sizeof(RecordManager));

// 	// Initalizing the Buffer Pool using LFU page replacement policy
// 	initBufferPool(&recordManager->bufferPool, name, MAX_NUMBER_OF_PAGES, RS_LRU, NULL);

// 	char data[PAGE_SIZE];
// 	char *pageHandle = data;
	 
// 	int result, k;

// 	// Setting number of tuples to 0
// 	*(int*)pageHandle = 0; 

// 	// Incrementing pointer by sizeof(int) because 0 is an integer
// 	pageHandle = pageHandle + sizeof(int);
	
// 	// Setting first page to 1 since 0th page if for schema and other meta data
// 	*(int*)pageHandle = 1;

// 	// Incrementing pointer by sizeof(int) because 1 is an integer
// 	pageHandle = pageHandle + sizeof(int);

// 	// Setting the number of attributes
// 	*(int*)pageHandle = schema->numAttr;

// 	// Incrementing pointer by sizeof(int) because number of attributes is an integer
// 	pageHandle = pageHandle + sizeof(int); 

// 	// Setting the Key Size of the attributes
// 	*(int*)pageHandle = schema->keySize;

// 	// Incrementing pointer by sizeof(int) because Key Size of attributes is an integer
// 	pageHandle = pageHandle + sizeof(int);
	
// 	for(k = 0; k < schema->numAttr; k++) {
// 		strncpy(pageHandle, schema->attrNames[k], ATTRIBUTE_SIZE);
// 		pageHandle = pageHandle + ATTRIBUTE_SIZE;

// 		*(int*)pageHandle = (int)schema->dataTypes[k];
// 		pageHandle = pageHandle + sizeof(int);

// 		*(int*)pageHandle = (int) schema->typeLength[k];
// 		pageHandle = pageHandle + sizeof(int);
//     	}

// 	SM_FileHandle fileHandle;
		
// 	// Creating a page file page name as table name using storage manager
// 	if((result = createPageFile(name)) != RC_OK)
// 		return result;
		
// 	// Opening the newly created page
// 	if((result = openPageFile(name, &fileHandle)) != RC_OK)
// 		return result;
		
// 	// Writing the schema to first location of the page file
// 	if((result = writeBlock(0, &fileHandle, data)) != RC_OK)
// 		return result;
		
// 	// Closing the file after writing
// 	if((result = closePageFile(&fileHandle)) != RC_OK)
// 		return result;

// 	return RC_OK;
// }

// // This function opens the table with table name "name"
// extern RC openTable (RM_TableData *rel, char *name)
// {
// 	SM_PageHandle pageHandle;    
	
// 	int attributeCount, k;
	
// 	// Setting table's meta data to our custom record manager meta data structure
// 	rel->mgmtData = recordManager;
// 	// Setting the table's name
// 	rel->name = name;
    
// 	// Pinning a page i.e. putting a page in Buffer Pool using Buffer Manager
// 	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, 0);
	
// 	// Setting the initial pointer (0th location) if the record manager's page data
// 	pageHandle = (char*) recordManager->pageHandle.data;
	
// 	// Retrieving total number of tuples from the page file
// 	recordManager->tuplesCount= *(int*)pageHandle;
// 	pageHandle = pageHandle + sizeof(int);

// 	// Getting free page from the page file
// 	recordManager->freePage= *(int*) pageHandle;
//     pageHandle = pageHandle + sizeof(int);
	
// 	// Getting the number of attributes from the page file
//     	attributeCount = *(int*)pageHandle;
// 	pageHandle = pageHandle + sizeof(int);
 	
// 	Schema *schema;

// 	// Allocating memory space to 'schema'
// 	schema = (Schema*) malloc(sizeof(Schema));
    
// 	// Setting schema's parameters
// 	schema->numAttr = attributeCount;
// 	schema->attrNames = (char**) malloc(sizeof(char*) *attributeCount);
// 	schema->dataTypes = (DataType*) malloc(sizeof(DataType) *attributeCount);
// 	schema->typeLength = (int*) malloc(sizeof(int) *attributeCount);

// 	// Allocate memory space for storing attribute name for each attribute
// 	for(k = 0; k < attributeCount; k++)
// 		schema->attrNames[k]= (char*) malloc(ATTRIBUTE_SIZE);
      
// 	for(k = 0; k < schema->numAttr; k++)
//     	{
// 		// Setting attribute name
// 		strncpy(schema->attrNames[k], pageHandle, ATTRIBUTE_SIZE);
// 		pageHandle = pageHandle + ATTRIBUTE_SIZE;
	   
// 		// Setting data type of attribute
// 		schema->dataTypes[k]= *(int*) pageHandle;
// 		pageHandle = pageHandle + sizeof(int);

// 		// Setting length of datatype (length of STRING) of the attribute
// 		schema->typeLength[k]= *(int*)pageHandle;
// 		pageHandle = pageHandle + sizeof(int);
// 	}
	
// 	// Setting newly created schema to the table's schema
// 	rel->schema = schema;	

// 	// Unpinning the page i.e. removing it from Buffer Pool using BUffer Manager
// 	unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);

// 	// Write the page back to disk using BUffer Manger
// 	forcePage(&recordManager->bufferPool, &recordManager->pageHandle);

// 	return RC_OK;
// }   
  
// // This function closes the table referenced by "rel"
// extern RC closeTable (RM_TableData *rel)
// {
// 	// Storing the Table's meta data
// 	RecordManager *recordManager = rel->mgmtData;
	
// 	// Shutting down Buffer Pool	
// 	shutdownBufferPool(&recordManager->bufferPool);
// 	//rel->mgmtData = NULL;
// 	return RC_OK;
// }

// // This function deletes the table having table name "name"
// extern RC deleteTable (char *name)
// {
// 	// Removing the page file from memory using storage manager
// 	destroyPageFile(name);
// 	return RC_OK;
// }

// // This function returns the number of tuples (records) in the table referenced by "rel"
// extern int getNumTuples (RM_TableData *rel)
// {
// 	// Accessing our data structure's tuplesCount and returning it
// 	RecordManager *recordManager = rel->mgmtData;
// 	return recordManager->tuplesCount;
// }


// // ******** RECORD FUNCTIONS ******** //

// // This function inserts a new record in the table referenced by "rel" and updates the 'record' parameter with the Record ID of he newly inserted record
// extern RC insertRecord (RM_TableData *rel, Record *record)
// {
// 	// Retrieving our meta data stored in the table
// 	RecordManager *recordManager = rel->mgmtData;	
	
// 	// Setting the Record ID for this record
// 	RID *recordID = &record->id; 
	
// 	char *data, *slotPointer;
	
// 	// Getting the size in bytes needed to store on record for the given schema
// 	int recordSize = getRecordSize(rel->schema);
	
// 	// Setting first free page to the current page
// 	recordID->page = recordManager->freePage;

// 	// Pinning page i.e. telling Buffer Manager that we are using this page
// 	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, recordID->page);
	
// 	// Setting the data to initial position of record's data
// 	data = recordManager->pageHandle.data;
	
// 	// Getting a free slot using our custom function
// 	recordID->slot = findFreeSlot(data, recordSize);

// 	while(recordID->slot == -1)
// 	{
// 		// If the pinned page doesn't have a free slot then unpin that page
// 		unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);	
		
// 		// Incrementing page
// 		recordID->page++;
		
// 		// Bring the new page into the BUffer Pool using Buffer Manager
// 		pinPage(&recordManager->bufferPool, &recordManager->pageHandle, recordID->page);
		
// 		// Setting the data to initial position of record's data		
// 		data = recordManager->pageHandle.data;

// 		// Again checking for a free slot using our custom function
// 		recordID->slot = findFreeSlot(data, recordSize);
// 	}
	
// 	slotPointer = data;
	
// 	// Mark page dirty to notify that this page was modified
// 	markDirty(&recordManager->bufferPool, &recordManager->pageHandle);
	
// 	// Calculation slot starting position
// 	slotPointer = slotPointer + (recordID->slot * recordSize);

// 	// Appending '+' as tombstone to indicate this is a new record and should be removed if space is lesss
// 	*slotPointer = '+';

// 	// Copy the record's data to the memory location pointed by slotPointer
// 	memcpy(++slotPointer, record->data + 1, recordSize - 1);

// 	// Unpinning a page i.e. removing a page from the BUffer Pool
// 	unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);
	
// 	// Incrementing count of tuples
// 	recordManager->tuplesCount++;
	
// 	// Pinback the page	
// 	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, 0);

// 	return RC_OK;
// }

// // This function deletes a record having Record ID "id" in the table referenced by "rel"
// extern RC deleteRecord (RM_TableData *rel, RID id)
// {
// 	// Retrieving our meta data stored in the table
// 	RecordManager *recordManager = rel->mgmtData;
	
// 	// Pinning the page which has the record which we want to update
// 	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, id.page);

// 	// Update free page because this page 
// 	recordManager->freePage = id.page;
	
// 	char *data = recordManager->pageHandle.data;

// 	// Getting the size of the record
// 	int recordSize = getRecordSize(rel->schema);

// 	// Setting data pointer to the specific slot of the record
// 	data = data + (id.slot * recordSize);
	
// 	// '-' is used for Tombstone mechanism. It denotes that the record is deleted
// 	*data = '-';
		
// 	// Mark the page dirty because it has been modified
// 	markDirty(&recordManager->bufferPool, &recordManager->pageHandle);

// 	// Unpin the page after the record is retrieved since the page is no longer required to be in memory
// 	unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);

// 	return RC_OK;
// }

// // This function updates a record referenced by "record" in the table referenced by "rel"
// extern RC updateRecord (RM_TableData *rel, Record *record)
// {	
// 	// Retrieving our meta data stored in the table
// 	RecordManager *recordManager = rel->mgmtData;
	
// 	// Pinning the page which has the record which we want to update
// 	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, record->id.page);

// 	char *data;

// 	// Getting the size of the record
// 	int recordSize = getRecordSize(rel->schema);

// 	// Set the Record's ID
// 	RID id = record->id;

// 	// Getting record data's memory location and calculating the start position of the new data
// 	data = recordManager->pageHandle.data;
// 	data = data + (id.slot * recordSize);
	
// 	// '+' is used for Tombstone mechanism. It denotes that the record is not empty
// 	*data = '+';
	
// 	// Copy the new record data to the exisitng record
// 	memcpy(++data, record->data + 1, recordSize - 1 );
	
// 	// Mark the page dirty because it has been modified
// 	markDirty(&recordManager->bufferPool, &recordManager->pageHandle);

// 	// Unpin the page after the record is retrieved since the page is no longer required to be in memory
// 	unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);
	
// 	return RC_OK;	
// }

// // This function retrieves a record having Record ID "id" in the table referenced by "rel".
// // The result record is stored in the location referenced by "record"
// extern RC getRecord (RM_TableData *rel, RID id, Record *record)
// {
// 	// Retrieving our meta data stored in the table
// 	RecordManager *recordManager = rel->mgmtData;
	
// 	// Pinning the page which has the record we want to retreive
// 	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, id.page);

// 	// Getting the size of the record
// 	int recordSize = getRecordSize(rel->schema);
// 	char *dataPointer = recordManager->pageHandle.data;
// 	dataPointer = dataPointer + (id.slot * recordSize);
	
// 	if(*dataPointer != '+')
// 	{
// 		// Return error if no matching record for Record ID 'id' is found in the table
// 		return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
// 	}
// 	else
// 	{
// 		// Setting the Record ID
// 		record->id = id;

// 		// Setting the pointer to data field of 'record' so that we can copy the data of the record
// 		char *data = record->data;

// 		// Copy data using C's function memcpy(...)
// 		memcpy(++data, dataPointer + 1, recordSize - 1);
// 	}

// 	// Unpin the page after the record is retrieved since the page is no longer required to be in memory
// 	unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);

// 	return RC_OK;
// }


// // ******** SCAN FUNCTIONS ******** //

// // This function scans all the records using the condition
// extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
// {
// 	// Checking if scan condition (test expression) is present
// 	if (cond == NULL)
// 	{
// 		return RC_SCAN_CONDITION_NOT_FOUND;
// 	}

// 	// Open the table in memory
// 	openTable(rel, "ScanTable");

//     	RecordManager *scanManager;
// 	RecordManager *tableManager;

// 	// Allocating some memory to the scanManager
//     	scanManager = (RecordManager*) malloc(sizeof(RecordManager));
    	
// 	// Setting the scan's meta data to our meta data
//     	scan->mgmtData = scanManager;
    	
// 	// 1 to start scan from the first page
//     	scanManager->recordID.page = 1;
    	
// 	// 0 to start scan from the first slot	
// 	scanManager->recordID.slot = 0;
	
// 	// 0 because this just initializing the scan. No records have been scanned yet    	
// 	scanManager->scanCount = 0;

// 	// Setting the scan condition
//     	scanManager->condition = cond;
    	
// 	// Setting the our meta data to the table's meta data
//     	tableManager = rel->mgmtData;

// 	// Setting the tuple count
//     	tableManager->tuplesCount = ATTRIBUTE_SIZE;

// 	// Setting the scan's table i.e. the table which has to be scanned using the specified condition
//     	scan->rel = rel;

// 	return RC_OK;
// }

// // This function scans each record in the table and stores the result record (record satisfying the condition)
// // in the location pointed by  'record'.
// extern RC next (RM_ScanHandle *scan, Record *record)
// {
// 	// Initiliazing scan data
// 	RecordManager *scanManager = scan->mgmtData;
// 	RecordManager *tableManager = scan->rel->mgmtData;
//     	Schema *schema = scan->rel->schema;
	
// 	// Checking if scan condition (test expression) is present
// 	if (scanManager->condition == NULL)
// 	{
// 		return RC_SCAN_CONDITION_NOT_FOUND;
// 	}

// 	Value *result = (Value *) malloc(sizeof(Value));
   
// 	char *data;
   	
// 	// Getting record size of the schema
// 	int recordSize = getRecordSize(schema);

// 	// Calculating Total number of slots
// 	int totalSlots = PAGE_SIZE / recordSize;

// 	// Getting Scan Count
// 	int scanCount = scanManager->scanCount;

// 	// Getting tuples count of the table
// 	int tuplesCount = tableManager->tuplesCount;

// 	// Checking if the table contains tuples. If the tables doesn't have tuple, then return respective message code
// 	if (tuplesCount == 0)
// 		return RC_RM_NO_MORE_TUPLES;

// 	// Iterate through the tuples
// 	while(scanCount <= tuplesCount)
// 	{  
// 		// If all the tuples have been scanned, execute this block
// 		if (scanCount <= 0)
// 		{
// 			// printf("INSIDE If scanCount <= 0 \n");
// 			// Set PAGE and SLOT to first position
// 			scanManager->recordID.page = 1;
// 			scanManager->recordID.slot = 0;
// 		}
// 		else
// 		{
// 			// printf("INSIDE Else scanCount <= 0 \n");
// 			scanManager->recordID.slot++;

// 			// If all the slots have been scanned execute this block
// 			if(scanManager->recordID.slot >= totalSlots)
// 			{
// 				scanManager->recordID.slot = 0;
// 				scanManager->recordID.page++;
// 			}
// 		}

// 		// Pinning the page i.e. putting the page in buffer pool
// 		pinPage(&tableManager->bufferPool, &scanManager->pageHandle, scanManager->recordID.page);
			
// 		// Retrieving the data of the page			
// 		data = scanManager->pageHandle.data;

// 		// Calulate the data location from record's slot and record size
// 		data = data + (scanManager->recordID.slot * recordSize);
		
// 		// Set the record's slot and page to scan manager's slot and page
// 		record->id.page = scanManager->recordID.page;
// 		record->id.slot = scanManager->recordID.slot;

// 		// Intialize the record data's first location
// 		char *dataPointer = record->data;

// 		// '-' is used for Tombstone mechanism.
// 		*dataPointer = '-';
		
// 		memcpy(++dataPointer, data + 1, recordSize - 1);

// 		// Increment scan count because we have scanned one record
// 		scanManager->scanCount++;
// 		scanCount++;

// 		// Test the record for the specified condition (test expression)
// 		evalExpr(record, schema, scanManager->condition, &result); 

// 		// v.boolV is TRUE if the record satisfies the condition
// 		if(result->v.boolV == TRUE)
// 		{
// 			// Unpin the page i.e. remove it from the buffer pool.
// 			unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
// 			// Return SUCCESS			
// 			return RC_OK;
// 		}
// 	}
	
// 	// Unpin the page i.e. remove it from the buffer pool.
// 	unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
	
// 	// Reset the Scan Manager's values
// 	scanManager->recordID.page = 1;
// 	scanManager->recordID.slot = 0;
// 	scanManager->scanCount = 0;
	
// 	// None of the tuple satisfy the condition and there are no more tuples to scan
// 	return RC_RM_NO_MORE_TUPLES;
// }

// // This function closes the scan operation.
// extern RC closeScan (RM_ScanHandle *scan)
// {
// 	RecordManager *scanManager = scan->mgmtData;
// 	RecordManager *recordManager = scan->rel->mgmtData;

// 	// Check if scan was incomplete
// 	if(scanManager->scanCount > 0)
// 	{
// 		// Unpin the page i.e. remove it from the buffer pool.
// 		unpinPage(&recordManager->bufferPool, &scanManager->pageHandle);
		
// 		// Reset the Scan Manager's values
// 		scanManager->scanCount = 0;
// 		scanManager->recordID.page = 1;
// 		scanManager->recordID.slot = 0;
// 	}
	
// 	// De-allocate all the memory space allocated to the scans's meta data (our custom structure)
//     	scan->mgmtData = NULL;
//     	free(scan->mgmtData);  
	
// 	return RC_OK;
// }


// // ******** SCHEMA FUNCTIONS ******** //

// // This function returns the record size of the schema referenced by "schema"
// extern int getRecordSize (Schema *schema)
// {
// 	int size = 0, i; // offset set to zero
	
// 	// Iterating through all the attributes in the schema
// 	for(i = 0; i < schema->numAttr; i++)
// 	{
// 		switch(schema->dataTypes[i])
// 		{
// 			// Switch depending on DATA TYPE of the ATTRIBUTE
// 			case DT_STRING:
// 				// If attribute is STRING then size = typeLength (Defined Length of STRING)
// 				size = size + schema->typeLength[i];
// 				break;
// 			case DT_INT:
// 				// If attribute is INTEGER, then add size of INT
// 				size = size + sizeof(int);
// 				break;
// 			case DT_FLOAT:
// 				// If attribite is FLOAT, then add size of FLOAT
// 				size = size + sizeof(float);
// 				break;
// 			case DT_BOOL:
// 				// If attribite is BOOLEAN, then add size of BOOLEAN
// 				size = size + sizeof(bool);
// 				break;
// 		}
// 	}
// 	return ++size;
// }

// // This function creates a new schema
// extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
// {
// 	// Allocate memory space to schema
// 	Schema *schema = (Schema *) malloc(sizeof(Schema));
// 	// Set the Number of Attributes in the new schema	
// 	schema->numAttr = numAttr;
// 	// Set the Attribute Names in the new schema
// 	schema->attrNames = attrNames;
// 	// Set the Data Type of the Attributes in the new schema
// 	schema->dataTypes = dataTypes;
// 	// Set the Type Length of the Attributes i.e. STRING size  in the new schema
// 	schema->typeLength = typeLength;
// 	// Set the Key Size  in the new schema
// 	schema->keySize = keySize;
// 	// Set the Key Attributes  in the new schema
// 	schema->keyAttrs = keys;

// 	return schema; 
// }

// // This function removes a schema from memory and de-allocates all the memory space allocated to the schema.
// extern RC freeSchema (Schema *schema)
// {
// 	// De-allocating memory space occupied by 'schema'
// 	free(schema);
// 	return RC_OK;
// }


// // ******** DEALING WITH RECORDS AND ATTRIBUTE VALUES ******** //

// // This function creates a new record in the schema referenced by "schema"
// extern RC createRecord (Record **record, Schema *schema)
// {
// 	// Allocate some memory space for the new record
// 	Record *newRecord = (Record*) malloc(sizeof(Record));
	
// 	// Retrieve the record size
// 	int recordSize = getRecordSize(schema);

// 	// Allocate some memory space for the data of new record    
// 	newRecord->data= (char*) malloc(recordSize);

// 	// Setting page and slot position. -1 because this is a new record and we don't know anything about the position
// 	newRecord->id.page = newRecord->id.slot = -1;

// 	// Getting the starting position in memory of the record's data
// 	char *dataPointer = newRecord->data;
	
// 	// '-' is used for Tombstone mechanism. We set it to '-' because the record is empty.
// 	*dataPointer = '-';
	
// 	// Append '\0' which means NULL in C to the record after tombstone. ++ because we need to move the position by one before adding NULL
// 	*(++dataPointer) = '\0';

// 	// Set the newly created record to 'record' which passed as argument
// 	*record = newRecord;

// 	return RC_OK;
// }

// // This function sets the offset (in bytes) from initial position to the specified attribute of the record into the 'result' parameter passed through the function
// RC attrOffset (Schema *schema, int attrNum, int *result)
// {
// 	int i;
// 	*result = 1;

// 	// Iterating through all the attributes in the schema
// 	for(i = 0; i < attrNum; i++)
// 	{
// 		// Switch depending on DATA TYPE of the ATTRIBUTE
// 		switch (schema->dataTypes[i])
// 		{
// 			// Switch depending on DATA TYPE of the ATTRIBUTE
// 			case DT_STRING:
// 				// If attribute is STRING then size = typeLength (Defined Length of STRING)
// 				*result = *result + schema->typeLength[i];
// 				break;
// 			case DT_INT:
// 				// If attribute is INTEGER, then add size of INT
// 				*result = *result + sizeof(int);
// 				break;
// 			case DT_FLOAT:
// 				// If attribite is FLOAT, then add size of FLOAT
// 				*result = *result + sizeof(float);
// 				break;
// 			case DT_BOOL:
// 				// If attribite is BOOLEAN, then add size of BOOLEAN
// 				*result = *result + sizeof(bool);
// 				break;
// 		}
// 	}
// 	return RC_OK;
// }

// // This function removes the record from the memory.
// extern RC freeRecord (Record *record)
// {
// 	// De-allocating memory space allocated to record and freeing up that space
// 	free(record);
// 	return RC_OK;
// }

// // This function retrieves an attribute from the given record in the specified schema
// extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
// {
// 	int offset = 0;

// 	// Getting the ofset value of attributes depending on the attribute number
// 	attrOffset(schema, attrNum, &offset);

// 	// Allocating memory space for the Value data structure where the attribute values will be stored
// 	Value *attribute = (Value*) malloc(sizeof(Value));

// 	// Getting the starting position of record's data in memory
// 	char *dataPointer = record->data;
	
// 	// Adding offset to the starting position
// 	dataPointer = dataPointer + offset;

// 	// If attrNum = 1
// 	schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];
	
// 	// Retrieve attribute's value depending on attribute's data type
// 	switch(schema->dataTypes[attrNum])
// 	{
// 		case DT_STRING:
// 		{
//      			// Getting attribute value from an attribute of type STRING
// 			int length = schema->typeLength[attrNum];
// 			// Allocate space for string hving size - 'length'
// 			attribute->v.stringV = (char *) malloc(length + 1);

// 			// Copying string to location pointed by dataPointer and appending '\0' which denotes end of string in C
// 			strncpy(attribute->v.stringV, dataPointer, length);
// 			attribute->v.stringV[length] = '\0';
// 			attribute->dt = DT_STRING;
//       			break;
// 		}

// 		case DT_INT:
// 		{
// 			// Getting attribute value from an attribute of type INTEGER
// 			int value = 0;
// 			memcpy(&value, dataPointer, sizeof(int));
// 			attribute->v.intV = value;
// 			attribute->dt = DT_INT;
//       			break;
// 		}
    
// 		case DT_FLOAT:
// 		{
// 			// Getting attribute value from an attribute of type FLOAT
// 	  		float value;
// 	  		memcpy(&value, dataPointer, sizeof(float));
// 	  		attribute->v.floatV = value;
// 			attribute->dt = DT_FLOAT;
// 			break;
// 		}

// 		case DT_BOOL:
// 		{
// 			// Getting attribute value from an attribute of type BOOLEAN
// 			bool value;
// 			memcpy(&value,dataPointer, sizeof(bool));
// 			attribute->v.boolV = value;
// 			attribute->dt = DT_BOOL;
//       			break;
// 		}

// 		default:
// 			printf("Serializer not defined for the given datatype. \n");
// 			break;
// 	}

// 	*value = attribute;
// 	return RC_OK;
// }

// // This function sets the attribute value in the record in the specified schema
// extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
// {
// 	int offset = 0;

// 	// Getting the ofset value of attributes depending on the attribute number
// 	attrOffset(schema, attrNum, &offset);

// 	// Getting the starting position of record's data in memory
// 	char *dataPointer = record->data;
	
// 	// Adding offset to the starting position
// 	dataPointer = dataPointer + offset;
		
// 	switch(schema->dataTypes[attrNum])
// 	{
// 		case DT_STRING:
// 		{
// 			// Setting attribute value of an attribute of type STRING
// 			// Getting the legeth of the string as defined while creating the schema
// 			int length = schema->typeLength[attrNum];

// 			// Copying attribute's value to the location pointed by record's data (dataPointer)
// 			strncpy(dataPointer, value->v.stringV, length);
// 			dataPointer = dataPointer + schema->typeLength[attrNum];
// 		  	break;
// 		}

// 		case DT_INT:
// 		{
// 			// Setting attribute value of an attribute of type INTEGER
// 			*(int *) dataPointer = value->v.intV;	  
// 			dataPointer = dataPointer + sizeof(int);
// 		  	break;
// 		}
		
// 		case DT_FLOAT:
// 		{
// 			// Setting attribute value of an attribute of type FLOAT
// 			*(float *) dataPointer = value->v.floatV;
// 			dataPointer = dataPointer + sizeof(float);
// 			break;
// 		}
		
// 		case DT_BOOL:
// 		{
// 			// Setting attribute value of an attribute of type STRING
// 			*(bool *) dataPointer = value->v.boolV;
// 			dataPointer = dataPointer + sizeof(bool);
// 			break;
// 		}

// 		default:
// 			printf("Serializer not defined for the given datatype. \n");
// 			break;
// 	}			
// 	return RC_OK;
// }