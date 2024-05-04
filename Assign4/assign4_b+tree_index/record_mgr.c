#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

/* Some constants taht are helpful for the assignment */
const int MAXIMUM_PAGES = 100;
const int SIZE_OF_ATTRIBUTE = 15;

/* custom functions declarations */ 
int findFreeSlot(char *data, int recordSize);
RC attributeOffset (Schema *schema, int attributeNumber, int *output) ;

/* table and manager functions declarations */
RC initRecordManager (void *mgmtData);
RC shutdownRecordManager ();
RC openTable (RM_TableData *rel, char *name);
RC createTable (char *name, Schema *schema);
int getNumTuples (RM_TableData *rel);
RC deleteTable (char *name);
RC closeTable (RM_TableData *rel);

/* handling records in a table functions declarations */
RC insertRecord (RM_TableData *rel, Record *record);
RC deleteRecord (RM_TableData *rel, RID id);
RC updateRecord (RM_TableData *rel, Record *record);
RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans functions declarations 
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
RC next (RM_ScanHandle *scan, Record *record);
RC closeScan (RM_ScanHandle *scan);

/* dealing with schemas functions declarations */
int getRecordSize (Schema *schema);
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
RC freeSchema (Schema *schema);

/* dealing with records and attribute values functions declarations */
RC createRecord (Record **record, Schema *schema);
RC freeRecord (Record *record);
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);

/* The Main Data Structure */
typedef struct RecordM {
	BM_BufferPool buffer;
	BM_PageHandle pHandler;
	RID recordId;
	Expr *constraint;
	int tupleCount;
	int freeCount;
	int scanCount;
} RecordManagement;

RecordManagement * manager;

/* ============================== table and manager ========================== */

/**
 * @author : Deneshwara Sai Ila
 * @details : The function `initRecordManager` initializes the storage manager and prints a message indicating the
 * completion of the initialization process.
 * 
 * @param mgmtData : The `mgmtData` parameter is a pointer to any information that may be needed for managing records.
 * 
 * @return RC_OK
 */
RC initRecordManager (void *mgmtData) {
	initStorageManager();
	printf("\nInitialization of storage manager is done here\n");
	return RC_OK;
}

/**
 * @author : Prudhvi Teja Kari
 * @details : The function `shutdownRecordManager` deallocates memory for the storage manager and prints a message
 * indicating the shutdown process is complete.
 * 
 * @return RC_OK
 */
RC shutdownRecordManager () {
	manager = NULL;
	free(manager);
	printf("\nShutting down storage manager is done here\n");
	return RC_OK;
}

/**
 * @author : Deneshwara Sai Ila
 * @details : The `createTable` function creates a table with the specified name and schema, storing the schema
 * information in a newly created page file.
 * 
 * @param name : The `name` parameter is a character pointer that represents the name of the table being opened. 
 * 
 * @param schema : The `Schema` struct contains information about the attributes of a record, such as data types and type
 * lengths.
 * 
 * @return The function `createTable` is returning an `RC` (Return Code) value. 
 * If block of code execution is successful, it will return `RC_OK`. 
 * else returns the repective error code.
 */
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

/**
 * @author : Prudhvi Teja Kari
 * @details : The function `openTable` reads table information from a buffer manager and initializes the table
 * data structure accordingly.
 * 
 * @param rel : `rel` is a pointer to the `RM_TableData` structure, which contains information about the
 * table such as schema and management data.
 * @param name : The `name` parameter is a character pointer that represents the name of the table being opened. 
 * 
 * @return RC_OK
 */
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

/**
 * @author : Deneshwara Sai Ila
 * @details : The closeTable function closes a table by shutting down the buffer pool.
 * 
 * @param rel : `rel` is a pointer to the `RM_TableData` structure, which contains information about the
 * table such as schema and management data.
 * 
 * @return RC_OK
 */

RC closeTable (RM_TableData *rel) {
	RecordManagement * recordManagement = rel->mgmtData;
	shutdownBufferPool(&recordManagement->buffer);
	return RC_OK;
}

/**
 * @author : Prudhvi Teja Kari
 * @details : The function `deleteTable` deletes a table by destroying its page file.
 * 
 * @param name The `name` parameter in the `deleteTable` function is a pointer to a character array
 * that represents the name of the table to be deleted.
 * 
 * @return returns an `RC` (Return Code) value, specifically `RC_OK`.
 */

RC deleteTable (char *name) {
	destroyPageFile(name);
	return RC_OK;
}

/**
 * @author : Prudhvi Teja Kari
 * @details : The function `getNumTuples` returns the number of tuples in a given table.
 * 
 * @param rel : `rel` is a pointer to the `RM_TableData` structure, which contains information about the
 * table such as schema and management data.
 * 
 * @return returns the number of tuples (records) in the given relation `rel`.
 */

int getNumTuples (RM_TableData *rel) {
	RecordManagement *recordManagement = rel->mgmtData;
	return recordManagement->tupleCount;
}

/* ====================== HANDLING RECORDS IN A TABLE ====================== */


/**
 * @author : Deneshwara Sai Ila
 * @details : The insertRecord function inserts a record into a table by 
 * 1) finding a free available slot in a page, 
 * 2) marking the page as dirty, 
 * 3) copying the record data into the slot, and 
 * 4) updating the tuple count.
 * 
 * @param rel : `rel` is a pointer to the `RM_TableData` structure, which contains information about the
 * table such as schema and management data.
 * @param record : The `record` parameter in the `getRecord` function is a pointer to a `Record` struct
 * where the retrieved record data will be stored. 
 * 
 * @return this function returns an error code of `RC_OK` upon successful completion.
 */
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

/**
 * @author : Prudhvi Teja Kari
 * @details : The deleteRecord function deletes a record from a table by marking it as deleted in the buffer pool.
 * 
 * @param rel : `rel` is a pointer to the `RM_TableData` structure, which contains information about the
 * table such as schema and management data.
 * @param id The `id` parameter represents the Record ID (RID) of the record that needs to be deleted. 
 * It typically consists of two parts: `page` and `slot`. 
 * 
 * @return This function returns an `RC` (Return Code) value, specifically `RC_OK`.
 */
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

/**
 * @author : Prudhvi Teja Kari
 * @details : Updates a record in the specified table by modifying data on disk.
 * 
 * @param rel : `rel` is a pointer to the `RM_TableData` structure, which contains information about the
 * table such as schema and management data.
 * @param record : The `record` parameter in the `getRecord` function is a pointer to a `Record` struct
 * where the retrieved record data will be stored. 
 * 
 * @return RC_OK on success, error code otherwise.
 */
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

/**
 * @author : Deneshwara Sai Ila
 * @details : This method retrieves a record from a table using the provided record ID.
 * 
 * @param rel : `rel` is a pointer to the `RM_TableData` structure, which contains information about the
 * table such as schema and management data.
 * @param id : The `id` parameter in the `getRecord` function represents the unique identifier of a
 * record within a table. 
 * @param record : The `record` parameter in the `getRecord` function is a pointer to a `Record` struct
 * where the retrieved record data will be stored. 
 * 
 * @return RC (Return Code) 
 * 1) If the record is successfully retrieved, it returns RC_OK. 
 * 2) If there is no tuple with the given RID (Record ID), it returns RC_RM_NO_TUPLE_WITH_GIVEN_RID.
 */
RC getRecord (RM_TableData *rel, RID id, Record *record) {
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

 
/**
 * @author : Deneshwara Sai Ila
 * @details : The function `startScan` initializes a scan operation on a table with a given condition.
 * 
 * @param rel : rel is a pointer to the RM_TableData structure, which represents a  
 * name with String datatype, schema with Schema data structure and mgmtData with void type.
 * @param scan The `scan` parameter is a pointer to an `RM_ScanHandle` struct, which is used to manage
 * a scan operation on a table.  
 * 
 * @param cond The `cond` parameter in the `startScan` function represents the condition that needs to
 * be satisfied during the scan operation.  
 * 
 * @return It returns an error code 
 * `RC_SCAN_CONDITION_NOT_FOUND` if the input condition `cond` is NULL. 
 * Otherwise, it will return `RC_OK` indicating that the scan has been successfully started.
 */
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {

	if(cond == NULL) {
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	RecordManagement * scanManagement;
	RecordManagement * tableManagement;

	// RC openTable(RM_TableData *rel, char *name)
	openTable(rel, "TableScaning");

	scanManagement = (RecordManagement *) malloc (sizeof(RecordManagement));

	scanManagement->scanCount = 0;
	scanManagement->constraint = cond;
	scanManagement->recordId.page = 1;
	scanManagement->recordId.slot = 0;
	scanManagement->tupleCount = SIZE_OF_ATTRIBUTE;

	tableManagement = rel->mgmtData;

	scan->rel = rel;
	scan->mgmtData = scanManagement; 

	return RC_OK;
}

/**
 * @author : Prudhvi Teja Kari
 * @details : The function `next` iterates through records in a table based on a scan condition and returns the
 * next record that satisfies the condition.
 * 
 * @param scan : The `scan` parameter contains information about a scan operation in a record manager.
 * @param record : The `record` parameter is a part of Record Struct.
 * 
 * @return The function `RC next` returns  : 
 * 1) `RC_OK` if a matching record is found based on the scan condition,(that too on bool datatype)  
 * 2) `RC_RM_NO_MORE_TUPLES` if there are no more tuples to scan.
 */

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

/**
 * @author : Deneshwara Sai Ila
 * @details : The closeScan function closes a scanned RM_ScanHandle and frees up allocated memory.
 * 
 * @param scan : The `scan` parameter is a pointer to an `RM_ScanHandle` structure, which is used for scanning records.
 * 
 * @return RC_OK
 */

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


/* =========================================================== */


/**
 * @author : Prudhvi Teja Kari
 * @details : The function calculates the size of a record based on the data types and lengths specified in a
 * given schema.
 * 
 * @param schema : The `Schema` struct contains information about the attributes of a record, such as data types and type
 * lengths.
 * 
 * @return the total size of a record in bytes based on the data types and size of datatype specified in the
 * given schema.
 */
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


/**
 * @author : Deneshwara Sai Ila
 * @details : The function `createSchema` dynamically allocates memory for a Schema struct and assigns value to the attributes of schema
 
 * @param numAttr : The `numAttr` parameter represents the number of attributes in the schema.
 * @param attrNames : The `attrNames` parameter is a pointer to an array of strings representing the attribute names in a schema structure.  
 * @param dataTypes : The `dataTypes` parameter is a pointer to an array `dataTypes` 
 * @param typeLength : The `typeLength` parameter is an array that specifies the length of each attribute's data type. 
 * @param keySize The `keySize` parameter represents the size of the key attributes in the schema. 
 * @param keys : The `keys` parameter represents an array of integers that specifies the attribute indices  
 * 
 * @return This function is returning a pointer to a Schema struct that has been dynamically allocated
 * in memory.
 */
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

/**
 * @author : Prudhvi Teja Kari
 * @details : The function `freeSchema` frees the memory allocated for a Schema structure.
 * 
 * @param schema : The `schema` parameter is a pointer to a `Schema` structure that needs to be freed from memory.
 * 
 * @return an RC (Return Code) value, specifically RC_OK.
 */
RC freeSchema (Schema *schema) {
	free(schema);
	return RC_OK;
}

/**
 * @author : Deneshwara Sai Ila
 * @details : The `createRecord` function allocates memory for a new record, initializes its fields.
 * 
 * @param record Record **record is a pointer to a pointer to a Record struct. 
 * @param schema The `schema` parameter in the `createRecord` function is a pointer to a `Schema` struct.
 * 
 * @return The function `createRecord` is returning an `RC` (Return Code) value, specifically `RC_OK`.
 */
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

/**
 * @author : Prudhvi Teja Kari
 * @details : The function `freeRecord` frees the memory allocated for a Record structure.
 * 
 * @param record The `record` parameter is a pointer to a `Record` structure that needs to be freed
 * from memory.
 * 
 * @return an RC (Return Code) value, specifically RC_OK.
 */
RC freeRecord (Record *record) {
	if (record == NULL) {
		return RC_OK;
	}
	free(record);
	return RC_OK;
}

/**
 * @author : Deneshwara Sai Ila
 * @details : Retrieves the attribute value from a given record based on the schema and attribute number.
 *
 * @param record Pointer to the record from which to retrieve the attribute.
 * @param schema Pointer to the schema defining the structure of the record.
 * @param attrNum The number of the attribute to retrieve.
 * @param value Pointer to a pointer where the retrieved attribute value will be stored.
 * @return Returns RC_OK if successful, or an error code if an error occurs.
 */
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) {

	int offset = 0;
	int intValue = 0; // for DT_INT
	float floatValue = 0.0f; // for DT_FLOAT
	bool boolValue = true; // for DT_BOOL
	int length = 0;  // DT_STRING
	Value *attributeValue = (Value*) malloc(sizeof(Value));
	char *data;

	// attributeOffset(Schema *schema, int attributeNumber, int *output)
	attributeOffset(schema, attrNum, &offset);

	data = record->data;
	data = data + offset;

	schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];

	switch(schema->dataTypes[attrNum]) {
		case DT_INT:
					intValue = 0;
					memcpy(&intValue, data, sizeof(int));
					attributeValue->dt = DT_INT;
					attributeValue->v.intV = intValue;
					break;

		case DT_FLOAT:
					memcpy(&floatValue, data, sizeof(float));
					attributeValue->dt = DT_FLOAT;
					attributeValue->v.floatV = floatValue;
					break;

		case DT_BOOL:
					memcpy(&boolValue, data, sizeof(bool));
					attributeValue->dt = DT_BOOL;
					attributeValue->v.boolV = boolValue;
					break;

		case DT_STRING:
				length = schema->typeLength[attrNum];
				attributeValue->dt = DT_STRING;
				attributeValue->v.stringV = (char *) malloc(length + 1);
				strncpy(attributeValue->v.stringV, data, length);
				attributeValue->v.stringV[length] = '\0';
				break;

		default:
			printf("No such datatype under the desired datatype \n");
			break;
	}

	*value = attributeValue;
	return RC_OK;
}

/**
 * @author : Prudhvi Teja Kari
 * @details: function updates the value of the attribute at the specified index in the given record
 * according to the provided schema. It handles different data types including integer, float,
 * string, and boolean.
 *
 * @param record : Pointer to the record whose attribute needs to be set.
 * @param schema : Pointer to the schema defining the structure of the record.
 * @param attrNum : The index of the attribute to be set.
 * @param value : Pointer to the new value to be assigned to the attribute.
 * 
 * @return RC_OK : if the operation is successful then return 0, otherwise an error code.
 */
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {
	int offset = 0;
	char *data;

	// attributeOffset(Schema *schema, int attributeNumber, int *output)
	attributeOffset(schema, attrNum, &offset);

	data = record->data;
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
			exit(0);
	}
	return RC_OK;
}

/* ============================================= MY CUSTOM FUNCTIONS FOR HELPING THE CODE ===================================== */

/**
 * @author : Deneshwara Sai Ila
 * @details: The function attributeOffset calculates the offset of a specific attribute within a schema based on
 * its data type and length.
 * 
 * @param schema The `schema` parameter is a pointer to a structure of type `Schema`.
 * @param attributeNumber The `attributeNumber` parameter represents the number of attributes in a schema.
 * @param output The `output` parameter is a pointer to an integer where the calculated offset value will be stored. 
 * @return The function `attributeOffset` is returning an `RC` (Return Code) value, specifically `RC_OK`.
 */
RC attributeOffset (Schema *schema, int attributeNumber, int *output) {
	int i=0;
	*output = 1;

	while (i < attributeNumber) {

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
		i++;
	}
	return RC_OK;
}

/**
 * @author : Prudhvi Teja Kari
 * @details: The function `findFreeSlot` searches for the first available free slot in a character data array based on a record size.
 * 
 * @param data The `data` parameter is a pointer to the beginning of a memory block where records are
 * stored. 
 * @param recordSize Record size is the size of each record in bytes. It is used to calculate the total
 * number of slots that can fit in a page.
 * 
 * @return The function `findFreeSlot` returns the index of the first free slot in the data array.
 */
int findFreeSlot(char *data, int recordSize) {
	int i = 0, totalNoOfSlots = PAGE_SIZE / recordSize;

	while(i < totalNoOfSlots) {
		if (data[i * recordSize] != '+') {
			return i;
		}
		i++;
	}
	return -1;
}
