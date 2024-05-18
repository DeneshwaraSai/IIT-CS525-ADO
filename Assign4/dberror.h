#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_MEMORY_ALLOCATION_FAIL 5
#define RC_ERROR 400 
#define RC_PINNED_PAGES_IN_BUFFER 500 
#define RC_INVALID_INPUT 20
#define RC_INVALID_HANDLE 21

#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205

#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_ERROR 400  
#define RC_PINNED_PAGES_IN_BUFFER 500  
#define RC_PAGES_STILL_FIXED 501
#define RC_MEMORY_ALLOCATION_FAIL 5
#define RC_INVALID_FILENAME 7
#define RC_ERR 404
#define RC_INVALID_INPUT 20
#define RC_FILE_OPEN_ERROR 20
#define RC_WRITE_OUT_OF_BOUNDS 20
#define RC_END_OF_FILE 20
#define RC_SUCCESS 20
#define RC_WRITE_ERROR 20
#define RC_SEEK_ERROR 20
#define RC_SCAN_CONDITION_NOT_FOUND 25
#define RC_RM_NO_TUPLE_WITH_GIVEN_RID 30
#define RC_MEMORY_ALLOCATION_ERROR 601
#define RC_RECORD_MANAGER_NOT_INIT 602
#define RC_RM_NOT_INITIALIZED 603
#define RC_PAGE_DATA_NOT_FOUND 604
#define RC_BUFFER_POOL_NOT_INIT 605
#define RC_TABLE_DATA_NOT_FOUND 606
#define RC_INVALID_TABLE_NAME 607
#define RC_SCAN_HANDLE_NOT_INIT 608
#define RC_RM_ALREADY_INIT 609
#define RC_RM_NOT_INIT 610
#define RC_STRING_TOO_LONG 611
#define RC_SERIALIZATION_ERROR 612
#define RC_INVALID_DATATYPE 613
#define RC_INVALID_ATTR_NUM 614
#define RC_UNSUPPORTED_DATATYPE 615
#define RC_MEM_ALLOCATION_FAIL 617
#define RC_SCHEMA_NOT_FOUND 618


#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302

// Added new definitions for Record Manager
#define RC_RM_NO_TUPLE_WITH_GIVEN_RID 600
#define RC_SCAN_CONDITION_NOT_FOUND 601

// Added new definition for B-Tree
#define RC_ORDER_TOO_HIGH_FOR_PAGE 701
#define RC_INSERT_ERROR 702

// ASSIGNMENT 4
#define RC_MEMORY_ALLOCATION_MANAGER_ERROR 4000
#define RC_MANAGER_NULL_ERROR 4001
#define RC_B_PLUS_TREE_NOT_DELETING 4002
#define RC_ALREADY_EXISTED_KEY 4003
#define RC_IM_KEY_NOT_FOUND 4004
#define RC_NULL_FOR_TREE_MANAGER 4005
#define RC_ERROR_NULL_POINTER 4006
#define RC_BTREE_DELETE_FAILED 4007
#define RC_NO_INFO_TO_SCAN 4008
#define RC_DONE_NO_MORE_ENTRIES 4009
#define RC_MEMORY_CREATION_ERROR 4010

/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
  do {			  \
    RC_message=message;	  \
    return rc;		  \
  } while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
  do {									\
    int rc_internal = (code);						\
    if (rc_internal != RC_OK)						\
      {									\
	char *message = errorMessage(rc_internal);			\
	printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
	free(message);							\
	exit(1);							\
      }									\
  } while(0);


#endif
