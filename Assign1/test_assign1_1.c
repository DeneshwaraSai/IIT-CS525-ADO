#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void testCreateOpenClose(void);
static void testSinglePageContent(void);

/* Additional Test cases. */
static void testSinglePageContentTestCases(void);

/* main function running all tests */
int main (void) {
  testName = "";
  
  initStorageManager();

  testCreateOpenClose();
  testSinglePageContent();

  testSinglePageContentTestCases();

  return 0;
}


/* check a return code. If it is not RC_OK then output a message, error description, and exit */
/* Try to create, open, and close a page file */
void testCreateOpenClose(void) {
  SM_FileHandle fh;

  testName = "test create open and close methods";

  TEST_CHECK(createPageFile (TESTPF));
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
  ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");

  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TESTPF));

  // // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TESTPF, &fh) != RC_OK), "opening non-existing file should return an error.");

  TEST_DONE();
}

/* Try to create, open, and close a page file */
void testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");
  
  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++) 
    ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("first block was empty\n");

  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeBlock (0, &fh, ph));
  printf("writing first block\n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < 10; i++) {
    printf("CHARACTER : %c ", (i % 10) + '0');
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  }
  printf("reading first block\n");

  ASSERT_TRUE(appendEmptyBlock(&fh) == 0, "Appended empty blocks successfully.");

  TEST_CHECK(ensureCapacity(10, &fh));

  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));  
  
  TEST_DONE();
}

/* Additional Test cases. */
void testSinglePageContentTestCases() {

    SM_FileHandle fh;
    SM_PageHandle ph;
    int i;

    testName = "test single page content";

    ph = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));
    
    // Test case - 1
    printf("\nTest case 1\n");
    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh))
    ASSERT_TRUE(destroyPageFile(TESTPF) == 0, " Expected to destroy the file.");
    ASSERT_ERROR((openPageFile(TESTPF, &fh) > 0), "File cannot be opened because it was destroyed previous!");
    printf("--------------------------------------------------------");
    
    // Test case - 2
    printf("\n\nTest case 2\n");
    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh))
    ASSERT_TRUE(closePageFile(&fh) == 0, " Expected to close the file!");
    ASSERT_ERROR((openPageFile(TESTPF, &fh) == 0), "File cannot be opened because it was destroyed previous!");
    printf("--------------------------------------------------------");
    
    destroyPageFile(TESTPF);

    // Test case - 3
    printf("\n\nTest case 3\n");
    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh))
    TEST_CHECK(readFirstBlock(&fh, ph));
    for(int index = 0;index<10;index++) {
        printf("-- %c -- %d\n", ph[index], (ph[index] == 0));
        ASSERT_EQUALS_INT(ph[index] == 0, 1, "NO CHARACTER PLACED, SO NULL/EMPTY!");
    }
    printf("--------------------------------------------------------");

    // Test case - 4
    printf("\n\nTest case 4\n");
    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh)) 
    for(int index = 0;index<PAGE_SIZE * 4;index++) {
       ph[index] = (index%26) + 'a';
    }
    ASSERT_TRUE(writeBlock(0, &fh, ph) == 0, "Written in blocks!");
    printf("\n--------------------------------------------------------\n");

    // Test case - 5
    printf("\n\nTest case 5\n");
    TEST_CHECK(readFirstBlock(&fh, ph));
    for(int index=0; index < 10;index++) {
        ASSERT_TRUE((ph[index] == (index%26) + 'a'), "Values matched");
    }
    printf("\n--------------------------------------------------------\n");

    // Test case - 6
    printf("\n\nTest case 6\n");
    printf("\n%d\n" , getBlockPos(&fh));
    ASSERT_EQUALS_INT(getBlockPos(&fh) == 0, 1, "In 0th page");
    ASSERT_TRUE(readNextBlock(&fh, ph)==0, "Moved to next block.");
    ASSERT_EQUALS_INT(getBlockPos(&fh) == 1, 1, "In 1st page");
    printf("\n--------------------------------------------------------\n");

    // Test case - 7
    printf("\n\nTest case 7\n");
    printf("\n%d\n" , getBlockPos(&fh));
    ASSERT_TRUE(readPreviousBlock(&fh, ph)==0, "Moved to previous block.");
    ASSERT_EQUALS_INT(getBlockPos(&fh) == 0, 1, "In 1st page");
    printf("\n--------------------------------------------------------\n");

    // Test case - 8
    printf("\n\nTest case 8\n");
    printf("\n%d\n" , getBlockPos(&fh));
    ASSERT_TRUE(readCurrentBlock(&fh, ph)==0, "Moved to current block.");
    ASSERT_EQUALS_INT(getBlockPos(&fh) == 0, 1, "Current page = 1st page");
    printf("\n--------------------------------------------------------\n");

    // Test case - 9 
    printf("\n\nTest case 9\n");
    printf("%d\n" , getBlockPos(&fh));
    ASSERT_TRUE(readLastBlock(&fh, ph)==0, "Moved to last block.");
    printf("%d\n" , getBlockPos(&fh));
    ASSERT_EQUALS_INT(getBlockPos(&fh) == 0, 1, " getBlock position correct.");
    printf("\n--------------------------------------------------------\n");

    // Test case - 10
    printf("\n\nTest case 10\n");
    printf("%d ", fh.totalNumPages);
    for(int index = 0; index<PAGE_SIZE * 5;index++) {
        ph[index] = ((index+1)%26) + 'A';
    }
    ASSERT_TRUE(writeCurrentBlock(&fh, ph) == 0, "Written at current Block");
    printf("\n--------------------------------------------------------\n");

    // // Test Case - 11
    // printf("\n\n Test case 11\n");	
    // TEST_CHECK(ensureCapacity(6, &fh));
    // printf("\n--------------------------------------------------------\n");

    // Test Case - 11
    printf("\n\n Test case 11\n");
    TEST_CHECK(readFirstBlock(&fh, ph));

    ASSERT_TRUE(closePageFile(&fh) == 0, " Expected to close the file!");
    destroyPageFile(TESTPF);
    printf("\n--------------------------------------------------------");

}