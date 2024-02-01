//  gcc -o test_assign2 test_assign1_2.c storage_mgr.c dberror.c 
// ./test_assign1_2
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.txt"

void testSinglePageContent(void);

int main (void) {
  testName = "";
  
  initStorageManager();

  testSinglePageContent();

  return 0;
}

void testSinglePageContent() {

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

    // Test Case - 11
    printf("\n\n Test case 11\n");	
    TEST_CHECK(ensureCapacity(6, &fh));
    printf("\n--------------------------------------------------------\n");

    // Test Case - 12
    printf("\n\n Test case 12\n");
    TEST_CHECK(readFirstBlock(&fh, ph));

    for (int index = 0; index < 10; index++) {
        ASSERT_TRUE(ph[index] == 0, "Block is empty");
    }
    // ASSERT_TRUE(closePageFile(&fh) == 0, " Expected to close the file!");
    // destroyPageFile(TESTPF);
    printf("\n--------------------------------------------------------");

}