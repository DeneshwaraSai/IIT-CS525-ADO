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

/**
 * Runs the test cases for single page content.
 * @returns None
 */
void testSinglePageContentTestCases(void);

int main (void) {
  testName = "";
  
  destroyPageFile(TESTPF);

  initStorageManager();

  testSinglePageContentTestCases();

  return 0;
}

void testSinglePageContentTestCases() {

    SM_FileHandle fh;
    SM_PageHandle ph;
    int i;

    testName = "test single page content";

    ph = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));
    
    // Test case - 1
    /**
     * Test case 1: File creation, opening, and destruction.
     *
     * 1) Creates a new page file.
     * 2) Opens the created file.
     * 3) Destroys the file, ensuring success.
     * 4) Tries to reopen the destroyed file, expecting an error.
     * 5) Prints a separator line for clarity.
     *
     * @returns None
     */
    printf("\nTest case 1\n");
    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh))
    ASSERT_TRUE(destroyPageFile(TESTPF) == 0, " Expected to destroy the file.");
    ASSERT_ERROR((openPageFile(TESTPF, &fh) > 0), "File cannot be opened because it was destroyed previous!");
    printf("--------------------------------------------------------");
    
 
    // Test case - 2
    /**
     * Test case 2: File I/O operations
     *
     * This test case checks the functionality of creating, opening, and closing a page file.
     * It also verifies that a file cannot be opened if it was destroyed previously.
     *
     * @param &fh The address of file handle.
     * @param TESTPF The file name.
     * @returns None
     */
    printf("\n\nTest case 2\n");
    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh))
    ASSERT_TRUE(closePageFile(&fh) == 0, " Expected to close the file!");
    ASSERT_ERROR((openPageFile(TESTPF, &fh) == 0), "File cannot be opened because it was destroyed previous!");
    printf("--------------------------------------------------------");
    
    destroyPageFile(TESTPF);

    // Test case - 3
    /**
     * Performs a test case to check if a page file is created successfully and if the first block is read correctly.
     *
     * @returns None
     */
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
    /**
     * Test case 4: Writes data to a page file.
     *
     * This test case creates a new page file, opens it, and writes data to the first block.
     * The data written to the block is a sequence of lowercase letters from 'a' to 'z'.
     * The expected result is that the write operation returns 0, indicating success.
     *
     * @returns None
     */
    printf("\n\nTest case 4\n");
    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh)) 
    for(int index = 0;index<PAGE_SIZE * 4;index++) {
       ph[index] = (index%26) + 'a';
    }
    ASSERT_TRUE(writeBlock(0, &fh, ph) == 0, "Written in blocks!");
    printf("\n--------------------------------------------------------\n");

    // Test case - 5
    /**
     * Test case 5: Reads the first block of data and checks if the values match the expected values.
     *
     * @returns None
     */
    printf("\n\nTest case 5\n");
    TEST_CHECK(readFirstBlock(&fh, ph));
    for(int index=0; index < 10;index++) {
        ASSERT_TRUE((ph[index] == (index%26) + 'a'), "Values matched");
    }
    printf("\n--------------------------------------------------------\n");

    // Test case - 6
    /**
     * Test case 6.
     *
     * 1) Tests getBlockPos function: Prints test case number, gets block position, and asserts it's 0.
     * 2) Tests readNextBlock function: Calls it and asserts it returns 0, indicating successful move to the next block.
     * 3) Validates getBlockPos again: Gets block position and asserts it's now 1.
     *
     * @returns None
     */
    printf("\n\nTest case 6\n");
    printf("\n%d\n" , getBlockPos(&fh));
    ASSERT_EQUALS_INT(getBlockPos(&fh) == 0, 1, "In 0th page");
    ASSERT_TRUE(readNextBlock(&fh, ph)==0, "Moved to next block.");
    ASSERT_EQUALS_INT(getBlockPos(&fh) == 1, 1, "In 1st page");
    printf("\n--------------------------------------------------------\n");

    // Test case - 7
    /**
     * Test case 7.
     *
     * 1) This test case verifies the functionality of the `readPreviousBlock` function.
     * 2) It prints the current block position using the `getBlockPos` function and asserts that it is equal to 0.
     * 3) Then, it attempts to move to the previous block using the `readPreviousBlock` function and asserts that the return value is 0, indicating success.
     * 4) Finally, it asserts that the current block position is equal to 0, indicating that it is in the first page.
     *
     * @returns None
     */
    printf("\n\nTest case 7\n");
    printf("\n%d\n" , getBlockPos(&fh));
    ASSERT_TRUE(readPreviousBlock(&fh, ph)==0, "Moved to previous block.");
    ASSERT_EQUALS_INT(getBlockPos(&fh) == 0, 1, "In 1st page");
    printf("\n--------------------------------------------------------\n");

    // Test case - 8
    /**
     * Test case 8.
     *
     * 1) Calls the readCurrentBlock function to move to the current block.
     * 2) Asserts that the return value of readCurrentBlock is 0, indicating success.
     * 3) Asserts that the current block position is equal to 0, indicating it is the first page.
     *
     * @returns None
     */
    printf("\n\nTest case 8\n");
    printf("\n%d\n" , getBlockPos(&fh));
    ASSERT_TRUE(readCurrentBlock(&fh, ph)==0, "Moved to current block.");
    ASSERT_EQUALS_INT(getBlockPos(&fh) == 0, 1, "Current page = 1st page");
    printf("\n--------------------------------------------------------\n");

    // Test case - 9 
    /**
     * Test case 9.
     * 
     * 1) Prints the current block position using the getBlockPos function.
     * 2) Moves to the last block using the readLastBlock function.
     * 3) Prints the current block position again using the getBlockPos function, to know whether it moved to respective block.
     * 4) Asserts that the current block position is 0 using the getBlockPos function.
     *
     * @param fh A pointer to the file handle.
     * @param ph A pointer to the page handle.
     * @returns None
     */
    printf("\n\nTest case 9\n");
    printf("%d\n" , getBlockPos(&fh));
    ASSERT_TRUE(readLastBlock(&fh, ph)==0, "Moved to last block.");
    printf("%d\n" , getBlockPos(&fh));
    ASSERT_EQUALS_INT(getBlockPos(&fh) == 0, 1, " getBlock position correct.");
    printf("\n--------------------------------------------------------\n");

    // Test case - 10
    printf("\n\nTest case 10\n");
    /**
     * Prints the total number of pages in the file handle.
     * Writes data to the current block of the file handle.
     *
     * @param fh A pointer to the file handle.
     * @param ph A pointer to the page handle.
     *
     * @returns None
     */
    printf("%d ", fh.totalNumPages);
    for(int index = 0; index<PAGE_SIZE * 5;index++) {
        ph[index] = ((index+1)%26) + 'A';
    }
    ASSERT_TRUE(writeCurrentBlock(&fh, ph) == 0, "Written at current Block");
    printf("\n--------------------------------------------------------\n");

    // Test Case - 11
    printf("\n\n Test case 11\n");
    /**
     * Tests the readFirstBlock function by reading the first block of a page file.
     * Then, it checks if the page file is successfully closed and destroys the page file.
     *
     * @param fh A pointer to the file handle.
     * @param ph A pointer to the page handle.
     *
     * @returns None
     */
    TEST_CHECK(readFirstBlock(&fh, ph));
    ASSERT_TRUE(closePageFile(&fh) == 0, " Expected to close the file!");
    destroyPageFile(TESTPF);
    printf("\n--------------------------------------------------------");
}