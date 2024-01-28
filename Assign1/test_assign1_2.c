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

    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh))

}