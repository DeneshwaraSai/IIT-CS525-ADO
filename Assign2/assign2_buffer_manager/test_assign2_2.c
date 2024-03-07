#include "storage_mgr.h"
#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"
#include "dberror.h"
#include "test_helper.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// var to store the current test's name
char *testName;

typedef struct PageStructure
{
    int dirtyBit; 
	int fixCountInfo;
    
    SM_PageHandle data; 
	PageNumber pageNumber; 

	int hitNumber;
	int refNumber;
} FramesInPage;


// check whether two the content of a buffer pool is the same as an expected content 
// (given in the format produced by sprintPoolContent)
#define ASSERT_EQUALS_POOL(expected,bm,message)			        \
  do {									\
    char *real;								\
    char *_exp = (char *) (expected);                                   \
    real = sprintPoolContent(bm);					\
    if (strcmp((_exp),real) != 0)					\
      {									\
	printf("[%s-%s-L%i-%s] FAILED: expected <%s> but was <%s>: %s\n",TEST_INFO, _exp, real, message); \
	free(real);							\
	exit(1);							\
      }									\
    printf("[%s-%s-L%i-%s] OK: expected <%s> and was <%s>: %s\n",TEST_INFO, _exp, real, message); \
    free(real);								\
  } while(0)

// test and helper methods
static void testCreatingAndReadingDummyPages (void);
static void createDummyPages(BM_BufferPool *bm, int num);
static void checkDummyPages(BM_BufferPool *bm, int num);

static void testReadPage (void);

static void testClock (void);
static void testLRU (void);

static void testFIFO(void); 

// main method
// We are only with FIFO and LRU.  
int 
main (void) 
{
  initStorageManager();
  testName = "";

  testCreatingAndReadingDummyPages();
  testReadPage();
  // testClock();
  // testLFU();
  
  testFIFO();
  printf("\n=================================================================================================================================\n\n");
  testLRU();
  
  return 0;
}

// create n pages with content "Page X" and read them back to check whether the content is right
void testCreatingAndReadingDummyPages (void)
{
  BM_BufferPool *bm = MAKE_POOL();
  testName = "Creating and Reading Back Dummy Pages";

  CHECK(createPageFile("testbuffer.bin"));

  createDummyPages(bm, 22);
  checkDummyPages(bm, 20);

  createDummyPages(bm, 10000);
  checkDummyPages(bm, 10000);

  CHECK(destroyPageFile("testbuffer.bin"));

  free(bm);
  TEST_DONE();
}


void createDummyPages(BM_BufferPool *bm, int num)
{
  int i;
  BM_PageHandle *h = MAKE_PAGE_HANDLE();

  CHECK(initBufferPool(bm, "testbuffer.bin", 3, RS_FIFO, NULL));
  
  for (i = 0; i < num; i++)
    {
      CHECK(pinPage(bm, h, i));
      sprintf(h->data, "%s-%i", "Page", h->pageNum);
      CHECK(markDirty(bm, h));
      CHECK(unpinPage(bm,h));
    }

  CHECK(shutdownBufferPool(bm));

  free(h);
}

void checkDummyPages(BM_BufferPool *bm, int num)
{
  int i;
  BM_PageHandle *h = MAKE_PAGE_HANDLE();
  char *expected = malloc(sizeof(char) * 512);

  CHECK(initBufferPool(bm, "testbuffer.bin", 3, RS_FIFO, NULL));

  for (i = 0; i < num; i++)
    {
      CHECK(pinPage(bm, h, i));

      sprintf(expected, "%s-%i", "Page", h->pageNum);
      ASSERT_EQUALS_STRING(expected, h->data, "reading back dummy page content");

      CHECK(unpinPage(bm,h));
    }

  CHECK(shutdownBufferPool(bm));

  free(expected);
  free(h);
}

void testReadPage ()
{
  BM_BufferPool *bm = MAKE_POOL();
  BM_PageHandle *h = MAKE_PAGE_HANDLE();
  testName = "Reading a page";

  CHECK(createPageFile("testbuffer.bin"));
  CHECK(initBufferPool(bm, "testbuffer.bin", 3, RS_FIFO, NULL));
  
  CHECK(pinPage(bm, h, 0));
  CHECK(pinPage(bm, h, 0));

  CHECK(markDirty(bm, h));

  CHECK(unpinPage(bm,h));
  CHECK(unpinPage(bm,h));

  CHECK(forcePage(bm, h));

  CHECK(shutdownBufferPool(bm));
  CHECK(destroyPageFile("testbuffer.bin"));

  free(bm);
  free(h);

  TEST_DONE();
}

void testClock(void)
{
    // expected results
    const char *poolContents[]= {
        
    "[3x0],[-1 0],[-1 0],[-1 0]",
    "[3x0],[2 0],[-1 0],[-1 0]",
    "[3x0],[2 0],[0 0],[-1 0]",
    "[3x0],[2 0],[0 0],[8 0]",
    "[4 0],[2 0],[0 0],[8 0]",
    "[4 0],[2 0],[0 0],[8 0]",
    "[4 0],[2 0],[5 0],[8 0]",
    "[4 0],[2 0],[5 0],[0 0]",
    "[9 0],[2 0],[5 0],[0 0]",
    "[9 0],[8 0],[5 0],[0 0]",
    "[9 0],[8 0],[3x0],[0 0]",
    "[9 0],[8 0],[3x0],[2 0]"
    };
    const int orderRequests[]= {3,2,0,8,4,2,5,0,9,8,3,2};
        
    int i;
    int snapshot = 0;
    BM_BufferPool *bm = MAKE_POOL();
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    testName = "Testing CLOCK page replacement";

    CHECK(createPageFile("testbuffer.bin"));
    createDummyPages(bm, 100);
    CHECK(initBufferPool(bm, "testbuffer.bin", 4, RS_CLOCK, NULL));
    
    for (i=0;i<11;i++)
    {
        pinPage(bm,h,orderRequests[i]);
	if(orderRequests[i] == 3)
		markDirty(bm,h);
        unpinPage(bm,h);
        ASSERT_EQUALS_POOL(poolContents[snapshot++], bm, "check pool content using pages");
    }
    
    forceFlushPool(bm);
    // check number of write IOs
    ASSERT_EQUALS_INT(2, getNumWriteIO(bm), "check number of write I/Os");
    ASSERT_EQUALS_INT(10, getNumReadIO(bm), "check number of read I/Os");
    
    CHECK(shutdownBufferPool(bm));
    CHECK(destroyPageFile("testbuffer.bin"));
    
    free(bm);
    free(h);
    TEST_DONE();
}

void testLFU(void)
{
    // expected results
    const char *poolContents[]= {
        
   "[3 0],[-1 0],[-1 0]",
   "[3 0],[7 0],[-1 0]",
   "[3 0],[7 0],[6 0]",
   "[4 0],[7 0],[6 0]",
   "[4 0],[7 0],[6 0]",
   "[4 0],[2 0],[6 0]",
   "[1 0],[2 0],[6 0]",
   "[1 0],[9 0],[6 0]",
   "[2 0],[9 0],[6 0]",
   "[2 0],[8 0],[6 0]"
    };
    const int orderRequests[]= {3,7,6,4,6,2,1,9,2,8};
        
    int i;
    int snapshot = 0;
    BM_BufferPool *bm = MAKE_POOL();
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    testName = "Testing LFU page replacement";

    CHECK(createPageFile("testbuffer.bin"));
    createDummyPages(bm, 100);
    CHECK(initBufferPool(bm, "testbuffer.bin", 3, RS_LFU, NULL));
    
    for (i=0;i<10;i++)
    {
        pinPage(bm,h,orderRequests[i]);
	//if(orderRequests[i] == 3)
	//	markDirty(bm,h);
        unpinPage(bm,h);
        ASSERT_EQUALS_POOL(poolContents[snapshot++], bm, "check pool content using pages");
    }
    
    forceFlushPool(bm);
    // check number of write IOs
    ASSERT_EQUALS_INT(0, getNumWriteIO(bm), "check number of write I/Os");
    ASSERT_EQUALS_INT(9, getNumReadIO(bm), "check number of read I/Os");
    
    CHECK(shutdownBufferPool(bm));
    CHECK(destroyPageFile("testbuffer.bin"));
    
    free(bm);
    free(h);
    TEST_DONE();
}

void testFIFO() {
  const char * poolContents[]= {
            "[1 0],[2 0],[3 0]",
            "[4 0],[5 0],[6 0]",
            "[7 0],[8 0],[9 0]",
            "[-1 -1],[-2 -1],[-3 0]",
            "[-4 -4],[-5 0],[-6 0]",
            "[7 0],[-8 0],[-9 0]",
            "[1 0],[2 0],[3 0]",
            "[4 0],[5 0],[6 0]",
            "[7 0],[8 0],[9 0]",
            "[-2 0],[-8 -9],[-6 0]"
          };

  const int orderRequests[]= {2, 1, 9, 2, 8, 3, 7, 6, 4, 6};
  char* fileName = "testBufferFIFO.bin";
  int snapshot = 0;

  BM_BufferPool * bufferPool = MAKE_POOL();
  BM_PageHandle * pHandler = MAKE_PAGE_HANDLE();

  testName = "Testing FIFO Page Replacement Algorithm.";

  CHECK(createPageFile(fileName));
  printf("File created Successfully : %s\n", fileName);

  createDummyPages(bufferPool, 100);
  printf("Page created Successfully : %s\n", fileName);

  CHECK(initBufferPool(bufferPool, fileName, 3, RS_FIFO, NULL));

  printf("Page Initiated Successfully : %s\n", fileName);
  FramesInPage* framesInPage = NULL;
  for (int i=0;i<9;i++) {

    ASSERT_TRUE(pinPage(bufferPool, pHandler, orderRequests[i]) == 0, "The pinPage() executed successfully.");

    FramesInPage* framesInPage = (FramesInPage *) bufferPool->mgmtData; 
    printf("NOW FRAMES : %d", framesInPage->fixCountInfo);
        
    ASSERT_TRUE(markDirty(bufferPool, pHandler) == 0, "The makeDirty() executed successfully.");

    ASSERT_TRUE(unpinPage(bufferPool, pHandler) == 0, "The unpinPage() executed successfully.");

    framesInPage = (FramesInPage *) bufferPool->mgmtData; 

    snapshot++;
    printf("poolContents[snapshot++] : %s", (poolContents[snapshot]));
    printf("\n BEFORE fixCountInfo : %d | hitNumber : %d | refNumber: %d \n", framesInPage->fixCountInfo, framesInPage->hitNumber, framesInPage->refNumber);
    
    ASSERT_EQUALS_INT(bufferPool->numPages, 3, "True: Testing the number of pages.");

    printf("\ngetNumReadIO : %d", getNumReadIO(bufferPool));
    printf("\ngetNumWriteIO : %d", getNumWriteIO(bufferPool));

    PageNumber * pageNum = getFrameContents(bufferPool);
  
    printf("\nBEFORE : Page Number : %d", *pageNum);
    
    printf("\n--------------------------------------- %d\n", i);
  }

  ASSERT_EQUALS_INT(getNumReadIO(bufferPool),(8), "Checking read IO Info");
  ASSERT_EQUALS_INT(getNumWriteIO(bufferPool),(5), "Checking write IO Info");

  PageNumber * pageNum = getFrameContents(bufferPool);
  ASSERT_EQUALS_INT(* pageNum, 6, "checking the page number.");

  printf("AFTER : Page Number : %d", * pageNum);
  ASSERT_EQUALS_INT(forceFlushPool(bufferPool), 0, "The forceFlushPool() executed Successfully.");

  free(bufferPool);
  free(pHandler);

  TEST_DONE();
}

void testLRU() {
  const char * poolContents[]= {
            "[1 0],[2 0],[3 0]",
            "[1 0],[2 0],[3 0]",
            "[4 0],[5 0],[6 0]",
            "[4 0],[5 0],[6 0]",
            "[7 0],[8 0],[9 0]",
            "[7 0],[8 0],[9 0]",
            "[-2 0],[-8 -9],[-6 0]"
            "[-1 -1],[-2 -1],[-3 0]",
            "[-4 -4],[-5 0],[-6 0]",
            "[-7 0],[-8 0],[-9 0]",
          };

  const int orderRequests[]= {2, 1, 9, 2, 8, 3, 7, 6, 4, 6};
  char* fileName = "testBufferLRU.bin";
  int snapshot = 0;

  BM_BufferPool * bufferPool = MAKE_POOL();
  BM_PageHandle * pHandler = MAKE_PAGE_HANDLE();

  testName = "Testing LRU Page Replacement Algorithm.";

  CHECK(createPageFile(fileName));
  printf("File created Successfully : %s\n", fileName);

  createDummyPages(bufferPool, 100);
  printf("Page created Successfully : %s\n", fileName);

  CHECK(initBufferPool(bufferPool, fileName, 3, RS_LRU, NULL));

  printf("Page Initiated Successfully : %s\n", fileName);
  FramesInPage* framesInPage = NULL;

  int actualRead[]  = {1, 2, 3, 3, 4, 5, 6, 7, 8};
  int actualWrite[] = {0, 0, 0, 0, 1, 2, 3, 4, 5};

  for (int i=0;i<9;i++) {

    ASSERT_TRUE(pinPage(bufferPool, pHandler, orderRequests[i]) == 0, "The pinPage() executed successfully.");

    FramesInPage* framesInPage = (FramesInPage *) bufferPool->mgmtData; 
    printf("NOW FRAMES : %d", framesInPage->fixCountInfo);
        
    ASSERT_TRUE(markDirty(bufferPool, pHandler) == 0, "The makeDirty() executed successfully.");

    ASSERT_TRUE(unpinPage(bufferPool, pHandler) == 0, "The unpinPage() executed successfully.");

    framesInPage = (FramesInPage *) bufferPool->mgmtData; 

    snapshot++;
    printf("\n BEFORE fixCountInfo : %d | hitNumber : %d | refNumber: %d \n", framesInPage->fixCountInfo, framesInPage->hitNumber, framesInPage->refNumber);
    
    ASSERT_EQUALS_INT(bufferPool->numPages, 3, "True: Testing the number of pages.");

    PageNumber * pageNum = getFrameContents(bufferPool);
  
    printf("\nBEFORE : Page Number : %d", *pageNum);

    printf("\nREAD  : %d", (getNumReadIO(bufferPool)));
    printf("\nWRITE : %d\n", (getNumWriteIO(bufferPool)));

    ASSERT_EQUALS_INT((getNumReadIO(bufferPool)), actualRead[i], "Checking read IO pool value.");
    ASSERT_EQUALS_INT((getNumWriteIO(bufferPool)), actualWrite[i], "Checking write IO pool value.");

    printf("\n--------------------------------------- %d\n", i);
  }

  ASSERT_EQUALS_INT(getNumReadIO(bufferPool),(8), "Checking read IO Info");
  ASSERT_EQUALS_INT(getNumWriteIO(bufferPool),(5), "Checking write IO Info");

  PageNumber * pageNum = getFrameContents(bufferPool);
  ASSERT_EQUALS_INT(* pageNum, 7, "checking the page number.");

  printf("AFTER : Page Number : %d", * pageNum);
  ASSERT_EQUALS_INT(forceFlushPool(bufferPool), 0, "The forceFlushPool() executed Successfully.");

  free(bufferPool);
  free(pHandler);

  TEST_DONE();
}