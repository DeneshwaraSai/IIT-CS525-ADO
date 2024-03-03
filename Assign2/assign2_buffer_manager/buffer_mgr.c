#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

int clockPointerCount = 0;

int lfuPointerCount = 0;

int bufferSize = 0; // total buffer size

int indexForRear = 0; // present rear index

int hitCount = 0; // count of hits 

int writeCount = 0; // count for write operations

typedef struct PageStructure
{
    int dirtyBit; 
	int fixCountInfo;
    
    SM_PageHandle data; 
	PageNumber pageNumber; 

	int hitNumber;
	int refNumber;
} FramesInPage;


/* ==================================================== */

extern void FIFO(BM_BufferPool *const bm, FramesInPage *page) {
    int indexForFront = indexForRear % bufferSize;

    FramesInPage * framesInPage = (FramesInPage *) bm->mgmtData;

    for (int i=0;i<bufferSize;i++) {

        if (framesInPage[indexForFront].fixCountInfo == 0) {
            if (framesInPage[indexForFront].dirtyBit == 1) {
                SM_FileHandle fHandler;

                // RC openPageFile(char *fileName, SM_FileHandle *fHandle)
                openPageFile(bm->pageFile, &fHandler);

                // RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
                writeBlock(framesInPage[indexForFront].pageNumber, &fHandler, framesInPage[indexForFront].data);
                writeCount++;
            }    

            framesInPage[indexForFront].data = page->data;
            framesInPage[indexForFront].dirtyBit = page->dirtyBit;
            framesInPage[indexForFront].fixCountInfo = page->fixCountInfo;
            framesInPage[indexForFront].pageNumber = page->pageNumber;
            break;
        } else {

            indexForFront++;
            indexForFront = (indexForFront % bufferSize == 0) ? 0 : indexForFront;
        }
    }
}

// Defining LFU (Least Frequently Used) function
extern void LFU(BM_BufferPool *const bm, FramesInPage *page)
{
	//printf("LFU Started");
	FramesInPage *pageFrame = (FramesInPage *) bm->mgmtData;
	
	int i, j, leastFreqIndex, leastFreqRef;
	leastFreqIndex = lfuPointerCount;	
	
	// Interating through all the page frames in the buffer pool
	for(i = 0; i < bufferSize; i++)
	{
		if(pageFrame[leastFreqIndex].fixCountInfo == 0)
		{
			leastFreqIndex = (leastFreqIndex + i) % bufferSize;
			leastFreqRef = pageFrame[leastFreqIndex].refNumber;
			break;
		}
	}

	i = (leastFreqIndex + 1) % bufferSize;

	// Finding the page frame having minimum refNum (i.e. it is used the least frequent) page frame
	for(j = 0; j < bufferSize; j++)
	{
		if(pageFrame[i].refNumber < leastFreqRef)
		{
			leastFreqIndex = i;
			leastFreqRef = pageFrame[i].refNumber;
		}
		i = (i + 1) % bufferSize;
	}
		
	// If page in memory has been modified (dirtyBit = 1), then write page to disk	
	if(pageFrame[leastFreqIndex].dirtyBit == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pageFrame[leastFreqIndex].pageNumber, &fh, pageFrame[leastFreqIndex].data);
		
		// Increase the writeCount which records the number of writes done by the buffer manager.
		writeCount++;
	}
	
	// Setting page frame's content to new page's content		
	pageFrame[leastFreqIndex].data = page->data;
	pageFrame[leastFreqIndex].pageNumber = page->pageNumber;
	pageFrame[leastFreqIndex].dirtyBit = page->dirtyBit;
	pageFrame[leastFreqIndex].fixCountInfo = page->fixCountInfo;
	lfuPointerCount = leastFreqIndex + 1;
}

// Defining LRU (Least Recently Used) function
extern void LRU(BM_BufferPool *const bm, FramesInPage *page)
{	
	FramesInPage *pageFrame = (FramesInPage *) bm->mgmtData;
	int i, leastHitIndex, leastHitNum;

	// Interating through all the page frames in the buffer pool.
	for(i = 0; i < bufferSize; i++)
	{
		// Finding page frame whose fixCount = 0 i.e. no client is using that page frame.
		if(pageFrame[i].fixCountInfo == 0)
		{
			leastHitIndex = i;
			leastHitNum = pageFrame[i].hitNumber;
			break;
		}
	}	

	// Finding the page frame having minimum hitNum (i.e. it is the least recently used) page frame
	for(i = leastHitIndex + 1; i < bufferSize; i++)
	{
		if(pageFrame[i].hitNumber < leastHitNum)
		{
			leastHitIndex = i;
			leastHitNum = pageFrame[i].hitNumber;
		}
	}

	// If page in memory has been modified (dirtyBit = 1), then write page to disk
	if(pageFrame[leastHitIndex].dirtyBit == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pageFrame[leastHitIndex].pageNumber, &fh, pageFrame[leastHitIndex].data);
		
		// Increase the writeCount which records the number of writes done by the buffer manager.
		writeCount++;
	}
	
	// Setting page frame's content to new page's content
	pageFrame[leastHitIndex].data = page->data;
	pageFrame[leastHitIndex].pageNumber = page->pageNumber;
	pageFrame[leastHitIndex].dirtyBit = page->dirtyBit;
	pageFrame[leastHitIndex].fixCountInfo = page->fixCountInfo;
	pageFrame[leastHitIndex].hitNumber = page->hitNumber;
}

// Defining CLOCK function
extern void CLOCK(BM_BufferPool *const bm, FramesInPage *page)
{	
	//printf("CLOCK Started");
	FramesInPage *pageFrame = (FramesInPage *) bm->mgmtData;
	while(1)
	{
		clockPointerCount = (clockPointerCount % bufferSize == 0) ? 0 : clockPointerCount;

		if(pageFrame[clockPointerCount].hitNumber == 0)
		{
			// If page in memory has been modified (dirtyBit = 1), then write page to disk
			if(pageFrame[clockPointerCount].dirtyBit == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pageFrame[clockPointerCount].pageNumber, &fh, pageFrame[clockPointerCount].data);
				
				// Increase the writeCount which records the number of writes done by the buffer manager.
				writeCount++;
			}
			
			// Setting page frame's content to new page's content
			pageFrame[clockPointerCount].data = page->data;
			pageFrame[clockPointerCount].pageNumber = page->pageNumber;
			pageFrame[clockPointerCount].dirtyBit = page->dirtyBit;
			pageFrame[clockPointerCount].fixCountInfo = page->fixCountInfo;
			pageFrame[clockPointerCount].hitNumber = page->hitNumber;
			clockPointerCount++;
			break;	
		}
		else
		{
			// Incrementing clockPointer so that we can check the next page frame location.
			// We set hitNum = 0 so that this loop doesn't go into an infinite loop.
			pageFrame[clockPointerCount++].hitNumber = 0;		
		}
	}
}


/* ==================================================== */


/* Buffer Manager Interface Pool Handling */

/* initBufferPool() - creates a new buffer pool with numPages page frames using the page replacement
strategy strategy. The pool is used to cache pages from the page file with name pageFileName.
Initially, all page frames should be empty. The page file should already exist, i.e., this method
should not generate a new page file. stratData can be used to pass parameters for the page
replacement strategy. For example, for LRU-k this could be the parameter k.
*/

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData) {
    bufferSize = numPages;

    bm->pageFile = (char *) (pageFileName);
    bm->strategy = strategy;
    bm->numPages = numPages;
    bm->mgmtData = NULL;

    FramesInPage *framesInPage = malloc (sizeof(FramesInPage) * numPages);

    for (int i=0;i<bufferSize;i++) {
        framesInPage[i].data = NULL;
        framesInPage[i].dirtyBit = 0;
        framesInPage[i].fixCountInfo = 0;
        framesInPage[i].hitNumber = 0;
        framesInPage[i].refNumber = 0;
        framesInPage[i].pageNumber = -1;
    }

    bm->mgmtData = framesInPage;
    lfuPointerCount = 0;
    writeCount = 0;
    clockPointerCount = 0;

    return RC_OK;
}   

/* shutdownBufferPool() - destroys a buffer pool. This method should free up all resources associated
with buffer pool. For example, it should free the memory allocated for page frames. If the buffer
pool contains any dirty pages, then these pages should be written back to disk before destroying
the pool. It is an error to shutdown a buffer pool that has pinned pages.
*/
RC shutdownBufferPool(BM_BufferPool *const bm) {

    FramesInPage *framesInPage = (FramesInPage *)bm->mgmtData;

	forceFlushPool(bm);
    
    for (int i=0;i<bufferSize;i++) {
        
        printf("frames Page fixCount : %d\n", framesInPage[i].fixCountInfo);

        if(framesInPage[i].fixCountInfo != 0) {
            return RC_PINNED_PAGES_IN_BUFFER;
        } else {

        }
    }
    free(framesInPage);
    bm->mgmtData = NULL;
    return RC_OK;
}

/*
forceFlushPool() - causes all dirty pages (with fix count 0) from the buffer pool to be written to
disk.
*/

RC forceFlushPool(BM_BufferPool *const bm) {

    FramesInPage* framesInPage = (FramesInPage *) bm->mgmtData;

    for (int i=0;i<bufferSize;i++) {

        printf("FixCount Info : %d | Dirty Count Bit : %d\n", framesInPage[i].fixCountInfo, framesInPage[i].dirtyBit);
        if ((framesInPage[i].fixCountInfo == 0) 
                && (framesInPage[i].dirtyBit == 1)) {
            
            printf("When condition true\n");
            SM_FileHandle fileHandler;
            printf("PageFile : %s\n",bm->pageFile);
            
            /* RC openPageFile(char *fileName, SM_FileHandle *fHandle) */
            openPageFile(bm->pageFile, &fileHandler);

            /* RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) */
            writeBlock(framesInPage[i].pageNumber, &fileHandler, framesInPage[i].data);
            writeCount++;

            framesInPage[i].dirtyBit = 0;
        }
    }
    return RC_OK;
}

// ========================================================================================================================================================================================================================================================================

// Buffer Manager Interface Access Pages
// ***** PAGE MANAGEMENT FUNCTIONS ***** //

// markDirty() - marks a page as dirty
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    
    FramesInPage* framesInPage = (FramesInPage *) bm->mgmtData;

    for (int i = 0;i<bufferSize;i++) {
        if(page->pageNum == framesInPage[i].pageNumber) {
            // here we are setting dirty bit as 1 if the above condition is true
            framesInPage[i].dirtyBit = 1;
            return RC_OK;
        }
    }
    return RC_ERROR;
}

/*
unpinPage unpins the page. 
The pageNum field of page should be used to figure out which
page to unpin.
*/
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {

    FramesInPage* framesInPage = (FramesInPage *) bm->mgmtData;
	printf("\n");
    for (int i = 0;i<bufferSize;i++) {
        if(page->pageNum == framesInPage[i].pageNumber) {
            printf("Before - Page frame - fix : %d\n", framesInPage[i].fixCountInfo);
			framesInPage[i].fixCountInfo--;
			printf("After  - Page frame - fix : %d\n", framesInPage[i].fixCountInfo);
            break;
        }
    }
    return RC_OK;
}

// forcePage() should write the current content of the page back to the page file on disk
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {

    FramesInPage* framesInPage = (FramesInPage *) bm->mgmtData;

    for (int i = 0;i<bufferSize;i++) {
        if(framesInPage[i].pageNumber == page->pageNum) {
            SM_FileHandle fHandle;

            // RC openPageFile(char *fileName, SM_FileHandle *fHandle)
            openPageFile(bm->pageFile, &fHandle);

            // RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
            writeBlock(framesInPage[i].pageNumber, &fHandle, framesInPage[i].data);
            writeCount++;

            framesInPage[i].dirtyBit = 0;
        }
    }
    return RC_OK;
}

/*
pinPage() pins the page with page number pageNum. The buffer manager is responsible to set the
pageNum field of the page handle passed to the method. Similarly, the data field should point to
the page frame the page is stored in (the area in memory storing the content of the page).
*/ 
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
	FramesInPage *framesInPage = (FramesInPage *)bm->mgmtData;
	
	if(framesInPage[0].pageNumber == -1){
		SM_FileHandle fHandle;
		framesInPage[0].data = (SM_PageHandle) malloc(PAGE_SIZE);

		// RC openPageFile (char *fileName, SM_FileHandle *fHandle) 
        openPageFile(bm->pageFile, &fHandle);

		// RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
        ensureCapacity(pageNum, &fHandle);
		
        // RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
        readBlock(pageNum, &fHandle, framesInPage[0].data);
		
        framesInPage[0].pageNumber = pageNum;
		framesInPage[0].fixCountInfo++;
		
        indexForRear = hitCount = 0;

		framesInPage[0].refNumber = 0;
		framesInPage[0].hitNumber = hitCount;	
		
		page->data = framesInPage[0].data;
        page->pageNum = pageNum;
		
		return RC_OK;		
	} else {	
		bool isBufferPoolFull = true;
		
		for(int i = 0; i < bufferSize; i++) {
			if(framesInPage[i].pageNumber != -1) {	
				if(framesInPage[i].pageNumber == pageNum) {
					
                    framesInPage[i].fixCountInfo++;
					isBufferPoolFull = false;
					hitCount++; 
                    
					if(bm->strategy == RS_LRU){
						framesInPage[i].hitNumber = hitCount;
                    } else if(bm->strategy == RS_CLOCK) {
						framesInPage[i].hitNumber = 1;
                    } else if(bm->strategy == RS_LFU) {
						framesInPage[i].refNumber++;
                    }
					
					page->pageNum = pageNum;
					page->data = framesInPage[i].data;

					clockPointerCount++;
					break;
				}				
			} else {
				SM_FileHandle fh;
                framesInPage[i].data = (SM_PageHandle) malloc(PAGE_SIZE);

                // RC openPageFile (char *fileName, SM_FileHandle *fHandle) 
				openPageFile(bm->pageFile, &fh);
								        
                // RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
                readBlock(pageNum, &fh, framesInPage[i].data);
				
				framesInPage[i].fixCountInfo = 1;
                framesInPage[i].pageNumber = pageNum;
				framesInPage[i].refNumber = 0;
				
                indexForRear++;	
				hitCount++;

				if(bm->strategy == RS_LRU) {
 					framesInPage[i].hitNumber = hitCount;
                } else if(bm->strategy == RS_CLOCK) {
 					framesInPage[i].hitNumber = 1;
                }
						
				page->pageNum = pageNum;
				page->data = framesInPage[i].data;
				
				isBufferPoolFull = false;
				break;
			}
		}
		
		if(isBufferPoolFull == true) {
			FramesInPage *newFramePage = (FramesInPage *) malloc(sizeof(FramesInPage));		
			
			SM_FileHandle fh;
			newFramePage->data = (SM_PageHandle) malloc(PAGE_SIZE);

            // RC openPageFile (char *fileName, SM_FileHandle *fHandle) 
			openPageFile(bm->pageFile, &fh);
            
            // RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
			readBlock(pageNum, &fh, newFramePage->data);
		
			newFramePage->dirtyBit = 0;		
			newFramePage->fixCountInfo = 1;
			newFramePage->refNumber = 0;
            newFramePage->pageNumber = pageNum;
			
            indexForRear++;
			hitCount++;

			if(bm->strategy == RS_LRU) {
				newFramePage->hitNumber = hitCount;				
            }
			else if(bm->strategy == RS_CLOCK){
				newFramePage->hitNumber = 1;
            }

			page->pageNum = pageNum;
			page->data = newFramePage->data;			

			switch(bm->strategy) {					
				case RS_LRU: 
					LRU(bm, newFramePage);
					break;

                case RS_LFU: 
					LFU(bm, newFramePage);
					break;
				
				case RS_CLOCK: 
					CLOCK(bm, newFramePage);
					break;

                case RS_FIFO: 
					FIFO(bm, newFramePage);
					break;

                case RS_LRU_K:
					printf("\n LRU-k algorithm not implemented");
					break;
  				
				default:
					printf("The selected option/ algorithm is not present.");
					break;
			}		
		}		
		return RC_OK;
	}	
}

// ******************** STATISTICS FUNCTIONS ******************** //

/*The getFrameContents() returns an array of PageNumbers (of size numPages) where the ith element is the number of the page stored in the ith page frame. An empty page frame is represented using the constant NO PAGE.*/
PageNumber *getFrameContents (BM_BufferPool *const bm) {
	FramesInPage *framesInPage = (FramesInPage *) bm->mgmtData;
	PageNumber *frameInfo = malloc(sizeof (PageNumber) * bufferSize);
	
    for (int i=0;i<bufferSize;i++) {
		frameInfo[i] = (framesInPage[i].pageNumber != -1) ? framesInPage[i].pageNumber : NO_PAGE;
	}
	return frameInfo;
}

/*
 * The getDirtyFlags() returns an array of bools (of size numPages) 
 * where the ith element is TRUE if the page stored in the i
 * th page frame is dirty. Empty page frames are considered as clean.
*/
bool *getDirtyFlags (BM_BufferPool *const bm) {
	bool *dirtyBoolFlag = malloc(sizeof(bool) * bufferSize);
	FramesInPage *frameInPage = (FramesInPage *)bm->mgmtData;
	
	for(int i = 0; i < bufferSize; i++) {
		dirtyBoolFlag[i] = (frameInPage[i].dirtyBit == 1) ? true : false ;
	}	
	return dirtyBoolFlag;
}

/*
    The getFixCounts() returns an array of ints (of size numPages) where the i
    th element is the fix count of the page stored in the ith page frame. 
    Return 0 for empty page frames.
*/
int *getFixCounts (BM_BufferPool *const bm) {
	int *countFixList = malloc(sizeof(int) * bufferSize);
	FramesInPage *pageFrame= (FramesInPage *)bm->mgmtData;
	
    for(int i=0;i<bufferSize;i++) {
		countFixList[i] = (pageFrame[i].fixCountInfo != -1) ? pageFrame[i].fixCountInfo : 0;
	}	
	return countFixList;
}

/* 
The getNumReadIO() returns the number of pages that have been read from disk since a
buffer pool has been initialized. You code is responsible to initializing this statistic at pool creating
time and update whenever a page is read from the page file into a page frame.
*/
int getNumReadIO (BM_BufferPool *const bm) {
	return (indexForRear + 1);
}

/*
getNumWriteIO() returns the number of pages written to the page file since the buffer pool has been
initialized.
*/
int getNumWriteIO (BM_BufferPool *const bm) {
	return writeCount;
}