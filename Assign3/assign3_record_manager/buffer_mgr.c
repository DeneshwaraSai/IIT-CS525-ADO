#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

int clockPointerCount = 0;

int lfuPointerCount = 0;

int sizeOfBuffer = 0; // total buffer size

int indexForRear = 0; // present rear index

int hitCount = 0; // count of hits 

int writeCount = 0; // count for write operations


/**
 * The `FramesInPage` struct represents a page in memory with attributes such as dirty bit, fix count,
 * data, page number, hit number, and reference number.
 * 
 * dirtyBit - The `dirtyBit` property in the `FramesInPage` struct typically represents
 * whether the page has been changed since it was last read from or written to the disk. 
 * 
 * fixCountInfo - The `fixCountInfo` property in the `FramesInPage` struct represents
 * the number of times the page has been pinned in memory. 
 * 
 * {SM_PageHandle} data - This property is used to store the actual data or contents of the page.
 * 
 * {PageNumber} pageNumber - The `pageNumber` property represents the page number of the page in memory. 
 *
 * hitNumber - The `hitNumber` property in the `FramesInPage` struct represents the
 * number of times a page has been accessed or "hit" in the memory. 
 * 
 * refNumber - The `refNumber` property in the `FramesInPage` struct represents the
 * reference number of the page. 
 */
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

/**
 * author : Ila Deneshwara Sai 
 * Description:
 * The FIFO function iterates through the buffer pool size to find a frame with fix count 0, writes the
 * frame to disk if it's dirty, and then replaces it with a new frame.
 * 
 * @param bm BM_BufferPool *const bm: This parameter is a pointer to a buffer pool structure.  
 * @param page: represents a structure that contains information about a page in memory.
 */
void FIFO(BM_BufferPool *const bm, FramesInPage *page) {
    int indexForFront = indexForRear % sizeOfBuffer;

    FramesInPage * framesInPage = (FramesInPage *) bm->mgmtData;

    for (int i=0;i<sizeOfBuffer;i++) {

        if (framesInPage[indexForFront].fixCountInfo == 0) {
            if (framesInPage[indexForFront].dirtyBit == 1) {
				SM_FileHandle fHandler;

                // RC openPageFile(char *fileName, SM_FileHandle *fHandle)
                openPageFile(bm->pageFile, &fHandler);

                // RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
                writeBlock(framesInPage[indexForFront].pageNumber, &fHandler, framesInPage[indexForFront].data);
                writeCount++;
            }    

           /* The code snippet is assigning values from a `page` struct to a `framesInPage` array at a specific index `indexForFront`. 
		   It is copying the `data`, `dirtyBit`, `fixCountInfo`, and `pageNumber` values from the `page` struct 
		   to the corresponding fields in the `framesInPage` array at the specified index. */
		    framesInPage[indexForFront].data = page->data;
            framesInPage[indexForFront].dirtyBit = page->dirtyBit;
            framesInPage[indexForFront].fixCountInfo = page->fixCountInfo;
            framesInPage[indexForFront].pageNumber = page->pageNumber;
            break;
        } else {
            indexForFront++;
            indexForFront = (indexForFront % sizeOfBuffer == 0) ? 0 : indexForFront;
        }
    }
}

/**
 * 
 * author : Prudhvi Teja Kari
 * Description:
 * The LRU (Least Recently Used) function is used to implement a page replacement algorithm within a buffer pool.
 * 
 * @param bm BM_BufferPool *const bm: This parameter is a pointer to a buffer pool structure.  
 * @param page: represents a structure that contains information about a page in memory.
 */
void LRU(BM_BufferPool *const bm, FramesInPage *page) {

	SM_FileHandle fhandler;
	int lruHitNumber = 0, lruHitIndex = 0;

	FramesInPage * framesInPages = (FramesInPage * ) bm->mgmtData;

	/* 
	* Loops till present bufferSize.
	* It checks if the `fixCountInfo` of each element is equal to 0. 
	* If it is, it assigns the `hitNumber` of that element to `lruHitNumber` 
	* and the index of that element to `lruHitIndex`, and then breaks out
	* of the loop. If the `fixCountInfo` is not 0, it prints "Fix count is not 0". 
	*/
	for (int i=0;i<sizeOfBuffer;i++) {
		if (framesInPages[i].fixCountInfo == 0) {
			lruHitNumber = framesInPages[i].hitNumber;
			lruHitIndex = i;
			break;
		} else {
			printf("Fix count is not 0");
		}
	}

	printf("\nThe lru hit index is : %d \n", lruHitIndex);
	/* 
	* The below for loop iterates from `lruHitIndex + 1` up to `sizeOfBuffer`. 
	* It is checking if the `hitNumber` of each element is less than the current `lruHitNumber`. 
	* If it is, then it updates the `lruHitNumber` and `lruHitIndex` with the values from that element. 
	* This code is essentially finding the element with the lowest `hitNumber` in the array after a specific index. 
	*/
	for (int i = (lruHitIndex + 1);i<sizeOfBuffer;i++) {
		if (framesInPages[i].hitNumber < lruHitNumber) {
			lruHitNumber = framesInPages[i].hitNumber;
			lruHitIndex = i;
		}
	}

	if(framesInPages[lruHitIndex].dirtyBit == 1) {

		// RC openPageFile(char *fileName, SM_FileHandle *fHandle)
		openPageFile(bm->pageFile, &fhandler);

		// RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
		writeBlock(framesInPages[lruHitIndex].pageNumber, &fhandler, framesInPages[lruHitIndex].data);
		writeCount++;
	}

	/* 
	* The code snippet is assigning values from a `page` struct to a `framesInPage` array at a specific index `lruHitIndex`. 
	* It is copying the `data`, `dirtyBit`, `hitNumber`, `fixCountInfo`, and `pageNumber` values from the `page` struct 
	* to the corresponding fields in the `framesInPage` array at the specified index. 
	*/	

	framesInPages[lruHitIndex].pageNumber = page->pageNumber;
	framesInPages[lruHitIndex].dirtyBit = page->dirtyBit;
	framesInPages[lruHitIndex].data = page->data;

	framesInPages[lruHitIndex].hitNumber = page->hitNumber;
	framesInPages[lruHitIndex].fixCountInfo = page->fixCountInfo;
}

/* ==================================================== */


/* Buffer Manager Interface Pool Handling */

/* 
 * author : Ila Deneshwara Sai 
 * Description:
 * initBufferPool() - creates a new buffer pool with numPages page frames using the page replacement
strategy strategy. The pool is used to cache pages from the page file with name pageFileName.
Initially, all page frames should be empty. The page file should already exist, i.e., this method
should not generate a new page file. stratData can be used to pass parameters for the page
replacement strategy. For example, for LRU-k this could be the parameter k.
*/

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData) {
    sizeOfBuffer = numPages;

    bm->pageFile = (char *) (pageFileName);
    bm->strategy = strategy;
    bm->numPages = numPages;
    bm->mgmtData = NULL;

    FramesInPage *framesInPage = malloc (sizeof(FramesInPage) * numPages);

/* 
	* The above code is initializing an array of structures named `framesInPage`. 
 	* The fields being initialized include `data`, `dirtyBit`, `fixCountInfo`, `hitNumber`, `refNumber`, and `pageNumber`. 
*/
    for (int i=0;i<sizeOfBuffer;i++) {
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

/* 

 * shutdownBufferPool() - destroys a buffer pool. This method should free up all resources associated
	with buffer pool. For example, it should free the memory allocated for page frames. If the buffer
	pool contains any dirty pages, then these pages should be written back to disk before destroying
	the pool. It is an error to shutdown a buffer pool that has pinned pages.
*/

/**
 * author : Prudhvi Teja Kari
 * Description:
 * The function `shutdownBufferPool` closes the buffer pool and checks for pinned pages before locking
 * memory and setting the management data to NULL.
 * 
 * @param bm BM_BufferPool structure containing information about the buffer pool and its management data.
 * 
 * @return RC_OK
 */

RC shutdownBufferPool(BM_BufferPool *const bm) {

    FramesInPage *framesInPage = (FramesInPage *)bm->mgmtData;

	forceFlushPool(bm);
    
    for (int i=0;i<sizeOfBuffer;i++) {
        
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

/**
 * author : Ila Deneshwara Sai 
 * Description:
 * The function `forceFlushPool` iterates through the buffer pool frames, writing dirty pages back to disk if their fix count is zero.
 * 
 * @param bm BM_BufferPool *const bm
 * 
 * @return The function `RC forceFlushPool(BM_BufferPool *const bm)` is returning an `RC` (Return Code) value, specifically `RC_OK`.
 */
RC forceFlushPool(BM_BufferPool *const bm) {

    FramesInPage* framesInPage = (FramesInPage *) bm->mgmtData;

    for (int i=0;i<sizeOfBuffer;i++) {

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

/**
 * The function `markDirty` sets the dirty bit to 1 for a page in the buffer pool if the page number matches.
 * 
 * author : Ila Deneshwara Sai 
 * Description:
 * @param bm BM_BufferPool *const bm
 * @param page The `page` parameter is a pointer to a structure `BM_PageHandle` which contains 
 * information about a page in the buffer pool. 
 * 
 * @return RC_OK or RC_ERROR is being returned, depending on whether the page number matches any page in the buffer pool.
 */

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    
    FramesInPage* framesInPage = (FramesInPage *) bm->mgmtData;

    for (int i = 0;i<sizeOfBuffer;i++) {
        if(page->pageNum == framesInPage[i].pageNumber) {
            // here we are setting dirty bit as 1 if the above condition is true
            framesInPage[i].dirtyBit = 1;
            return RC_OK;
        }
    }
    return RC_ERROR;
}

/* unpinPage unpins the page. The pageNum field of page should be used to figure out which page to unpin. */
/** 
 * author : Prudhvi Teja Kari
 * Description:
 * The unpinPage function decreases the fix count of a page frame in the buffer pool.
 * 
 * @param bm BM_BufferPool *const bm
 * @param page The `page` parameter is a pointer to a structure `BM_PageHandle` which contains 
 * information about a page in the buffer pool. 
 * 
 * @return RC_OK
 */

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {

    FramesInPage* framesInPage = (FramesInPage *) bm->mgmtData;
	printf("\n");
    for (int i = 0;i<sizeOfBuffer;i++) {
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
/**
 * author : Ila Deneshwara Sai 
 * Description:
 * The function `forcePage` writes a page from the buffer pool to disk if it is dirty.
 * @param bm BM_BufferPool *const bm
 * @param page The `page` parameter is a pointer to a structure `BM_PageHandle` which contains 
 * information about a page in the buffer pool. 
 * 
 * @return The function `RC forcePage` is returning `RC_OK`.
 */

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {

    FramesInPage* framesInPage = (FramesInPage *) bm->mgmtData;

    for (int i = 0;i<sizeOfBuffer;i++) {
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
 * author : Prudhvi Teja Kari
 * Description:
pinPage() pins the page with page number pageNum. The buffer manager is responsible to set the
pageNum field of the page handle passed to the method. Similarly, the data field should point to
the page frame the page is stored in (the area in memory storing the content of the page).
*/ 
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
	FramesInPage *framesInPage = (FramesInPage *)bm->mgmtData;
	
	printf("Here in PIN_PAGE");
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
		
		for(int i = 0; i < sizeOfBuffer; i++) {
			if(framesInPage[i].pageNumber != -1) {	
				if(framesInPage[i].pageNumber == pageNum) {
					
                    framesInPage[i].fixCountInfo++;
					isBufferPoolFull = false;
					hitCount++; 
                    
					if(bm->strategy == RS_LRU){
						framesInPage[i].hitNumber = hitCount;
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

			page->pageNum = pageNum;
			page->data = newFramePage->data;			

			switch(bm->strategy) {					
				case RS_LRU: 
					LRU(bm, newFramePage);
					break;

                case RS_LFU: 
					printf("RS_LFU : This Algorithm is not inplemented");
					break;
				
				case RS_CLOCK: 
					printf("RS_CLOCK : This Algorithm is not inplemented");
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

/* 
 * author : Prudhvi Teja Kari
 * Description:
 * The getFrameContents() returns an array of PageNumbers (of size numPages) where the ith element is the 
 * number of the page stored in the ith page frame. An empty page frame is represented using the constant NO PAGE.
*/
PageNumber *getFrameContents (BM_BufferPool *const bm) {
	FramesInPage *framesInPage = (FramesInPage *) bm->mgmtData;
	PageNumber *frameInfo = malloc(sizeof (PageNumber) * sizeOfBuffer);
	
    for (int i=0;i<sizeOfBuffer;i++) {
		frameInfo[i] = (framesInPage[i].pageNumber != -1) ? framesInPage[i].pageNumber : NO_PAGE;
	}
	return frameInfo;
}

/*
 * author : Ila Deneshwara Sai 
 * Description:
 * The getDirtyFlags() returns an array of bools (of size numPages) 
 * where the ith element is TRUE if the page stored in the i
 * th page frame is dirty. Empty page frames are considered as clean.
*/
bool *getDirtyFlags (BM_BufferPool *const bm) {
	bool *dirtyBoolFlag = malloc(sizeof(bool) * sizeOfBuffer);
	FramesInPage *frameInPage = (FramesInPage *)bm->mgmtData;
	
	for(int i = 0; i < sizeOfBuffer; i++) {
		dirtyBoolFlag[i] = (frameInPage[i].dirtyBit == 1) ? true : false ;
	}	
	return dirtyBoolFlag;
}

/*
 * author : Prudhvi Teja Kari
 * Description:
    The getFixCounts() returns an array of ints (of size numPages) where the i
    th element is the fix count of the page stored in the ith page frame. 
    Return 0 for empty page frames.
*/
int * getFixCounts (BM_BufferPool *const bm) {
	int *countFixList = malloc(sizeof(int) * sizeOfBuffer);
	FramesInPage *pageFrame= (FramesInPage *)bm->mgmtData;
	
    for(int i=0;i<sizeOfBuffer;i++) {
		countFixList[i] = (pageFrame[i].fixCountInfo != -1) ? pageFrame[i].fixCountInfo : 0;
	}	
	return countFixList;
}

/* 
 * author : Ila Deneshwara Sai 
 * Description:
The getNumReadIO() returns the number of pages that have been read from disk since a
buffer pool has been initialized. You code is responsible to initializing this statistic at pool creating
time and update whenever a page is read from the page file into a page frame.
*/
int getNumReadIO (BM_BufferPool *const bm) {
	return (indexForRear + 1);
}

/*
 * author : Ila Deneshwara Sai 
 * Description:
getNumWriteIO() returns the number of pages written to the page file since the buffer pool has been
initialized.
*/
int getNumWriteIO (BM_BufferPool *const bm) {
	return writeCount;
}