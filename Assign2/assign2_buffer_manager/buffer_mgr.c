#include<stdio.h>
#include<stdlib.h>
#include<math.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

// numpages
// pageFileName
// stratData

typedef struct PageFrame {
    SM_PageHandle data;
    PageNumber pageNumber;
    int fixCountInfo;
    int dirtyBit;
    int hitNumber;
    int referNumber;
} FrameInPage;

int bufferSize = 0;
int indexRear = 0;
int writeCount = 0;
int hitCount = 0;
int pointerForClock = 0;
int pointerForLFU = 0;

void getConditionalBasedStrategy(BM_BufferPool *const bm);

/*
initBufferPool creates a new buffer pool with numPages page frames using the page replacement
strategy strategy. The pool is used to cache pages from the page file with name pageFileName.
Initially, all page frames should be empty. The page file should already exist, i.e., this method
should not generate a new page file. stratData can be used to pass parameters for the page
replacement strategy. For example, for LRU-k this could be the parameter k
*/
/* ---------- Buffer Manager Interface Pool Handling ---------- */
RC initBufferPool(BM_BufferPool * const bm, const char * const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData) {

            bm->pageFile = (char *) (pageFileName);
            bm->strategy = strategy;
            bm->numPages = numPages;
            bm->mgmtData = NULL;

            bufferSize = numPages;
            
            FrameInPage * frameInPage = malloc(numPages * sizeof(FrameInPage));

            for (int i=0;i<bufferSize;i++) {
                frameInPage[i].pageNumber = -1;
                frameInPage[i].dirtyBit = 0;
                frameInPage[i].hitNumber = 0;
                frameInPage[i].fixCountInfo = 0;
                frameInPage[i].referNumber = 0;
                frameInPage[i].data = NULL;
            }

            bm->mgmtData = frameInPage;

            writeCount = 0;
            pointerForClock = 0;
            pointerForLFU = 0;
            
            return RC_OK;
    }

RC shutdownBufferPool(BM_BufferPool *const bm) {
	if(bm!=NULL) {
        FrameInPage *frameInPage = (FrameInPage *) bm->mgmtData;
        forceFlushPool(bm);

        for(int i = 0; i < bufferSize; i++) {
            if(frameInPage[i].fixCountInfo != 0) {
                return RC_PINNED_PAGES_IN_BUFFER;
            }
        }

        free(frameInPage);
        bm->mgmtData = NULL;
    }
	return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm) {
    if (bm != NULL) {
        FrameInPage *frameInPage = (FrameInPage *) bm->mgmtData;
        
        for (int i=0;i<bufferSize;i++) {
            if((frameInPage[i].dirtyBit == 1) && (frameInPage[i].fixCountInfo==0)) {
                
                SM_FileHandle fileHandle;

                openPageFile(bm->pageFile, &fileHandle);

                writeBlock(frameInPage[i].pageNumber, &fileHandle, frameInPage[i].data);
			    
                frameInPage[i].dirtyBit = 0;

                writeCount++;
            }
        }
    }
    return RC_OK;
}

/* ------------ Buffer Manager Interface Access Pages ------------*/

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    FrameInPage *frameInPage = (FrameInPage *) bm->mgmtData;

    for (int i=0;i<bufferSize;i++) {
        if (frameInPage[i].pageNumber == page->pageNum) {
            frameInPage[i].dirtyBit = 1;
            return RC_OK;
        }
    }

    return RC_ERROR;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {

    FrameInPage *frameInPage = (FrameInPage *) bm->mgmtData;

    for (int i=0;i<bufferSize;i++) {
        if(frameInPage[i].pageNumber == page->pageNum) {
            frameInPage[i].fixCountInfo--;
            break;
        }
    }

    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    return RC_OK;

    FrameInPage * frameInPage = (FrameInPage *) bm->mgmtData;

    for (int i=0;i<bufferSize;i++) {
        if(frameInPage[i].pageNumber == page->pageNum) {
            SM_FileHandle fileHandler;
            openPageFile(bm->pageFile, &fileHandler);
            writeBlock(frameInPage[i].pageNumber, &fileHandler, frameInPage[i].data);

            frameInPage[i].dirtyBit = 0;

            writeCount++;
        }
    }
    return RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum) {

        FrameInPage * frameInPage = (FrameInPage *)bm->mgmtData;

        if(frameInPage[0].pageNumber == -1) {
            SM_FileHandle fileHandler;

            openPageFile(bm->pageFile, &fileHandler);
            frameInPage[0].data = (SM_PageHandle) malloc (PAGE_SIZE);

            ensureCapacity(pageNum, &fileHandler);

            readBlock(pageNum, &fileHandler, frameInPage[0].data);

            frameInPage[0].pageNumber=pageNum;
            frameInPage[0].fixCountInfo++;
            indexRear=hitCount=0;
            frameInPage[0].hitNumber=hitCount;
            frameInPage[0].referNumber=0;
            page->pageNum=pageNum;
            page->data = frameInPage[0].data;

            return RC_OK;
        } else {

            bool isBufferFull = true;

            for (int i=0;i<bufferSize;i++) {
                if(frameInPage[i].pageNumber != -1) {
                    if(frameInPage[i].pageNumber == pageNum) {

                        frameInPage[i].fixCountInfo++;
                        isBufferFull = false;

                        if(bm->strategy == RS_CLOCK) {
                            frameInPage[i].hitNumber = 1;
                        }
                        if (bm->strategy == RS_LRU) {
                            frameInPage[i].hitNumber = hitCount;
                        }
                        if(bm->strategy == RS_LFU) {
                            frameInPage[i].referNumber++;
                        } 

                        page->pageNum = pageNum;
                        page->data = frameInPage[i].data;

                        pointerForClock++;
                        break;
                    }
                } else {
                    SM_FileHandle fileHandler;

                    openPageFile(bm->pageFile, &fileHandler);
                    frameInPage[i].data = (SM_PageHandle) malloc (PAGE_SIZE);

                    readBlock(pageNum, &fileHandler, frameInPage[i].data);
                    
                    indexRear++;
                    hitCount++;

                    frameInPage[i].referNumber=0;
                    frameInPage[i].pageNumber=pageNum;
                    frameInPage[i].fixCountInfo=1;

                    if (bm->strategy == RS_CLOCK) {
                        frameInPage[i].hitNumber = 1; 
                    } else if(bm->strategy == RS_LRU) {
                        frameInPage[i].hitNumber = hitCount;
                    }

                    page->pageNum = pageNum;
                    page->data = frameInPage[i].data;

                    isBufferFull = false;
                    break;  
                }
            }

            if (isBufferFull == true) {
                FrameInPage * frameInPageTemp = (FrameInPage *) malloc (sizeof(FrameInPage));

                SM_FileHandle fileHandler;

                openPageFile(bm->pageFile, &fileHandler);

                frameInPageTemp->data = (SM_PageHandle) malloc (PAGE_SIZE);

                readBlock(pageNum, &fileHandler, frameInPageTemp->data);

                frameInPageTemp->pageNumber = pageNum;
                frameInPageTemp->fixCountInfo = 1;
                frameInPageTemp->referNumber = 0;
                frameInPageTemp->dirtyBit = 0;

                indexRear++;
                hitCount++;

                if(bm->strategy == RS_LRU) {
                    frameInPageTemp->hitNumber = hitCount;
                }
                else if (bm->strategy == RS_CLOCK) {
                    frameInPageTemp->hitNumber = 1;
                }

                page->data = frameInPageTemp->data;
                page->pageNum = pageNum;

                getConditionalBasedStrategy(bm);

            }
        }
        return RC_OK;
    }


void getConditionalBasedStrategy(BM_BufferPool *const bm) {

    switch (bm->strategy) {
        case RS_CLOCK:
            break;

        case RS_LRU:
            break;

        case RS_FIFO:
            break;

        case RS_LFU:
            break;

        case RS_LRU_K: 
            break;

        default:
            printf("You are in in wrong algorithm. This Algorithm is not available.");
            break;
    }
}


/* ------------------ Statistics Interface ------------------ */

PageNumber *getFrameContents (BM_BufferPool *const bm) {
    int i = 0;
    PageNumber * framesInPageNumber = malloc(sizeof(PageNumber) * bufferSize);
    FrameInPage * frameInPage = (FrameInPage *) bm->mgmtData;

    while (i< bufferSize) {
        framesInPageNumber[i] = (frameInPage[i].pageNumber != -1) ? frameInPage[i].pageNumber : NO_PAGE;
        i++;
    }

    return framesInPageNumber;
}

bool * getDirtyFlags (BM_BufferPool *const bm) {
    FrameInPage * frameInPage = (FrameInPage *) bm->mgmtData;
    bool *isFlagDirty = mallloc(sizeof(bool) * bufferSize);

    for (int i=0;i<bufferSize;i++) {
        isFlagDirty[i] = (frameInPage->dirtyBit == 1) ? true : false;
    }

    return isFlagDirty;
}

int *getFixCounts (BM_BufferPool *const bm) {
    FrameInPage * frameInPage = (FrameInPage *) bm->mgmtData;
    int *fixCounts = malloc(sizeof(int) * bufferSize);

    for (int i=0;i<bufferSize;i++) {
        fixCounts[i] = (frameInPage[i].fixCountInfo != -1) ? frameInPage[i].fixCountInfo : 0;
    }
    return fixCounts;
}

int getNumReadIO (BM_BufferPool *const bm) {
    // The read IO number is present at rear index so returning that.
    return (indexRear + 1);
}

int getNumWriteIO (BM_BufferPool *const bm) {
    // Returning the write IO number by using writeCount.
    return writeCount;
}
