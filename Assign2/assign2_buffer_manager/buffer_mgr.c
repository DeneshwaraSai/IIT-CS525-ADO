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
                
        return RC_OK;
    }