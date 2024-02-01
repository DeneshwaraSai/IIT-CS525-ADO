#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "storage_mgr.h"
#include "dberror.h"

/* manipulating page files */

void initStorageManager(void) {
    printf("The storage manager has initiated!");
}

RC createPageFile(char * fileName) {

    if (fileName == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    FILE *file = fopen(fileName, "w+");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    char *fileInfo = (char *)calloc(PAGE_SIZE, sizeof(char));
    fwrite(fileInfo, sizeof(char), PAGE_SIZE, file);

    free(fileInfo);
    fclose(file);

    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    } if (fileName == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    FILE *file = fopen(fileName,"r+");
    if(file == NULL) {
        return RC_FILE_NOT_FOUND;
    } 
    
    if (fseek(file, 0, SEEK_END) != 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    int size = ftell(file);
    printf("SIZE : %d \n", size);

    fHandle->mgmtInfo = file;
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;
    fHandle->totalNumPages = (int) (size/ PAGE_SIZE);

    printf("\n%s -> %d -> %d\n",fHandle->fileName, fHandle->curPagePos, fHandle->curPagePos);
    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle) {
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    FILE *file = fHandle->mgmtInfo;
    fclose(file);
    if(file != NULL) {
        file = NULL;
    }
    free(file);
    printf("\nFile closed\n");
    return RC_OK;
}

RC destroyPageFile(char *fileName) {
    if (fileName == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    
    int removeFile = remove(fileName);

    if (removeFile != 0) {
        return RC_FILE_NOT_FOUND;
    }

    return RC_OK;
}

/* -------------------------------------------------------------------------------------------------- */

/* reading blocks from disc */
/* 
readBlock
– The method reads the block at position pageNum from a file and stores its content in the memory pointed
to by the memPage page handle.
– If the file has less than pageNum pages, the method should return RC READ NON EXISTING PAGE 
*/
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // validates parameters
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    } if (memPage == NULL) {
        return RC_WRITE_FAILED;
    } if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    FILE *file = fHandle->mgmtInfo;
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    // seek to the offset of the file
    int offset = pageNum * PAGE_SIZE;
    if (fseek(file, offset, SEEK_SET) != 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // read data from memory, update page
    if (fread(memPage, sizeof(char), PAGE_SIZE, file) < PAGE_SIZE) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    fHandle->curPagePos = pageNum;
    return RC_OK;
}

int getBlockPos (SM_FileHandle *fHandle) {
    if(fHandle == NULL) {
        return -1;
    }
    return fHandle->curPagePos;
}
/* 
readFirstBlock , readLastBlock
– Read the first respective last page in a file
*/
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    } if (memPage == NULL) {
        return RC_WRITE_FAILED;
    }
    printf("FILE NAME : %s\n", fHandle->fileName);

    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    } if (memPage == NULL) {
        return RC_WRITE_FAILED;
    }

    int currentPageNumber = getBlockPos(fHandle);
    if ((currentPageNumber - 1) <= -1) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    return readBlock((currentPageNumber-1), fHandle, memPage);
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {

    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    } if (memPage == NULL) {
        return RC_WRITE_FAILED;
    }

    int currentPageNumber = getBlockPos(fHandle);
    if (currentPageNumber == -1) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    return readBlock((currentPageNumber), fHandle, memPage);
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {    
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    } if (memPage == NULL) {
        return RC_WRITE_FAILED;
    }

    int currentPageNumber = getBlockPos(fHandle);
    if(currentPageNumber == -1 || ((currentPageNumber + 1) > fHandle->totalNumPages)) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    return readBlock((currentPageNumber + 1), fHandle, memPage);
}

/* 
readFirstBlock , readLastBlock
– Read the first respective last page in a file
*/
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {

    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    } if (memPage == NULL) {
        return RC_WRITE_FAILED;
    }

    int lastPageNumber = fHandle->totalNumPages - 1;
    printf("LAST PAGE : %d -> fHandle last page : %d\n", lastPageNumber, (fHandle->totalNumPages));
    if(lastPageNumber == -1) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    return readBlock(lastPageNumber, fHandle, memPage);
}

/* writing blocks to a page file */
/*
writeBlock , writeCurrentBlock
– Write a page to disk using either the current position or an absolute position.
• appendEmptyBlock
– Increase the number of pages in the file by one. The new last page should be filled with zero bytes.
• ensureCapacity
– If the file has less than numberOfPages pages then increase the size to numberOfPages.
*/

/* writing blocks to a page file */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    } if(memPage == NULL) {
        return RC_WRITE_FAILED;
    }

    FILE * file = fHandle->mgmtInfo;
    if(file == NULL) {
        return RC_FILE_NOT_FOUND;
    } if(pageNum < 0 && pageNum > fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    int offset = pageNum * PAGE_SIZE;
    int fseekNum = fseek(file, offset, SEEK_SET);
    printf("\nFSEEK : %d\n", fseekNum);
    if (fseekNum != 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    printf("STRLEN : %lu", strlen(memPage));
    // Eg - Syntax : fwrite(str , 1 , sizeof(str) , fp );
    int fwriteFile = fwrite(memPage, sizeof(char), strlen(memPage), file);
    printf("\nfwriteFile : %d\n", fwriteFile);
    if (fwriteFile < PAGE_SIZE) {
        return RC_WRITE_FAILED;
    }

    fHandle->curPagePos = pageNum;
    return RC_OK;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    FILE * file = fHandle->mgmtInfo;
    if(file == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

RC appendEmptyBlock (SM_FileHandle *fHandle) { 
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
  
    FILE * file = fHandle->mgmtInfo;
    if(file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    int offset = 0;
    int fseekNum = fseek(file, offset, SEEK_SET);
    printf("\nFSEEK : %d\n", fseekNum);
    if (fseekNum != 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    char *str = (char*) calloc(PAGE_SIZE, sizeof(char));
    // Syntax = fwrite(str_data, min_size, Max_size, file);
    int fwriteFile = fwrite(str, sizeof(char), PAGE_SIZE, file);
    printf("\nfwriteFile : %d\n", fwriteFile);
    if (fwriteFile < PAGE_SIZE) {
        free(str);
        return RC_WRITE_FAILED;
    }
    printf("BEFORE INC : %d\n", fHandle->totalNumPages);
    fHandle->totalNumPages++;
    free(str);
    printf("AFTER INC : %d\n", fHandle->totalNumPages);

    return RC_OK;
}
/*
ensureCapacity – If the file has less than numberOfPages pages then increase the size to numberOfPages.
*/
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {

    printf("\nScanning pages (totalNumPages) : %d -> %d\n", (fHandle->totalNumPages), numberOfPages);
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    } if(numberOfPages < 1) {
        return RC_READ_NON_EXISTING_PAGE;
    } if(fHandle->totalNumPages >= numberOfPages) {
        return RC_WRITE_FAILED;
    }
    
    FILE * file = fHandle->mgmtInfo;
    if(file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    printf("\nScanning pages (totalNumPages) : %d", (fHandle->totalNumPages));
    printf("\nScanning pages : %d\n", (numberOfPages - fHandle->totalNumPages));

    for (int i = 0; i < (numberOfPages - fHandle->totalNumPages);i++) {
        RC responseCode = appendEmptyBlock(fHandle);
        printf("jjhj jgjhgj : %d", responseCode);
        if (responseCode != RC_OK) {
            return responseCode;
        }
    }

    printf("\nAFTER : Scanning pages (totalNumPages) : %d", (fHandle->totalNumPages));
    printf("\nAFTER : Scanning pages : %d\n", (numberOfPages - fHandle->totalNumPages));

    return RC_OK;
}