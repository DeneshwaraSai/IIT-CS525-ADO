#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "storage_mgr.h"
#include "dberror.h"

/* manipulating page files */

/**
 * Author : Deneshwara Sai Ila
 * Initializes the storage manager.
 *
 * @returns None
 */
void initStorageManager(void) {
    printf("The storage manager has initiated!");
}

/**
 * Author : Deneshwara Sai Ila
 * Creates a new page file with the given file name.
 *
 * @param fileName The name of the page file to be created.
 *
 * @returns RC_OK if the page file is successfully created, 
 * RC_FILE_NOT_FOUND if the file name is NULL or if there is an error creating the file.
 */
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

/**
 * Author : Prudhvi Teja Kari
 * Opens a page file and initializes the file handle.
 *
 * @param fileName The name of the page file to open.
 * @param fHandle Pointer to the file handle structure.
 *
 * @returns RC_OK if the file is successfully opened and the file handle is initialized,
 *          RC_FILE_HANDLE_NOT_INIT if the file handle is NULL,
 *          RC_FILE_NOT_FOUND if the file is not found,
 *          RC_READ_NON_EXISTING_PAGE if the file seek operation fails,
 *          or an appropriate error code.
 */
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

/**
 * Author : Deneshwara Sai Ila
 * Closes a page file.
 *
 * @param fHandle Pointer to the file handle.
 *
 * @returns RC_OK if the file is successfully closed, otherwise an error code.
 */
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

/**
 * Author : Prudhvi Teja Kari
 * Destroys a page file.
 *
 * @param fileName The name of the page file to be destroyed.
 *
 * @returns RC_OK if the page file is successfully destroyed, RC_FILE_NOT_FOUND if the file is not found.
 */
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

/**
 * Author : Deneshwara Sai Ila
 * Reads a block from a file into memory.
 *
 * @param pageNum The page number of the block to be read.
 * @param fHandle Pointer to the file handle structure.
 * @param memPage Pointer to the memory page where the block will be stored.
 *
 * @returns RC_OK if the block is successfully read, otherwise returns an error code:
 *          - Error Code : RC_FILE_HANDLE_NOT_INIT if the file handle is not initialized.
 *          - Error Code : RC_WRITE_FAILED if the memory page is not initialized.
 *          - Error Code : RC_READ_NON_EXISTING_PAGE if the page number is out of range.
 *          - Error Code : RC_FILE_NOT_FOUND if the file is not found.
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

/**
 * Author : Prudhvi Teja Kari
 * Reads the first block of a file into memory.
 *
 * @param fHandle Pointer to the file handle structure.
 * @param memPage Pointer to the memory page where the block will be stored.
 *
 * @returns RC value if it successfully reads the first block else it return errors. 
 * The readBlock function is responsible to do such operations.
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

/**
 * Author : Prudhvi Teja Kari
 * Reads the previous block from a file into the given memory page.
 *
 * @param fHandle Pointer to the file handle.
 * @param memPage Pointer to the memory page to store the read block.
 *
 * @returns RC value indicating the success or failure of the operation.
 *          - RC_FILE_HANDLE_NOT_INIT: If the file handle is NULL.
 *          - RC_WRITE_FAILED: If the memory page is NULL.
 *          - RC_READ_NON_EXISTING_PAGE: If the current page number is less than or equal to -1.
 *          - RC value returned by the readBlock function.
 */
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

/**
 * Author : Deneshwara Sai Ila
 * Reads the current block from a file into a memory page.
 *
 * @param fHandle Pointer to the file handle.
 * @param memPage Pointer to the memory page.
 *
 * @returns RC value indicating the success or failure of the operation.
 *          - RC_FILE_HANDLE_NOT_INIT: If the file handle is NULL.
 *          - RC_WRITE_FAILED: If the memory page is NULL.
 *          - RC_READ_NON_EXISTING_PAGE: If the current page number is -1.
 *          - RC value returned by the readBlock function.
 */
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

/**
 * Author : Prudhvi Teja Kari
 * Reads the next block from a file into memory.
 *
 * @param fHandle Pointer to the file handle structure.
 * @param memPage Pointer to the memory page.
 *
 * @returns RC value indicating the success or exception of the operation.
 *          - RC_FILE_HANDLE_NOT_INIT: If the file handle is not initialized.
 *          - RC_WRITE_FAILED: If the memory page is not initialized.
 *          - RC_FILE_READ_FAILED: If the block cannot be read from the file.
 *          - RC_OK: If the block is successfully read from the file.
 */
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
/**
 * Author : Deneshwara Sai Ila
 * Reads the last block of a file into the given memory page.
 *
 * @param fHandle Pointer to the file handle.
 * @param memPage Pointer to the memory page to store the last block.
 *
 * @returns RC value indicating the success or failure of the operation.
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
/**
 * Author : Prudhvi Teja Kari
 * Writes a block of data to a specified page in a file.
 *
 * @param pageNum The page number to write the block to.
 * @param fHandle Pointer to the file handle structure.
 * @param memPage Pointer to the memory page containing the data to be written.
 *
 * @returns RC_OK if the block is successfully written, otherwise returns an error code stating that:
 *          - RC_FILE_HANDLE_NOT_INIT if the file handle is not initialized.
 *          - RC_WRITE_FAILED if the write operation fails.
 *          - RC_FILE_NOT_FOUND if the file is not found.
 *          - RC_READ_NON_EXISTING_PAGE if the page number is out of range.
 */
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

/**
 * Author : Deneshwara Sai Ila
 * Writes the current block to a file.
 *
 * @param fHandle Pointer to the file handle structure.
 * @param memPage Pointer to the memory page to be written.
 *
 * @returns RC value indicating the success or failure of the operation.
 *          - RC_FILE_HANDLE_NOT_INIT: If the file handle is not initialized.
 *          - RC_FILE_NOT_FOUND: If the file is not found.
 *          - RC_OK: If the write operation is successful.
 */
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

/**
 * Author : Deneshwara Sai Ila
 * Appends an empty block to the file associated with the given file handle.
 *
 * @param fHandle Pointer to the file handle.
 *
 * @returns RC_OK if the operation is successful, otherwise an appropriate error code.
 */
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

/**
 * Author : Prudhvi Teja Kari
 * 
 * 1) Ensures that the file handle has enough capacity to accommodate the specified number of pages.
 * 2) If the file handle is not initialized, returns RC_FILE_HANDLE_NOT_INIT.
 * 3) If the number of pages is less than 1, returns RC_READ_NON_EXISTING_PAGE.
 * 4) If the file handle already has equal or greater capacity than the specified number of pages, returns RC_WRITE_FAILED.
 * 5) If the file associated with the file handle is not found, returns RC_FILE_NOT_FOUND.
 * 6) If the file handle does not have enough capacity, appends empty blocks to the file until the capacity is reached.
 *
 * @param numberOfPages The desired number of pages.
 * @param fHandle The file handle.
 *
 * @returns RC_OK if the
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