/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

/**
 * Authors: Erzhen Zhang 9075858317, Yingjie Shen 9076384123, Xinping Liu 9078291599
 * Filename: buffer.cpp
 * Purpose: This file defines the functions of Buffer Manager. By using clock algorithm and other operations, we could allocate a buffer frame
 */
#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

    /**
    * @param uint32_t
    * @return BufMgr 
    * @purpose Constructor of BufMgr class
    */
    BufMgr::BufMgr(std::uint32_t bufs)
            : numBufs(bufs) {
        bufDescTable = new BufDesc[bufs];

        for (FrameId i = 0; i < bufs; i++) {
            bufDescTable[i].frameNo = i;
            bufDescTable[i].valid = false;
        }

        bufPool = new Page[bufs];

        int htsize = ((((int) (bufs * 1.2)) * 2) / 2) + 1;
        hashTable = new BufHashTbl(htsize);  // set up a hash table for the buffer manager

        clockHand = bufs - 1;
    }

    /**
    * @param none
    * @return none 
    * @purpose Clean out the dirty pages out of buffer pool
    */
    BufMgr::~BufMgr() {
        delete[] bufPool;
    }

    /**
    * @param none
    * @return none 
    * @purpose find the next frame in the buffer pool
    */
    void BufMgr::advanceClock() {
        clockHand = (clockHand + 1) % numBufs;
    }

    /**
    * @param FrameId
    * @return none 
    * @purpose By using the clock algorithm, allocate a free frame
    */
    void BufMgr::allocBuf(FrameId &frame) {
        bool found = false;
        std::uint32_t pinnedCount = 0;
        while (!found) {
            advanceClock(); //point the clock to next frame
            //if all pages are pinned, throw exception
            if (pinnedCount >= numBufs) {
                throw BufferExceededException();
            }
            //check if it is valid
            if (bufDescTable[clockHand].valid) {
                //check if the refbit was recently referenced
                if (bufDescTable[clockHand].refbit) {
                    //reset
                    bufDescTable[clockHand].refbit = false;
                    continue;
                } else {
                    //check if the page is pinned
                    if (bufDescTable[clockHand].pinCnt > 0) {
                        pinnedCount++;
                        continue;
                     //if not pinned
                    } else {
                        //remove content from hasg table
                        hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
                        //check th dirty bit
                        if (bufDescTable[clockHand].dirty) {
                            bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
                            bufDescTable[clockHand].Clear();
                            found = true;
                        } else {
                            //set page
                            bufDescTable[clockHand].Clear();
                            //exit loop
                            found = true;
                        }
                        frame = bufDescTable[clockHand].frameNo; //set frame
                    }
                }
            } else {
                bufDescTable[clockHand].Clear();
                frame = bufDescTable[clockHand].frameNo; 
                found = true;
            }
        }
    }

    /**
    * @param File pointer, constant PageId, Page reference 
    * @return none 
    * @purpose Read a page from disk and set it into buffer pool 
    */
    void BufMgr::readPage(File *file, const PageId pageNo, Page *&page) {
        //check if the page is already in the buffer pool
        FrameId index;
        try {
            //page is in the pool
            hashTable->lookup(file, pageNo, index);
            //set refbit
            bufDescTable[index].refbit = true;
            bufDescTable[index].pinCnt++; //increment pin count
            page = &bufPool[index]; 
        }
        catch (HashNotFoundException e) {
            allocBuf(index); //allocate buffer frame 
            bufPool[index] = file->readPage(pageNo); //read page
            hashTable->insert(file, pageNo, index); //insert page into hash table
            bufDescTable[index].Set(file, pageNo);
            page = &bufPool[index];
        }
    }

    /**
    * @param File pointer, constant PageId, constant bool 
    * @return none 
    * @purpose decrement pinCnt and set dirty bit
    */
    void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty) {
        try {
            FrameId index;
            //find the file and page number
            hashTable->lookup(file, pageNo, index);
            if (bufDescTable[index].pinCnt == 0) {
                throw PageNotPinnedException("PinCnt already 0", pageNo, index);
            }
            bufDescTable[index].pinCnt--;
            if (dirty) {
                bufDescTable[index].dirty = true;
            }
        }
        catch (HashNotFoundException e) {} //catch exception if the look up failed
    }

    /**
    * @param File pointer
    * @return none 
    * @purpose remove all pages from the file 
    */
    void BufMgr::flushFile(const File *file) {
        for (unsigned int i = 0; i < numBufs; ++i) {
            //check if the page is valid
            if (bufDescTable[i].file == file && bufDescTable[i].valid == true) {
                if (bufDescTable[i].pinCnt > 0) {
                    throw PagePinnedException("Pinned page", bufDescTable[i].pageNo, bufDescTable[i].frameNo);
                }
                //check the dirty bit
                if (bufDescTable[i].dirty) {
                    //flush page to dick
                    bufDescTable[i].file->writePage(bufPool[bufDescTable[i].frameNo]);
                    //reset dirty bit
                    bufDescTable[i].dirty = false;
                }
                //remove page from hashtable
                hashTable->remove(file, bufDescTable[i].pageNo);
                bufDescTable[i].Clear();
            } else {
                throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid,
                                         bufDescTable[i].refbit);  
            }
        }
    }

    /**
    * @param File pointer, PageId, Page reference
    * @return none 
    * @purpose allocate an empty page and return both page number and a pointer to the buffer frame
    */
    void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page) {
        //allocate a new page
        Page newPage = file->allocatePage();
        FrameId index;
        allocBuf(index); // obtain a buffer pool frame
        bufPool[index] = newPage;
        //insert entry into hash table
        hashTable->insert(file, newPage.page_number(), index);
        bufDescTable[index].Set(file, newPage.page_number());
        //return both page number and a pointer to the buffer frame
        pageNo = newPage.page_number(); 
        page = &bufPool[index];
    }

    /**
    * @param File pointer, constant PageId
    * @return none 
    * @purpose delete a page from file 
    */
    void BufMgr::disposePage(File *file, const PageId PageNo) {
        FrameId index;
        //delete a page from file 
        try {
            //check if the page is allocated to a frame in the buffer pool
            hashTable->lookup(file, PageNo, index);
            bufDescTable[index].Clear();
            hashTable->remove(file, PageNo);
        } catch (HashNotFoundException e) {

        }
        file->deletePage(PageNo);
    }

    /**
    * @param void
    * @return void 
    * @purpose print the total number of valid frames in bufDescTable
    */
    void BufMgr::printSelf(void) {
        BufDesc *tmpbuf;
        int validFrames = 0;

        for (std::uint32_t i = 0; i < numBufs; i++) {
            tmpbuf = &(bufDescTable[i]);
            std::cout << "FrameNo:" << i << " ";
            tmpbuf->Print();

            if (tmpbuf->valid == true)
                validFrames++;
        }

        std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }

}
