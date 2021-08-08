/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "btree.h"
#include <algorithm>
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/file_exists_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"


using namespace std;

namespace badgerdb {

/**
 * Allocation helper method
 */

/**
 * Allocate a page in the buffer
 *
 * @param newPageId the page number of the new node
 * @return a reference to the new internal node
 */
non_leaf_node_int *BTreeIndex::allocNonLeafNode(PageId &newPageId) {
  non_leaf_node_int *newNode;
  bufMgr->allocPage(file, newPageId, (Page *&)newNode);
  memset(newNode, 0, Page::SIZE);
  return newNode;
}

/**
 * Allocate a page in the buffer for a leaf node
 *
 * @param newPageId the page number for the new node
 * @return a reference to the new leaf node
 */
leaf_node_int *BTreeIndex::allocLeafNode(PageId &newPageId) {
  leaf_node_int *newNode = (leaf_node_int *)allocNonLeafNode(newPageId);
  newNode->level = -1;
  return newNode;
}

/**
 * This is the constructor of the btree. It checks if the specified index file exists.
 * If the index file exists, the file is opened, if the index file does not exist, a new
 * index file is created.
 *
 * @param relationName The name of the relation on which to build the index.
 * @param outIndexName The name of the index file.
 * @param bufMgrIn The instance of the global buffer manager.
 * @param attrByteOffset The byte offset of the attribute in the tuple on which
 * to build the index.
 * @param attrType The data type of the attribute we are indexing.
 */
BTreeIndex::BTreeIndex(const string &relationName, string &outIndexName,
                       BufMgr *bufMgrIn, const int attrByteOffset_,
                       const Datatype attrType) {
  bufMgr = bufMgrIn;
  attrByteOffset = attrByteOffset_;
  attributeType = attrType;

  ostringstream idx_str{};
  idx_str << relationName << ',' << attrByteOffset;
  outIndexName = idx_str.str();

  relationName.copy(indexMetaInfo.relationName, 20, 0);
  indexMetaInfo.attrByteOffset = attrByteOffset;
  indexMetaInfo.attrType = attrType;

  file = new BlobFile(outIndexName, true);

  allocLeafNode(indexMetaInfo.rootPageNo);
  bufMgr->unPinPage(file, indexMetaInfo.rootPageNo, true);

  FileScan fscan(relationName, bufMgr);
  try {
    RecordId scanRecordID;
    while (1) {
      fscan.scanNext(scanRecordID);
      std::string recordStr = fscan.getRecord();
      const char *record = recordStr.c_str();
      int key = *((int *)(record + attrByteOffset));
      insertEntry(&key, scanRecordID);
    }
  } catch (EndOfFileException e) {
  }
}

/**
 * This is the helper method that checks if the page stores a leaf node or
 * an internal node.
 *
 * @param page the page 
 * @return true if the page stores a leaf node
 *         false if the page stores an internal node
 */
bool BTreeIndex::isLeaf(Page *page) { return *((int *)page) == -1; }

/**
 * This is the helper method to checks and see that if an internal node is full
 *
 * @param node node
 * @return true if an internal node is full
 *         false if an internal node is not full
 */
bool BTreeIndex::isNonLeafFull(non_leaf_node_int *node) {
  return node->pageNoArray[INTARRAYNONLEAFSIZE] != 0;
}

/**
 * This is the helper method to checks that if a leaf node is full
 *
 * @param node a leaf node
 * @return true if a leaf node is full
 *         false if a leaf node is not full
 */
bool BTreeIndex::isLeafFull(leaf_node_int *node) {
  return !(node->ridArray[INTARRAYLEAFSIZE - 1].page_number == 0 &&
           node->ridArray[INTARRAYLEAFSIZE - 1].slot_number == 0);
}

/**
 * This is the helper method that returns the number of records stored in the leaf 
 * node. And we assume that all records are continuously stored and valid records
 * are nonzero.
 *
 * @param node a leaf node
 * @return the number of records stored in the leaf node
 */
int BTreeIndex::numInLeaf(leaf_node_int *node) {
  static auto comp = [](const RecordId &r1, const RecordId &r2) {
    return r1.page_number > r2.page_number;
  };
  static RecordId emptyRecord{};

  RecordId *start = node->ridArray;
  RecordId *end = &node->ridArray[INTARRAYLEAFSIZE];

  return lower_bound(start, end, emptyRecord, comp) - start;
}

/**
 * This is the helper method that returns the number of records stored in the internal 
 * node. And we assume that all records are continuously stored and valid records
 * are nonzero.
 *
 * @param node an internal node
 * @return the number of records stored in the internal node
 */
int BTreeIndex::numInNonLeaf(non_leaf_node_int *node) {
  static auto comp = [](const PageId &p1, const PageId &p2) { return p1 > p2; };
  PageId *start = node->pageNoArray;
  PageId *end = &node->pageNoArray[INTARRAYNONLEAFSIZE + 1];
  return lower_bound(start, end, 0, comp) - start;
}

/**
 * This is the helper method that find the index of the first integer larger than 
 * (or equal to) the given key.
 *
 * @param array an interger array
 * @param length the length of the array
 * @param key the target key
 * @param includeKey whether the current key is included
 *
 * @return a. the index of the first integer larger than the given key if
 *            includeKey = false
 *         b. the index of the first integer larger than or equal to the
 *            given key if includeKey = true
 *         c. -1 if the key is not found
 */
int BTreeIndex::findLargerInt(const int *array, int length, int key,
                               bool includeKey) {
  if (!includeKey) key++;
  int result = lower_bound(array, &array[length], key) - array;
  return result >= length ? -1 : result;
}

/**
 * This is the helper method that find the index of the first key smaller than the 
 * given key.
 *
 * @param node an internal node
 * @param key the key to find
 * @return the index of the first key smaller than the given key
 *         return the largest index if not found
 */
int BTreeIndex::findSmallerKeyIndex(non_leaf_node_int *node, int key) {
  int len = numInNonLeaf(node);
  int result = findLargerInt(node->keyArray, len - 1, key);
  return result == -1 ? len - 1 : result;
}

/**
 * This is the helper method that find the insertaion index for a key in a leaf node
 *
 * @param node a leaf node
 * @param key the key to be inserted
 * @return the insertaion index for a key in a leaf node
 */
int BTreeIndex::findInsertionIndexLeaf(leaf_node_int *node, int key) {
  int len = numInLeaf(node);
  int result = findLargerInt(node->keyArray, len, key);
  return result == -1 ? len : result;
}

/**
 * This is the helper method that find the index of the first key larger than the given 
 * key in the leaf node
 *
 * @param node a leaf node
 * @param key the key to find
 * @param includeKey whether the current key is included
 *
 * @return a. the index of the first integer larger than the given key if
 *            includeKey is false
 *         b. the index of the first integer larger than or equal to the
 *            given key if includeKey is true
 *         c. -1 if the key is not found
 */
int BTreeIndex::findIndexLeaf(leaf_node_int *node, int key, bool includeKey) {
  return findLargerInt(node->keyArray, numInLeaf(node), key, includeKey);
}

/**
 * This is the helper method that inserts the given pair into the leaf node at 
 * the given insertion index.
 *
 * @param node a leaf node
 * @param i  insertion index
 * @param key  key of pair to be inserted
 * @param rid the record ID of the pair to be inserted
 */
void BTreeIndex::insertionLeafNode(leaf_node_int *node, int i, int key,
                                  RecordId rid) {
  const size_t len = INTARRAYLEAFSIZE - i - 1;

  // shift items for the extra space
  memmove(&node->keyArray[i + 1], &node->keyArray[i], len * sizeof(int));
  memmove(&node->ridArray[i + 1], &node->ridArray[i], len * sizeof(RecordId));

  // save the key and record id to the leaf node
  node->keyArray[i] = key;
  node->ridArray[i] = rid;
}

/**
 * This is the helper method that inserts the given key-(page number) pair into 
 * the given leaf node at the given index.
 *
 * @param n an internal node
 * @param i  insertion index
 * @param key  key of the pair
 * @param pid  page number of the pair
 */
void BTreeIndex::insertionNonLeafNode(non_leaf_node_int *n, int i, int key,
                                     PageId pid) {
  const size_t len = INTARRAYNONLEAFSIZE - i - 1;

  // shift items for extra space
  memmove(&n->keyArray[i + 1], &n->keyArray[i], len * sizeof(int));
  memmove(&n->pageNoArray[i + 2], &n->pageNoArray[i + 1], len * sizeof(PageId));

  // store the key and page number to the node
  n->keyArray[i] = key;
  n->pageNoArray[i + 1] = pid;
}

/**
 * This is the helper method to splits a leaf node into two.
 *
 * @param node a pointer to the original node
 * @param newNode a pointer to the new node
 * @param index the index where the split occurs.
 */
void BTreeIndex::splitLeaf(leaf_node_int *node, leaf_node_int *newNode,
                               int index) {
  const size_t len = INTARRAYLEAFSIZE - index;

  // copy elements to new node
  memcpy(&newNode->keyArray, &node->keyArray[index], len * sizeof(int));
  memcpy(&newNode->ridArray, &node->ridArray[index], len * sizeof(RecordId));

  // remove elements from old
  memset(&node->keyArray[index], 0, len * sizeof(int));
  memset(&node->ridArray[index], 0, len * sizeof(RecordId));
}

/**
 * This is the helper method that splits the internal node by the given index.
 *
 * @param node an internal node
 * @param i the index where the split occurs.
 * @param keepKey if keepKey is true, then the pair at the index does not need 
 * to be moved up and will be moved to the newly created
 * internal node.
 * @return a pointer to the newly created internal node.
 */
void BTreeIndex::splitNonLeaf(non_leaf_node_int *curr, non_leaf_node_int *next,
                                  int i, bool keepKey) {
  size_t len = INTARRAYNONLEAFSIZE - i;

  // copy keys to new node
  if (keepKey)
    memcpy(&next->keyArray, &curr->keyArray[i], len * sizeof(int));
  else
    memcpy(&next->keyArray, &curr->keyArray[i + 1], (len - 1) * sizeof(int));

  // copy values to new node
  memcpy(&next->pageNoArray, &curr->pageNoArray[i + 1], len * sizeof(PageId));

  // remove elements from old node
  memset(&curr->keyArray[i], 0, len * sizeof(int));
  memset(&curr->pageNoArray[i + 1], 0, len * sizeof(PageId));
}

/**
 * Create a new root with midVal, pid1 and pid2.
 *
 * @param midVal the first middle value of the new root
 * @param pid1 the first page number in the new root
 * @param pid2 the second page number in the new root
 *
 * @return the page id of the new root
 */
PageId BTreeIndex::splitRootNode(int midVal, PageId pid1, PageId pid2) {
  // alloc a new page for root
  PageId newRoot_pageID;
  non_leaf_node_int *newRoot = allocNonLeafNode(newRoot_pageID);

  // set key and page numbers
  newRoot->keyArray[0] = midVal;
  newRoot->pageNoArray[0] = pid1;
  newRoot->pageNoArray[1] = pid2;

  // unpin the root page
  bufMgr->unPinPage(file, newRoot_pageID, true);

  return newRoot_pageID;
}

/**
 * This is the helper method that inserts the given pair into the given leaf node.
 *
 * @param originalNode a leaf node
 * @param originalPage the page id of the page that stores the leaf node
 * @param key the key of the pair
 * @param rid the record id of the pair
 * @param midVal a reference to an integer in the parent node.
 * @return The page number of the newly created page.
 */
PageId BTreeIndex::insertToLeafPage(Page *origPage, PageId originalPage, int key,
                                    RecordId rid, int &midVal) {
  leaf_node_int *originalNode = (leaf_node_int *)origPage;

  // finde the insertion index
  int index = findInsertionIndexLeaf(originalNode, key);

  // if is not full, insert the key and record id
  if (!isLeafFull(originalNode)) {
    insertionLeafNode(originalNode, index, key, rid);
    bufMgr->unPinPage(file, originalPage, true);
    return 0;
  }

  // get the middle index for spliting the page
  const int midIndex = INTARRAYLEAFSIZE / 2;

  // find out whether the new element is insert to the left of the old node
  bool insertLeft = index < midIndex;

  // allocate a page for the new node
  PageId newPageId;
  leaf_node_int *newNode = allocLeafNode(newPageId);

  // split the node to originalNode and newNode
  splitLeaf(originalNode, newNode, midIndex + insertLeft);

  // insert the key and record id
  if (insertLeft)
    insertionLeafNode(originalNode, index, key, rid);
  else
    insertionLeafNode(newNode, index - midIndex, key, rid);

  // set the next page id
  newNode->rightSibPageNo = originalNode->rightSibPageNo;
  originalNode->rightSibPageNo = newPageId;

  // unpin the new node and the original node
  bufMgr->unPinPage(file, originalPage, true);
  bufMgr->unPinPage(file, newPageId, true);

  // set the middle value
  midVal = newNode->keyArray[0];
  return newPageId;
}

/**
 * This is the helper method that recursively insert the given pair into the 
 * subtree with the given root node.
 *
 * @param originalPage page id of the page 
 * @param key the key of the pair to be inserted
 * @param rid the record ID of the key-record pair to be inserted
 * @param midVal a pointer to an integer value to be stored in the parent node.
 * @return the page number of the newly created node if a split occurs, or 0
 *         otherwise.
 */
PageId BTreeIndex::insert(PageId originalPage, int key, RecordId rid,
                          int &midVal) {
  Page *origPage;
  bufMgr->readPage(file, originalPage, origPage);

  if (isLeaf(origPage)) 
    return insertToLeafPage(origPage, originalPage, key, rid, midVal);

  non_leaf_node_int *originalNode = (non_leaf_node_int *)origPage;

  // find the page id
  int origChildPageIndex = findSmallerKeyIndex(originalNode, key);
  PageId origChildPageId = originalNode->pageNoArray[origChildPageIndex];

  // insert key
  int newChildMidVal;
  PageId newChildPageId = insert(origChildPageId, key, rid, newChildMidVal);

  // not split in child
  if (newChildPageId == 0) {
    bufMgr->unPinPage(file, originalPage, false);
    return 0;
  }

  // add splitted child to currNode
  int index = findSmallerKeyIndex(originalNode, newChildMidVal);
  if (!isNonLeafFull(originalNode)) {  // current node is not full
    insertionNonLeafNode(originalNode, index, newChildMidVal, newChildPageId);
    bufMgr->unPinPage(file, originalPage, true);
    return 0;
  }

  // the middle index for spliting the page
  int midIndex = (INTARRAYNONLEAFSIZE - 1) / 2;

  // check to see that whether the new element is insert to the left half of the original node
  bool insertLeft = index < midIndex;

  // split
  int splitIndex = midIndex + insertLeft;
  int insertIndex = insertLeft ? index : index - midIndex;

  // insert to right
  bool moveKeyUp = !insertLeft && insertIndex == 0;

  // if we need to move key up, set midVal = key, else key at splited index
  midVal = moveKeyUp ? newChildMidVal : originalNode->keyArray[splitIndex];

  // alloc a page for the new node
  PageId newPageId;
  non_leaf_node_int *newNode = allocNonLeafNode(newPageId);

  // split the node to originalNode and newNode
  splitNonLeaf(originalNode, newNode, splitIndex, moveKeyUp);

  // need to insert
  if (!moveKeyUp) {
    non_leaf_node_int *node = insertLeft ? originalNode : newNode;
    insertionNonLeafNode(node, insertIndex, newChildMidVal, newChildPageId);
  }

  // write the page back
  bufMgr->unPinPage(file, originalPage, true);
  bufMgr->unPinPage(file, newPageId, true);

  // return new page
  return newPageId;
}

/**
 * Insert a new entry using the pair <value,rid>.
 * Start from root to recursively find out the leaf to insert the entry in.
 * The insertion may cause splitting of leaf node. This splitting will require
 * addition of new leaf page number entry into the parent non-leaf, which may
 * in-turn get split. This may continue all the way upto the root causing the
 * root to get split. If root gets split, metapage needs to be changed
 * accordingly. Make sure to unpin pages as soon as you can.
 * @param key			Key to insert, pointer to integer/double/char
 *string
 * @param rid			Record ID of a record whose entry is getting
 *inserted into the index.
 **/
const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
  int midval;
  PageId pid = insert(indexMetaInfo.rootPageNo, *(int *)key, rid, midval);

  if (pid != 0)
    indexMetaInfo.rootPageNo = splitRootNode(midval, indexMetaInfo.rootPageNo, pid);
}

/**
 * This is the helper method that changes the currently scanning page to the next page pointed 
 * to by the current page.
 * @param node the node stored in the currently scanning page.
 */
void BTreeIndex::moveToNext(leaf_node_int *node) {
  bufMgr->unPinPage(file, currentPageNum, false);
  currentPageNum = node->rightSibPageNo;
  bufMgr->readPage(file, currentPageNum, currentPageData);
  nextEntry = 0;
}

/**
 * This is the helper method that recursively find the page id of the first element larger than 
 * or equal to the lower bound given.
 */
void BTreeIndex::setPageScan() {
  bufMgr->readPage(file, currentPageNum, currentPageData);
  if (isLeaf(currentPageData)) return;

  non_leaf_node_int *node = (non_leaf_node_int *)currentPageData;

  bufMgr->unPinPage(file, currentPageNum, false);
  currentPageNum = node->pageNoArray[findSmallerKeyIndex(node, lowValInt)];
  setPageScan();
}

/**
 * This is the helper method that finds the first element in the currently scanning page that 
 * is within the given bound.
 */
void BTreeIndex::entryScanIndex() {
  leaf_node_int *node = (leaf_node_int *)currentPageData;
  int entryIndex = findIndexLeaf(node, lowValInt, lowOp == GTE);
  if (entryIndex == -1)
    moveToNext(node);
  else
    nextEntry = entryIndex;
}

/**
 *
 * This method is used to begin a filtered scan” of the index.
 *
 * For example, if the method is called using arguments (1,GT,100,LTE), then
 * the scan should seek all entries greater than 1 and less than or equal to
 * 100.
 *
 * @param lowValParm The low value to be tested.
 * @param lowOpParm The operation to be used in testing the low range.
 * @param highValParm The high value to be tested.
 * @param highOpParm The operation to be used in testing the high range.
 */
const void BTreeIndex::startScan(const void *lowValParm,
                                 const Operator lowOpParm,
                                 const void *highValParm,
                                 const Operator highOpParm) {
  if (lowOpParm != GT && lowOpParm != GTE) throw BadOpcodesException();
  if (highOpParm != LT && highOpParm != LTE) throw BadOpcodesException();

  lowValInt = *((int *)lowValParm);
  highValInt = *((int *)highValParm);
  if (lowValInt > highValInt) throw BadScanrangeException();

  lowOp = lowOpParm;
  highOp = highOpParm;

  scanExecuting = true;

  currentPageNum = indexMetaInfo.rootPageNo;

  setPageScan();
  entryScanIndex();

  leaf_node_int *node = (leaf_node_int *)currentPageData;
  RecordId outRid = node->ridArray[nextEntry];
  if ((outRid.page_number == 0 && outRid.slot_number == 0) ||
      node->keyArray[nextEntry] > highValInt ||
      (node->keyArray[nextEntry] == highValInt && highOp == LT)) {
    endScan();
    throw NoSuchKeyFoundException();
  }
}

/**
 * This is the helper method that continuely scanning the next entry.
 */
void BTreeIndex::setNextEntry() {
  nextEntry++;
  leaf_node_int *node = (leaf_node_int *)currentPageData;
  if (nextEntry >= INTARRAYLEAFSIZE ||
      node->ridArray[nextEntry].page_number == 0) {
    moveToNext(node);
  }
}

/**
 * This method fetches the record id of the next tuple that matches the scan
 * criteria. If the scan has reached the end, then it should throw the
 * following exception: IndexScanCompletedException.
 *
 * @param outRid An output value
 * @throws ScanNotInitializedException If no scan has been initialized.
 * @throws IndexScanCompletedException If no more records, satisfying the scan 
 * criteria, are left to be scanned.
 */
const void BTreeIndex::scanNext(RecordId &outRid) {
  if (!scanExecuting) throw ScanNotInitializedException();

  leaf_node_int *node = (leaf_node_int *)currentPageData;
  outRid = node->ridArray[nextEntry];
  int val = node->keyArray[nextEntry];

  if ((outRid.page_number == 0 &&
       outRid.slot_number == 0) ||            // empty record ID is empty
      val > highValInt ||                     // value is out of range
      (val == highValInt && highOp == LT)) {  // value reaches the higher end
    throw IndexScanCompletedException();
  }
  setNextEntry();
}

/**
 * This method terminates the current scan and unpins all the pages that have
 * been pinned for the purpose of the scan.
 *
 * @throws ScanNotInitializedException If no scan has been initialized.
 */
const void BTreeIndex::endScan() {
  if (!scanExecuting) throw ScanNotInitializedException();
  scanExecuting = false;
  bufMgr->unPinPage(file, currentPageNum, false);
}

  /**
   * BTreeIndex Destructor. 
   * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
   * and delete file instance thereby closing the index file.
   * Destructor should not throw any exceptions. All exceptions should be caught in here itself. 
   * */
BTreeIndex::~BTreeIndex() {
  if (scanExecuting) endScan();
  bufMgr->flushFile(file);
  delete file;
}

} 