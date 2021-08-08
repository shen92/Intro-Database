/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#pragma once

#include <iostream>
#include <sstream>
#include <string>
#include "string.h"

#include "buffer.h"
#include "file.h"
#include "page.h"
#include "types.h"

namespace badgerdb {

/**
 * @brief Datatype enumeration type.
 */
enum Datatype { INTEGER = 0, DOUBLE = 1, STRING = 2 };

/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
enum Operator {
  LT,  /* Less Than */
  LTE, /* Less Than or Equal to */
  GTE, /* Greater Than or Equal to */
  GT   /* Greater Than */
};

/**
 * @brief Number of key slots in B+Tree leaf for INTEGER key.
 */
//                                                  sibling ptr             key
//                                                  rid
const int INTARRAYLEAFSIZE =
    (Page::SIZE - sizeof(PageId)) / (sizeof(int) + sizeof(RecordId));

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                     level     extra pageNo
//                                                     key       pageNo
const int INTARRAYNONLEAFSIZE = (Page::SIZE - sizeof(int) - sizeof(PageId)) /
                                (sizeof(int) + sizeof(PageId));

/**
 * @brief The meta page, which holds metadata for Index file, is always first
 * page of the btree index file and is cast to the following structure to store
 * or retrieve information from it. Contains the relation name for which the
 * index is created, the byte offset of the key value on which the index is
 * made, the type of the key and the page no of the root page. Root page starts
 * as page 2 but since a split can occur at the root the root page may get moved
 * up and get a new page no.
 */
struct IndexMetaInfo {
  /**
   * Name of base relation.
   */
  char relationName[20];

  /**
   * Offset of attribute, over which index is built, inside the record stored in
   * pages.
   */
  int attrByteOffset;

  /**
   * Type of the attribute over which index is built.
   */
  Datatype attrType;

  /**
   * Page number of root page of the B+ Tree inside the file index file.
   */
  PageId rootPageNo;
};

/*
Each node is a page, so once we read the page in we just cast the pointer to the
page to this struct and use it to access the parts These structures basically
are the format in which the information is stored in the pages for the index
file depending on what kind of node they are. The level memeber of each non leaf
structure seen below is set to 1 if the nodes at this level are just above the
leaf nodes. Otherwise set to 0.
*/

/**
 * @brief Structure for all non-leaf nodes when the key is of INTEGER type.
 */
struct non_leaf_node_int {
  /**
   * Level of the node in the tree.
   */
  int level = 0;

  /**
   * Stores keys.
   */
  int keyArray[INTARRAYNONLEAFSIZE]{};

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf
   * nodes in the tree.
   */
  PageId pageNoArray[INTARRAYNONLEAFSIZE + 1]{};
};

/**
 * @brief Structure for all leaf nodes when the key is of INTEGER type.
 */

struct leaf_node_int {
  int level = -1;

  /**
   * Stores keys.
   */
  int keyArray[INTARRAYLEAFSIZE]{};

  /**
   * Stores RecordIds.
   */
  RecordId ridArray[INTARRAYLEAFSIZE]{};

  /**
   * Page number of the leaf on the right side.
   * This linking of leaves allows to easily move from one leaf to the next leaf
   * during index scan.
   */
  PageId rightSibPageNo = 0;
};

/**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single attribute
 * of a relation. This index supports only one scan at a time.
 */
class BTreeIndex {
 private:
  /**
   * File object for the index file.
   */
  File *file{};

  /**
   * Buffer Manager Instance.
   */
  BufMgr *bufMgr{};

  /**
   * Datatype of attribute over which index is built.
   */
  Datatype attributeType;

  /**
   * Offset of attribute, over which index is built, inside records.
   */
  int attrByteOffset{};

  // MEMBERS SPECIFIC TO SCANNING

  /**
   * True if an index scan has been started.
   */
  bool scanExecuting{};

  /**
   * Index of next entry to be scanned in current leaf being scanned.
   */
  int nextEntry{};

  /**
   * Page number of current page being scanned.
   */
  PageId currentPageNum{};

  /**
   * Current Page being scanned.
   */
  Page *currentPageData{};

  /**
   * Low INTEGER value for scan.
   */
  int lowValInt{};

  /**
   * High INTEGER value for scan.
   */
  int highValInt{};

  /**
   * Low Operator. Can only be GT(>) or GTE(>=).
   */
  Operator lowOp{GT};

  /**
   * High Operator. Can only be LT(<) or LTE(<=).
   */
  Operator highOp{LT};

  struct IndexMetaInfo indexMetaInfo {};

  /**
   * Alloc a page in the buffer for a leaf node
   *
   * @param newPageId the page number for the new node
   * @return a pointer to the new leaf node
   */
  leaf_node_int *allocLeafNode(PageId &newPageId);
 
 /**
  * Allocate a page in the buffer
  *
  * @param newPageId the page number of the new node
  * @return a reference to the new internal node
  */
  non_leaf_node_int *allocNonLeafNode(PageId &newPageId);

  /**
  * This is the helper method that checks if the page stores a leaf node or
  * an internal node.
  *
  * @param page the page 
  * @return true if the page stores a leaf node
  *         false if the page stores an internal node
  */
  bool isLeaf(Page *page);

  /**
  * This is the helper method to checks and see that if an internal node is full
  *
  * @param node node
  * @return true if an internal node is full
  *         false if an internal node is not full
  */
  bool isNonLeafFull(non_leaf_node_int *node);

  /**
  * This is the helper method to checks that if a leaf node is full
  *
  * @param node a leaf node
  * @return true if a leaf node is full
  *         false if a leaf node is not full
  */
  bool isLeafFull(leaf_node_int *node);

 /**
  * This is the helper method that returns the number of records stored in the leaf 
  * node. And we assume that all records are continuously stored and valid records
  * are nonzero.
  *
  * @param node a leaf node
  * @return the number of records stored in the leaf node
  */
  int numInLeaf(leaf_node_int *node);

  /**
  * This is the helper method that returns the number of records stored in the internal 
  * node. And we assume that all records are continuously stored and valid records
  * are nonzero.
  *
  * @param node an internal node
  * @return the number of records stored in the internal node
  */
  int numInNonLeaf(non_leaf_node_int *node);

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
  int findLargerInt(const int *array, int length, int key, bool includeKey = true);

  /**
  * This is the helper method that find the index of the first key smaller than the 
  * given key.
  *
  * @param node an internal node
  * @param key the key to find
  * @return the index of the first key smaller than the given key
  *         return the largest index if not found
  */
  int findSmallerKeyIndex(non_leaf_node_int *node, int key);

  /**
  * This is the helper method that find the insertaion index for a key in a leaf node
  *
  * @param node a leaf node
  * @param key the key to be inserted
  * @return the insertaion index for a key in a leaf node
  */
  int findInsertionIndexLeaf(leaf_node_int *node, int key);

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
  int findIndexLeaf(leaf_node_int *node, int key, bool includeKey);

  /**
  * This is the helper method that inserts the given pair into the leaf node at 
  * the given insertion index.
  *
  * @param node a leaf node
  * @param i  insertion index
  * @param key  key of pair to be inserted
  * @param rid the record ID of the pair to be inserted
  */
  void insertionLeafNode(leaf_node_int *node, int i, int key, RecordId rid);

  /**
  * This is the helper method that inserts the given key-(page number) pair into 
  * the given leaf node at the given index.
  *
  * @param n an internal node
  * @param i  insertion index
  * @param key  key of the pair
  * @param pid  page number of the pair
  */
  void insertionNonLeafNode(non_leaf_node_int *n, int i, int key, PageId pid);

  /**
  * This is the helper method to splits a leaf node into two.
  *
  * @param node a pointer to the original node
  * @param newNode a pointer to the new node
  * @param index the index where the split occurs.
  */
  void splitLeaf(leaf_node_int *node, leaf_node_int *newNode, int index);

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
  void splitNonLeaf(non_leaf_node_int *curr, non_leaf_node_int *next, int i,
                        bool keepMidKey);

 /**
  * Create a new root with midVal, pid1 and pid2.
  *
  * @param midVal the first middle value of the new root
  * @param pid1 the first page number in the new root
  * @param pid2 the second page number in the new root
  *
  * @return the page id of the new root
  */
  PageId splitRootNode(int midVal, PageId pid1, PageId pid2);

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
  PageId insertToLeafPage(Page *origPage, PageId originalPage, int key,
                          RecordId rid, int &midVal);

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
  PageId insert(PageId originalPage, int key, RecordId rid, int &midVal);

 /**
  * This is the helper method that changes the currently scanning page to the next page pointed 
  * to by the current page.
  * @param node the node stored in the currently scanning page.
  */
  void moveToNext(leaf_node_int *node);

 /**
  * This is the helper method that recursively find the page id of the first element larger than 
  * or equal to the lower bound given.
  */
  void setPageScan();

 /**
  * This is the helper method that finds the first element in the currently scanning page that 
  * is within the given bound.
  */
  void entryScanIndex();

 /**
  * This is the helper method that continuely scanning the next entry.
  */
  void setNextEntry();

 public:
  /**
   * BTreeIndex Constructor.
   * Check to see if the corresponding index file exists. If so, open the
   * file. If not, create it and insert entries for every tuple in the base
   * relation using FileScan class.
   *
   * @param relationName        Name of file.
   * @param outIndexName        Return the name of index file.
   * @param bufMgrIn						Buffer
   * Manager Instance
   * @param attrByteOffset			Offset of attribute, over which
   * index is to be built, in the record
   * @param attrType						Datatype
   * of attribute over which index is built
   * @throws  BadIndexInfoException     If the index file already exists for
   * the corresponding attribute, but values in metapage(relationName,
   * attribute byte offset, attribute type etc.) do not match with values
   * received through constructor parameters.
   */
  BTreeIndex(const std::string &relationName, std::string &outIndexName,
             BufMgr *bufMgrIn, const int attrByteOffset,
             const Datatype attrType);

  /**
   * BTreeIndex Destructor.
   * End any initialized scan, flush index file, after unpinning any pinned
   * pages, from the buffer manager and delete file instance thereby closing the
   * index file. Destructor should not throw any exceptions. All exceptions
   * should be caught in here itself.
   * */
  ~BTreeIndex();

  /**
   * Insert a new entry using the pair <value,rid>.
   * Start from root to recursively find out the leaf to insert the entry in.
   *The insertion may cause splitting of leaf node. This splitting will require
   *addition of new leaf page number entry into the parent non-leaf, which may
   *in-turn get split. This may continue all the way upto the root causing the
   *root to get split. If root gets split, metapage needs to be changed
   *accordingly. Make sure to unpin pages as soon as you can.
   * @param key			Key to insert, pointer to integer/double/char
   *string
   * @param rid			Record ID of a record whose entry is getting
   *inserted into the index.
   **/
  const void insertEntry(const void *key, const RecordId rid);

  /**
   * Begin a filtered scan of the index.  For instance, if the method is called
   * using ("a",GT,"d",LTE) then we should seek all entries with a value
   * greater than "a" and less than or equal to "d".
   * If another scan is already executing, that needs to be ended here.
   * Set up all the variables for scan. Start from root to find out the leaf
   *page that contains the first RecordID that satisfies the scan parameters.
   *Keep that page pinned in the buffer pool.
   * @param lowVal	Low value of range, pointer to integer / double / char
   *string
   * @param lowOp		Low operator (GT/GTE)
   * @param highVal	High value of range, pointer to integer / double / char
   *string
   * @param highOp	High operator (LT/LTE)
   * @throws  BadOpcodesException If lowOp and highOp do not contain one of
   *their their expected values
   * @throws  BadScanrangeException If lowVal > highval
   * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that
   *satisfies the scan criteria.
   **/
  const void startScan(const void *lowVal, const Operator lowOp,
                       const void *highVal, const Operator highOp);

  /**
   * Fetch the record id of the next index entry that matches the scan.
   * Return the next record from current page being scanned. If current page has
   *been scanned to its entirety, move on to the right sibling of current page,
   *if any exists, to start scanning that page. Make sure to unpin any pages
   *that are no longer required.
   * @param outRid	RecordId of next record found that satisfies the scan
   *criteria returned in this
   * @throws ScanNotInitializedException If no scan has been initialized.
   * @throws IndexScanCompletedException If no more records, satisfying the scan
   *criteria, are left to be scanned.
   **/
  const void scanNext(RecordId &outRid);  // returned record id

  /**
   * Terminate the current scan. Unpin any pinned pages. Reset scan specific
   *variables.
   * @throws ScanNotInitializedException If no scan has been initialized.
   **/
  const void endScan();
};
}  // namespace badgerdb
