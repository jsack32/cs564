#include "catalog.h"
#include "error.h"
#include "heapfile.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status status;
RID rid;
HeapFileScan *heapFileScan;

AttrDesc attrDesc;
void* filter;
int i;
float f;

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
	// when relation is empty, reject the deletion
	if (relation.empty()) {
		return BADCATPARM;
	}

	// initialize heapFileScan
	heapFileScan = new HeapFileScan(relation, status);
	if (status != OK) {
		return status;
	}

	// if search condition is specified, find the attribute info and start scan
	if (attrName != "") {
		switch (type) {
			case STRING:
				filter = (void*)attrValue;
				break;

			case INTEGER:
				i = atoi(attrValue);
				filter = &i;
				break;

			case FLOAT:
				f = atof(attrValue);
				filter = &f;
				break;
		}

		status = attrCat -> getInfo(relation, attrName, attrDesc);
		if (status != OK) {
			return status;
		}
		status = heapFileScan -> startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char*)filter, op);
	}
	// no search condition specified
	else {
		status = heapFileScan -> startScan(0, 0, type, NULL, op);
	}

	if (status != OK) {
		return status;
	}

	// search for records matching the condition
	for (;;) {
		status = heapFileScan -> scanNext(rid);

		// break when you reach the end of the file
		if (status == FILEEOF) {
			break;
		}
		else if (status != OK) {
			return status;
		}

		// delete the current record found by the scan
		heapFileScan -> deleteRecord();
	}

	delete heapFileScan;
	return OK;
}


