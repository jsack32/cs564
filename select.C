#include "catalog.h"
#include "query.h"

// forward declaration
const Status ScanSelect(const string & result, 
                        const int projCnt, 
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc, 
                        const Operator op, 
                        const char *filter,
                        const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */

const Status QU_Select(const string & result, 
                       const int projCnt, 
                       const attrInfo projNames[],
                       const attrInfo *attr, 
                       const Operator op, 
                       const char *attrValue)
{
	// Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
    Status status;

    //get projection attributes
    AttrDesc attrDesc[projCnt];
    int reclen = 0;
    for (int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, attrDesc[i]);
        if (status != OK) return status;
        reclen += attrDesc[i].attrLen; //calculate output record length
    }

    //handle unconditional selection (no filter attribute)
    if (attr == NULL) {
        AttrDesc dummyAttrDesc; //placeholder for unconditional scans 
        strcpy(dummyAttrDesc.relName, projNames[0].relName);
        dummyAttrDesc.attrName[0] = NULL;
        dummyAttrDesc.attrOffset = 0;
        dummyAttrDesc.attrLen = 0;
        dummyAttrDesc.attrType = STRING;

        return ScanSelect(result, projCnt, attrDesc, &dummyAttrDesc, EQ, NULL, reclen);
    }

    //conditional selection (with filter attribute)
    AttrDesc filterAttrDesc;
    status = attrCat->getInfo(attr->relName, attr->attrName, filterAttrDesc);
    if (status != OK) return status;

    //convert attrValue to correct data type for filtering
    const char *value = attrValue; //default is string
    int intVal;
    float floatVal;

    if (filterAttrDesc.attrType == INTEGER) {
        intVal = atoi(attrValue);
        value = (char *)&intVal;
    } else if (filterAttrDesc.attrType == FLOAT) {
        floatVal = (float)atof(attrValue);
        value = (char *)&floatVal;
    }

    return ScanSelect(result, projCnt, attrDesc, &filterAttrDesc, op, value, reclen);
}

const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
                        const int projCnt, 
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc, 
                        const Operator op, 
                        const char *filter,
                        const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	Status status;
    InsertFileScan resultRel(result, status);
    if (status != OK) return status;

    HeapFileScan hfs(attrDesc->relName, status);
    if (status != OK) return status;

    status = hfs.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
    if (status != OK) return status;

    char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *)outputData;
    outputRec.length = reclen;
    RID rid;

    //loop through records that match
    while (hfs.scanNext(rid) == OK) {
        Record rec;
        status = hfs.getRecord(rec);
        if (status != OK) return status;

        //project attributes into result record
        int outputOffset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy(outputData + outputOffset, 
                   (char *)rec.data + projNames[i].attrOffset, 
                   projNames[i].attrLen);
            outputOffset += projNames[i].attrLen;
        }

        //insert result record
        RID outputRid;
        status = resultRel.insertRecord(outputRec, outputRid);
        if (status != OK) return status;
    }

    return OK;
}
