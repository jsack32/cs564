#include "catalog.h"
#include "query.h"
#include <stdlib.h>
#include "stdio.h"


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
 * 	OK on success
 * 	an error code otherwise
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
    
    //go through the projection list and look up each in attrcat 
    //to get an AttrDesc structure (for offset, length)
    AttrDesc attrDesc[projCnt];
    for (int i=0; i<projCnt; i++){
	status = attrCat->getInfo(projNames[i].relName,
			          projNames[i].attrName,
			  	  attrDesc[i]);
	if(status != OK) return status;
    }
    //get output record length from attrDesc structures
    int reclen = 0;
    for (int i=0;i<projCnt; i++){
	reclen = reclen + attrDesc[i].attrLen;
    }
    if(attr == NULL) {
	AttrDesc attrDesc1;
	strcpy(attrDesc1.relName, projNames[0].relName);

	attrDesc1.attrName[0] = NULL;
	attrDesc1.attrOffset = 0;
	attrDesc1.attrLen = 0;
	attrDesc1.attrType=STRING;

         
    	status = ScanSelect(result,
			projCnt,
			attrDesc,
			&attrDesc1,
			EQ,
			NULL,
			reclen);
       if(status != OK) return status;
    } else {
    
	    //get AtrDesc structure for the first selection attrbute
	    AttrDesc attrDesc1;
	    status = attrCat->getInfo(attr->relName,
				      attr->attrName,
				      attrDesc1);
	    if(status != OK) return status;
	
	    const char* value;
	    if(((Datatype)attrDesc1.attrType) == INTEGER){
        	int temp = atoi(attrValue);
		int *val = &temp;
		value = (char*) val;
	    }
	    else if ((Datatype)attrDesc1.attrType == FLOAT){
		float fTemp  = (float)atof(attrValue);
		float *fval = &fTemp;
		value = (char*)fval;
    	    }	
    	    else
		value = attrValue;	
    
    	status = ScanSelect(result,
				projCnt,
				attrDesc,
				&attrDesc1,
				op,
				value,
				reclen);
       if(status != OK) return status;
   }
   return OK;			      
}


const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    Status status;
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
    InsertFileScan resultRel(result, status);
    if(status != OK) return status;

    HeapFileScan hfs((string)attrDesc->relName, status);
    if(status != OK) return status;
    status = hfs.startScan(attrDesc->attrOffset,
			   attrDesc->attrLen,
			   (Datatype) attrDesc->attrType,
			   filter,
			   op);

    if(status != OK) return status;
    char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *)outputData;
    outputRec.length = reclen;
    RID rid;
    while(hfs.scanNext(rid) == OK){
	Record rec;
	status = hfs.getRecord(rec);
	if(status != OK) return status;
	int outputOffset = 0;
	for(int i=0 ; i<projCnt; i++){
		memcpy(outputData+outputOffset, (char*)rec.data +projNames[i].attrOffset, projNames[i].attrLen);
		outputOffset = outputOffset + projNames[i].attrLen;
	}
	
	RID outRid;
        status = resultRel.insertRecord(outputRec, outRid);
	if(status != OK) return status;	
    }
    return OK;   
}