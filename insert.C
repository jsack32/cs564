#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, const int attrCnt, const attrInfo attrList[])
{
// part 6

	Record rec;
	Status status;
	RelDesc rd;
  	AttrDesc* list_attr;		// Array of attribute descriptions, returned by getRealInfo()
	int length_list_attr = 0;	// Length of the above array
	int attr_int = 0;
	int rec_data_index = 0;
	float attr_float;
	RID insertRID;
	void* rearrange_attr;

	InsertFileScan filescan(relation, status);


	// make sure there are no nulls in attrList
	for (int i = 0; i < attrCnt; i++) {

		if (attrList[i].attrValue == NULL) return ATTRNOTFOUND; // We assume this error means attribute is null, but it is not specified in the documentation

	}

	// get the description of the relation from the relation name
	status = relCat->getInfo(relation, rd);
	if (status != OK) return status;

	// get the attributes from the relation
	status = attrCat->getRelInfo(relation, length_list_attr, list_attr);
	if (status != OK) return status;

	// get the record length from all the attributes
	rec.length = 0;
	for (int i = 0; i < attrCnt; i++) {
		rec.length += list_attr[i].attrLen;
	}

	// create space for the rec data
	char rec_data[rec.length];
	rec.data = &rec_data;

	// sort/rearrange the attributes in attrList to match list_attr.
	for (int i = 0; i < length_list_attr; i++) {		// Go through all attributes in list_attr
		for (int j = 0; j < attrCnt; j++) {				// Go through all attributes in attrList

			if (strcmp(list_attr[i].attrName, attrList[j].attrName) == 0) {		// once matching attributes are found, copy into rec data

				if (list_attr[i].attrType == 1) {		// integer type

					attr_int = atoi((char*)attrList[j].attrValue);		
					rearrange_attr = &attr_int;		

				} else if (list_attr[i].attrType == 2) {// float type

					attr_float = atof((char*)attrList[j].attrValue);
					rearrange_attr = &attr_float;
				} else {								// string type
					rearrange_attr = attrList[j].attrValue;
				}


				// Copy the value into rec data
				memcpy(&rec_data[rec_data_index], rearrange_attr, list_attr[i].attrLen);
				rec_data_index += list_attr[i].attrLen;		// go to next available index
			}
		}
	}
	


	// actually insert the record
	status = filescan.insertRecord(rec, insertRID);
	if (status != OK) return status;


	// Nothing wrong yay

return OK;
}

