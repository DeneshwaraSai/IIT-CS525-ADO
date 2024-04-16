RC insertRecord (RM_TableData *rel, Record *record) {
	int recordSize = getRecordSize(rel->schema);
	char *data, *inSlotPtr;
	RecordManagement *recordManagement = rel->mgmtData;
	RID *rids = &record->id;

	rids->page = recordManagement->freeCount;
	pinPage(&recordManagement->buffer, &recordManagement->pHandler, rids->page);

	data = recordManagement->pHandler.data;
	rids->slot = findFreeSlot(data, recordSize);

	while(rids->slot == -1) {
		unpinPage(&recordManagement->buffer, &recordManagement->pHandler);
		rids->page++;

		pinPage(&recordManagement->buffer, &recordManagement->pHandler, rids->page);
		data = recordManagement->pHandler.data;

		rids->slot = findFreeSlot(data, recordSize);
	}

	inSlotPtr = data;

	markDirty(&recordManagement->buffer, &recordManagement->pHandler);

	inSlotPtr = inSlotPtr + (rids->slot * recordSize);
	*inSlotPtr = '+';

	memcpy(++inSlotPtr, record->data + 1, recordSize - 1);

	unpinPage(&recordManagement->buffer, &recordManagement->pHandler);
	recordManagement->tupleCount++;
	pinPage(&recordManagement->buffer, &recordManagement->pHandler, 0);

	return RC_OK;
}