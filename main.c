/* 
 * File:   main.c
 * Author: root
 *
 * Created on 24 September, 2013, 9:55 AM
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <sys/fcntl.h>
#include <sys/types.h>

#include "globals.h"
#include "parseStructs.h"
#include "vmalloc.h"
#include "hashTable.h"
#include "pcm_ptlsim.h"
#include "sorting.h"
#include "constants.h"

#define FILTERED_S_SUPPKEY_COUNT 4
UINT32 scratchMemoryLeftCount;
UINT32 scratchMemoryRightCount;
UINT32 scratchMemoryOutCount;

char *scratchMemoryLeft;
char *scratchMemoryRight;
char *scratchMemoryOut;
char *pcmMemory;
int filtered_s_suppkey[FILTERED_S_SUPPKEY_COUNT];

Vmalloc_t *vmPCM; 
typedef struct projectedPartItem
{
	UINT32 p_partkey;
	char p_brand[11];
	char p_type[26];
	UINT32 p_size;
	
}projectedPartItem;

typedef struct projectedPartsuppItem
{
	UINT32 ps_partkey;
	UINT32 ps_suppkey;
	UINT32 ps_availqty;
	float ps_supplycost;
	char ps_comment[200];
}projectedPartsuppItem;

int scanAndFilterPartTable(){
    part pitem;
    int fpIn = open(PART_TABLE_FILE, O_RDONLY);
    projectedPartItem *pitemScratch = (projectedPartItem*)scratchMemoryLeft;
    scratchMemoryLeftCount = 0;
    while(read(fpIn, &pitem, sizeof(pitem)* 1) != 0){
        if( strncmp(pitem.p_brand, "Brand#35", 8) != 0 &&
                strstr(pitem.p_type, "ECONOMY BURNISHED") == NULL &&
                (pitem.p_size == 14 ||
                pitem.p_size == 7 ||
                pitem.p_size == 21 ||
                pitem.p_size == 24 ||
                pitem.p_size == 35 ||
                pitem.p_size == 33 ||
                pitem.p_size == 2 ||
                pitem.p_size == 20)){
            
                strncpy(pitemScratch[scratchMemoryLeftCount].p_brand, pitem.p_brand, sizeof(pitem.p_brand));
                pitemScratch[scratchMemoryLeftCount].p_partkey = pitem.p_partkey;
                pitemScratch[scratchMemoryLeftCount].p_size = pitem.p_size;
                strncpy(pitemScratch[scratchMemoryLeftCount].p_type, pitem.p_type, sizeof(pitem.p_type));
                scratchMemoryLeftCount++;
        }
    }
    close(fpIn);
}
int scanAndFilterSupplierTable(){
    /*We have used grep to find the s_suppkey of lines matching 
     * '%Customer%Complaints' and put it into an array. This is because 
     * this saves a lot of hassles in computing such expression in C. */ 
    
    //supplier *sitem = (supplier*)vmalloc(vmPCM, sizeof(supplier));
    //int fpSupp = open(SUPPLIER_TABLE_FILE, O_RDONLY);
    
    /*Dummy reads to just increase PCM write count */ 
    //while(read(fpSupp, sitem, sizeof(*sitem)* 1) != 0);
    //close(fpSupp);
    filtered_s_suppkey[0] = 358;
    filtered_s_suppkey[1] = 2820;
    filtered_s_suppkey[2] = 3804;
    filtered_s_suppkey[3] = 9504;
}

int scanAndFilterPartsupplierTable(){
    partsupp psitem;
    
    int fpPartsupp = open(PARTSUPPLIER_TABLE_FILE, O_RDONLY);
    projectedPartsuppItem *psitemScratch = (projectedPartsuppItem*)scratchMemoryRight;
    UINT32 index;
    BOOL discard;
    scratchMemoryRightCount = 0;
    while(read(fpPartsupp, &psitem, sizeof(psitem)*1) != 0){
        discard = 0;
        for(index=0; index<FILTERED_S_SUPPKEY_COUNT; index++){
            if(psitem.ps_suppkey == filtered_s_suppkey[index]){
                discard = 1;
                break;
            } 
        }
        if (!discard){
            psitemScratch[scratchMemoryRightCount].ps_suppkey = psitem.ps_suppkey;
            psitemScratch[scratchMemoryRightCount].ps_partkey = psitem.ps_partkey;
            scratchMemoryRightCount++;
        }
    }
#if 0
    part * pitemScratch = (part*)scratchMemoryLeft;
    for(index=0; index<scratchMemoryLeftCount; index++){
        printf("%d\n ", pitemScratch[index].p_partkey);
    }
#endif
    close(fpPartsupp);
}

int joinPartAndPartsuppByPartkey(){
    initHT(vmPCM, BUCKET_COUNT, OPT_ENTRIES_PER_PAGE);
    int index;
    void *lastPage = NULL, *lastIndex = NULL;
    projectedPartItem *pitem = (projectedPartItem*) scratchMemoryLeft;
    printf("Join:[LeftRecordCount:%d, RightRecordCount:%d]\n", 
            scratchMemoryLeftCount,
            scratchMemoryRightCount);
    for (index = 0; index < scratchMemoryLeftCount; index++) {
        insertHashEntry((void*) &(pitem[index]),
                (char*) &(pitem[index].p_partkey),
                sizeof (pitem[index].p_partkey));
        //printf("[index:%d partkey:%d] Inserted at :%p\n", index, pitem[index].p_partkey
          //      , &(pitem[index]));
    }
    projectedPartItem *tuplePtr;
    projectedPartsuppItem *psitem = (projectedPartsuppItem*) scratchMemoryRight;
    part_partsupp_join_struct *ppjsitem = (part_partsupp_join_struct*)scratchMemoryOut;
    scratchMemoryOutCount = 0;
    printf("Done Inserting. Now Joins\n");
    /*Ending Simulation because simulator crashing at some instruction here*/
    //SimEnd();
    for (index = 0; index < scratchMemoryRightCount; index++) {
        lastPage = NULL, lastIndex = NULL;
        if(index%100 == 0){
            printf("Completed: %d\n", index);
        }
        while (searchHashEntry((char*) &(psitem[index].ps_partkey),
                sizeof (psitem[index].ps_partkey),
                (void**) &tuplePtr, &lastPage, &lastIndex) == 1) {
            
            if (tuplePtr->p_partkey == psitem[index].ps_partkey) {
                
                strncpy(ppjsitem[scratchMemoryOutCount].p_brand ,
                        tuplePtr->p_brand, 
                        sizeof(ppjsitem[scratchMemoryOutCount].p_brand));
                ppjsitem[scratchMemoryOutCount].p_size = tuplePtr->p_size;
                strncpy(ppjsitem[scratchMemoryOutCount].p_type, 
                        tuplePtr->p_type, 
                        sizeof(ppjsitem[scratchMemoryOutCount].p_type));
                ppjsitem[scratchMemoryOutCount].ps_suppkey = psitem[index].ps_suppkey;
                
                scratchMemoryOutCount++;
            }
        }

    }
    //SimBegin();
    freeHashTable();
}
int comparePartPartSuppJoinElem(const void *p1, const void *p2){
    assert(p1 != NULL);
    assert(p2 != NULL);
    part_partsupp_join_struct ptr1 = *(part_partsupp_join_struct*)p1;
    part_partsupp_join_struct ptr2 = *(part_partsupp_join_struct*)p2;
    
    int brandComparison = strncmp(ptr1.p_brand, ptr2.p_brand, sizeof(ptr1.p_brand));
    if(brandComparison != 0){
        return brandComparison;
    }
    
    int typeComparison = strncmp(ptr1.p_type, ptr2.p_type, sizeof(ptr1.p_type));
    if(typeComparison != 0){
        return typeComparison;
    }
    
    int sizeComparison = ptr1.p_size - ptr2.p_size;
    if(sizeComparison != 0){
        return sizeComparison;
    }
    return 0;
    
}
void sortByBrandTypeSize(){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)scratchMemoryOut;
    sortMultiPivotAndUndo(vmPCM, inputTuples, 
            scratchMemoryOutCount, 
            sizeof(*inputTuples), 
            comparePartPartSuppJoinElem,
            scratchMemoryLeft);
    /**********Debug Purpose Only***************/
    
    int i;
    int remainingSize = scratchMemoryOutCount;
    char *currOutPtr = scratchMemoryLeft;
    int currSize;
    int *pos;
    part_partsupp_join_struct *tuples;

    printf("\n ===Final Array====\n");
    while (remainingSize > 0) {
        currSize = *(int*) currOutPtr;
        pos = (int *)(currOutPtr + sizeof (UINT32));
        tuples = (part_partsupp_join_struct*)((char*) pos + currSize * sizeof (int));
#if 0
        printf("CurrSize: %d\n", currSize);
        printf("Set of Pos\n");
        for (i = 0; i < currSize; i++) {
            printf("Pos : %d\n", pos[i]);
        }
#endif
#if 1
        for (i = 0; i < currSize; i++) {
            printf("Brand: %s Type: %s, Size: %d\n", tuples[pos[i]].p_brand,
                    tuples[pos[i]].p_type,
                    tuples[pos[i]].p_size);
        }
#endif
        /* We have to jump both pos array and tuples array
         * One extra UINT32 size jump to jump the size parameter itself*/
        currOutPtr += currSize * (sizeof(part_partsupp_join_struct) + sizeof (UINT32)) + sizeof (UINT32);
        remainingSize -= currSize;
    }
    /******************Debug End ***************/
}
void sortSimple(){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)scratchMemoryOut;
    qsort(inputTuples, scratchMemoryOutCount, sizeof(*inputTuples), comparePartPartSuppJoinElem);
    int i;
    for(i=0; i<scratchMemoryOutCount;i++){
        printf("Brand: %s Type: %s, Size: %d\n", inputTuples[i].p_brand,
                    inputTuples[i].p_type,
                    inputTuples[i].p_size);
    }
}

aggregateSorteditems(part_partsupp_join_struct *inputTuples, int inputCount, part_partsupp_join_struct * outputBuffer, int* outputCount){
    int i;
    *outputCount = 0;
    outputBuffer[(*outputCount)++] = inputTuples[0];
    for(i=1; i<inputCount; i++){
        if( comparePartPartSuppJoinElem(&(inputTuples[i]), &(inputTuples[i-1]))!= 0){
            outputBuffer[(*outputCount)++] = inputTuples[i];
        }
    }
        printf("aggregateCount :%d", *outputCount);
    
}

void init(){
    
    pcmMemory = (char*)malloc(PCM_MEMORY_SIZE);
    assert (pcmMemory != NULL);
    
    vmPCM = vmemopen(pcmMemory, PCM_MEMORY_SIZE, 0);
    assert(vmPCM != NULL);
    
    scratchMemoryLeft = (char*)vmalloc(vmPCM, SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryLeft != NULL);
    
    scratchMemoryRight = (char*)vmalloc(vmPCM, SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryRight != NULL);
    
    scratchMemoryOut = (char*)vmalloc(vmPCM, SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryOut != NULL);
    
    PCMRange((unsigned long long)pcmMemory, (unsigned long long)(pcmMemory + PCM_MEMORY_SIZE));
    //printf("PCM Range Set to [Beg:%p End:%p]\n", pcmMemory, (pcmMemory + PCM_MEMORY_SIZE));
    
    
    
}
void flushDRAM(){
char *dram_array = (char*) malloc(DRAM_MEMORY_SIZE);
int sum = 0, i;
for(i=0; i<DRAM_MEMORY_SIZE; i++){
        sum += dram_array[i];
}
printf("Flushing Sum: %d\n", sum);
}
/*
 * 
 */
int main(int argc, char** argv) {
    init();
    scanAndFilterPartTable();
    printf("scanAndFilterPartTable Over\n");
    scanAndFilterSupplierTable();
    printf("scanAndFilterSupplierTable Over\n");
    scanAndFilterPartsupplierTable();
    printf("scanAndFilterPartsupplierTable Over\n");
    joinPartAndPartsuppByPartkey();
    printf("joinPartAndPartsuppByPartkey Over\n");
    //SimBegin();
    sortByBrandTypeSize();
    //sortSimple();
    aggregateSorteditems();
    flushDRAM();
    SimEnd();
    return (EXIT_SUCCESS);
}

