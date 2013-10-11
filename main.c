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
#include "GB_hashTable.h"
#include "GeneralHashFunctions.h"
#include "clock.h"

#define FILTERED_S_SUPPKEY_COUNT 4
#define POSTGRES_QUERY 0
#define OPTIMIZED_QUERY 1

UINT32 scratchMemoryOneCount;
UINT32 scratchMemoryTwoCount;
UINT32 scratchMemoryThreeCount;
UINT32 scratchMemoryFourCount;


UINT32 bucketCountForAggregation;

char *scratchMemoryOne;
char *scratchMemoryTwo;
char *scratchMemoryThree;
char *scratchMemoryFour;

char *pcmMemory;
part_partsupp_join_struct *inputTupleStart;

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

typedef struct aggregated_part_partsupp_join{
        char p_brand[11];
        char p_type[26];
        UINT32 p_size;
        UINT32 distinct_ps_suppkey;
}aggregated_part_partsupp_join;

int scanAndFilterPartTable(){
    part pitem;
    int fpIn = open(PART_TABLE_FILE, O_RDONLY);
    projectedPartItem *pitemScratch = (projectedPartItem*)scratchMemoryOne;
    scratchMemoryOneCount = 0;
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
            
                strncpy(pitemScratch[scratchMemoryOneCount].p_brand, pitem.p_brand, sizeof(pitem.p_brand));
                pitemScratch[scratchMemoryOneCount].p_partkey = pitem.p_partkey;
                pitemScratch[scratchMemoryOneCount].p_size = pitem.p_size;
                strncpy(pitemScratch[scratchMemoryOneCount].p_type, pitem.p_type, sizeof(pitem.p_type));
                scratchMemoryOneCount++;
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
    projectedPartsuppItem *psitemScratch = (projectedPartsuppItem*)scratchMemoryTwo;
    UINT32 index;
    BOOL discard;
    scratchMemoryTwoCount = 0;
    while(read(fpPartsupp, &psitem, sizeof(psitem)*1) != 0){
        discard = 0;
        for(index=0; index<FILTERED_S_SUPPKEY_COUNT; index++){
            if(psitem.ps_suppkey == filtered_s_suppkey[index]){
                discard = 1;
                break;
            } 
        }
        if (!discard){
            psitemScratch[scratchMemoryTwoCount].ps_suppkey = psitem.ps_suppkey;
            psitemScratch[scratchMemoryTwoCount].ps_partkey = psitem.ps_partkey;
            scratchMemoryTwoCount++;
        }
    }
#if 0
    part * pitemScratch = (part*)scratchMemoryOne;
    for(index=0; index<scratchMemoryOneCount; index++){
        printf("%d\n ", pitemScratch[index].p_partkey);
    }
#endif
    close(fpPartsupp);
}

int joinPartAndPartsuppByPartkey(UINT8 queryType){
    if(queryType == POSTGRES_QUERY){
        initHT(vmPCM, BUCKET_COUNT, PSQL_ENTRIES_PER_PAGE);
    }
    else{
        initHT(vmPCM, BUCKET_COUNT, OPT_ENTRIES_PER_PAGE);
    }
    
    int index;
    void *lastPage = NULL, *lastIndex = NULL;
    projectedPartItem *pitem = (projectedPartItem*) scratchMemoryOne;
    printf("Join:[LeftRecordCount:%d, RightRecordCount:%d]\n", 
            scratchMemoryOneCount,
            scratchMemoryTwoCount);
    for (index = 0; index < scratchMemoryOneCount; index++) {
        insertHashEntry((void*) &(pitem[index]),
                (char*) &(pitem[index].p_partkey),
                sizeof (pitem[index].p_partkey));
        //printf("[index:%d partkey:%d] Inserted at :%p\n", index, pitem[index].p_partkey
          //      , &(pitem[index]));
    }
    projectedPartItem *tuplePtr;
    projectedPartsuppItem *psitem = (projectedPartsuppItem*) scratchMemoryTwo;
    part_partsupp_join_struct *ppjsitem = (part_partsupp_join_struct*)scratchMemoryThree;
    scratchMemoryThreeCount = 0;
    printf("Done Inserting. Now Joins\n");
    /*Ending Simulation because simulator crashing at some instruction here*/
    SimEnd();
    start_counter();
    for (index = 0; index < scratchMemoryTwoCount; index++) {
        lastPage = NULL, lastIndex = NULL;
//            printf("Completed: %d\n", index);
//        if(index%100 == 0){
//        }
        while (searchHashEntry((char*) &(psitem[index].ps_partkey),
                sizeof (psitem[index].ps_partkey),
                (void**) &tuplePtr, &lastPage, &lastIndex) == 1) {
            
            if (tuplePtr->p_partkey == psitem[index].ps_partkey) {
                
                strncpy(ppjsitem[scratchMemoryThreeCount].p_brand ,
                        tuplePtr->p_brand, 
                        sizeof(ppjsitem[scratchMemoryThreeCount].p_brand));
                ppjsitem[scratchMemoryThreeCount].p_size = tuplePtr->p_size;
                strncpy(ppjsitem[scratchMemoryThreeCount].p_type, 
                        tuplePtr->p_type, 
                        sizeof(ppjsitem[scratchMemoryThreeCount].p_type));
                ppjsitem[scratchMemoryThreeCount].ps_suppkey = psitem[index].ps_suppkey;
                
                scratchMemoryThreeCount++;
            }
        }

    }
    double cycles = get_counter();
    printf("Join Cycles %d", cycles);
    SimBegin();
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
UINT32 hashValue(UINT32 rid){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)inputTupleStart;
    char *p_type = inputTuples[rid].p_type;
    UINT32 size = sizeof(inputTuples[rid].p_type);
    UINT32 hash = PJWHash(p_type, size);
    return ((PJWHash(p_type, size) & HASH_MASK) >> HASH_SHIFT);
}
UINT32 bucketId(UINT32 rid) {
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)inputTupleStart;
    char *p_type = inputTuples[rid].p_type;
    UINT32 size = sizeof(inputTuples[rid].p_type);
    return (PJWHash(p_type, size) % BUCKET_COUNT_FOR_AGG);
}
UINT32 compareRidOverall(UINT32 rid1, UINT32 rid2){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)inputTupleStart;
    part_partsupp_join_struct ptr1 = inputTuples[rid1];
    part_partsupp_join_struct ptr2 = inputTuples[rid2];
#if 0
        printf("ptr1 Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", ptr1.p_brand,
                    ptr1.p_type,
                    ptr1.p_size,
                    ptr1.ps_suppkey);
    printf("ptr2 Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", ptr2.p_brand,
                    ptr2.p_type,
                    ptr2.p_size,
                    ptr2.ps_suppkey);
#endif
    int ps_suppkey_comparison = ptr1.ps_suppkey - ptr2.ps_suppkey;
    
    if(ps_suppkey_comparison != 0){
        return -(ps_suppkey_comparison);
    }
    
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
UINT32 compareRidWithoutSuppkey(UINT32 rid1, UINT32 rid2){
    part_partsupp_join_struct ptr1 = inputTupleStart[rid1];
    part_partsupp_join_struct ptr2 = inputTupleStart[rid2];
    //printf("ptr1 Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", ptr1.p_brand,
            //        ptr1.p_type,
            //        ptr1.p_size,
            //        ptr1.ps_suppkey);
    //printf("ptr2 Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", ptr2.p_brand,
                //    ptr2.p_type,
                //    ptr2.p_size,
                  //  ptr2.ps_suppkey);
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
    printf("rid:%d and rid:%d match\n", rid1, rid2);
    return 0;
}
void aggregateByGB(){
        //UINT32 ARRAY[] = {5,1,2,3,6,4, 5, 6};
    
    
#define NUM_PASS 1
    //GB_initHT(vmPCM, BUCKET_COUNT_FOR_AGG, 16, 4192, NUM_PASS, bucketId, hashValue, compareRidOverall);
    GB_initHT(vmPCM, BUCKET_COUNT_FOR_AGG, 64, 4192, NUM_PASS, bucketId, hashValue, compareRidOverall);
    
    assert(vmPCM != NULL);
    UINT32 pass,
            j;
    UINT32 lastCount = 0;
    inputTupleStart = (part_partsupp_join_struct*)scratchMemoryThree;
    for(j=0; j<scratchMemoryThreeCount; j++){
        printf("Handling rid [%d] Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", j, inputTupleStart[j].p_brand,
                    inputTupleStart[j].p_type,
                    inputTupleStart[j].p_size,
                    inputTupleStart[j].ps_suppkey); }
    
    for(pass=0; pass<NUM_PASS; pass++)
    {
        for(j=0; j<scratchMemoryThreeCount; j++){
    //for(j=0; j<100; j++){
//        printf("Handling rid [%d] Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", j, inputTupleStart[j].p_brand,
//                    inputTupleStart[j].p_type,
//                    inputTupleStart[j].p_size,
//                    inputTupleStart[j].ps_suppkey);    
        GB_searchNoUpdateAndInsert(j, pass);
    }
    
    GB_printMemoryRec(scratchMemoryFour, &scratchMemoryFourCount);
    
    printf("Hash Table Records in this partition (Phase 1) %d\n", scratchMemoryFourCount);
    
    GB_freePages();
    
    UINT32 *semiAggTuplesPos = (UINT32*) scratchMemoryFour;
    GB_changeRIDComparison(compareRidWithoutSuppkey);
    inputTupleStart = scratchMemoryThree;
    
    for(j=0; j<scratchMemoryFourCount; j++){
        GB_searchUpdateAndInsert(semiAggTuplesPos[2*j], pass);
    }
    
    GB_printMemoryRec(scratchMemoryTwo, &scratchMemoryTwoCount);
    
    aggregated_part_partsupp_join *aggregatedOuputTuples = (aggregated_part_partsupp_join*)scratchMemoryOne;
    aggregated_part_partsupp_join tempAggregatedOuputTuple;
    semiAggTuplesPos = (UINT32*)scratchMemoryTwo; 
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)inputTupleStart;
    for(j=lastCount; j<lastCount+scratchMemoryTwoCount; j++){
         
        strncpy(tempAggregatedOuputTuple.p_brand, inputTuples[semiAggTuplesPos[2*j]].p_brand, sizeof(tempAggregatedOuputTuple.p_brand));
            strncpy(tempAggregatedOuputTuple.p_type, inputTuples[semiAggTuplesPos[2*j]].p_type, sizeof(tempAggregatedOuputTuple.p_type));
            tempAggregatedOuputTuple.p_size = inputTuples[semiAggTuplesPos[2*j]].p_size;
            tempAggregatedOuputTuple.distinct_ps_suppkey = semiAggTuplesPos[(2*j)+1];
            aggregatedOuputTuples[j] = tempAggregatedOuputTuple;
            printf("COPY Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", aggregatedOuputTuples[j].p_brand,
                    aggregatedOuputTuples[j].p_type,
                    aggregatedOuputTuples[j].p_size,
                    aggregatedOuputTuples[j].distinct_ps_suppkey);
    }
    lastCount+=scratchMemoryTwoCount;
}
    scratchMemoryOneCount = lastCount;
    printf("lastCount:%d\n", lastCount);
    GB_freeHashTable();
    return;
}
void sortByBrandTypeSizeAndAggregate(){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)scratchMemoryThree;
    part_partsupp_join_struct *outputTuples = (part_partsupp_join_struct*)scratchMemoryThree;
    
    aggregated_part_partsupp_join tempAggregatedOuputTuple ;
    /*This stores the location where final aggregated tuples will reside*/
    aggregated_part_partsupp_join *aggregatedOuputTuples = (aggregated_part_partsupp_join*)scratchMemoryOne;
    sortMultiPivotAndUndo(vmPCM, inputTuples, 
            scratchMemoryThreeCount, 
            sizeof(*inputTuples), 
            comparePartPartSuppJoinElem,
            scratchMemoryTwo,
            100, 6000);
    /**********Debug Purpose Only***************/
    
    int i;
    int remainingSize = scratchMemoryThreeCount;
    char *currOutPtr = scratchMemoryTwo;
    int currSize;
    int *positions;
    UINT32 aggregateCount = 0;
    UINT32 distinctSuppkeyCount = 0;
    UINT32 partitionId= 0;

    printf("\n ===Final Array====\n");
    /*Simulator crashing in while loop. Reasons not clear. Can consider writes to be added accordingly*/
    SimEnd();
    //SimBegin();
    while (remainingSize > 0) {
        currSize = *(int*) currOutPtr;
        positions = (int *)(currOutPtr + sizeof (UINT32));
        
#if 0
        printf("CurrSize: %d\n", currSize);
        printf("Set of Pos\n");
        for (i = 0; i < currSize; i++) {
            printf("Partition ID:%d Pos[%d] : %d\n", partitionId, i, positions[i]);
        }
#endif
        
#if 1   
        partitionId++;
            
        for (i = 0; i < currSize; i++) {
            int j;
            assert(positions[i] < currSize);
            strncpy(tempAggregatedOuputTuple.p_brand, outputTuples[positions[i]].p_brand, sizeof(outputTuples[positions[i]].p_brand));
            strncpy(tempAggregatedOuputTuple.p_type, outputTuples[positions[i]].p_type, sizeof(outputTuples[positions[i]].p_type));
            tempAggregatedOuputTuple.p_size = outputTuples[positions[i]].p_size;
            i++;
            distinctSuppkeyCount=1;
            while((i<currSize) && 
                   ((strncmp(outputTuples[positions[i]].p_brand, outputTuples[positions[i-1]].p_brand, sizeof(outputTuples[positions[i]].p_brand)) == 0) &&
                    (strncmp(outputTuples[positions[i]].p_type, outputTuples[positions[i-1]].p_type, sizeof(outputTuples[positions[i]].p_type)) == 0) &&
                    (outputTuples[positions[i]].p_size == outputTuples[positions[i-1]].p_size))){
              if(outputTuples[positions[i]].ps_suppkey != outputTuples[positions[i-1]].ps_suppkey){
                    distinctSuppkeyCount++;
                }
                i++;
            }
            i--;
            tempAggregatedOuputTuple.distinct_ps_suppkey = distinctSuppkeyCount;
            aggregatedOuputTuples[aggregateCount] = tempAggregatedOuputTuple;
            aggregateCount++;
            
        }
#endif
        /* We have to jump both positions array and tuples array
         * One extra UINT32 size jump to jump the size parameter itself*/
        currOutPtr += (currSize * sizeof (UINT32)) + sizeof (UINT32);
        outputTuples += currSize;
        remainingSize -= currSize;
    }
    SimBegin();
    //SimEnd();
    scratchMemoryOneCount = aggregateCount;
    /******************Debug End ***************/
}
void PG_sortSimpleAndAggregate(){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)scratchMemoryThree;
    aggregated_part_partsupp_join tempAggregatedOuputTuple ;
    aggregated_part_partsupp_join *aggregatedOuputTuples = (aggregated_part_partsupp_join*)scratchMemoryOne;
    UINT32 aggregateCount = 0;
    UINT32 distinctSuppkeyCount = 0;
    
    qsort(inputTuples, scratchMemoryThreeCount, sizeof(*inputTuples), comparePartPartSuppJoinElem);
    int i;
    for(i=0; i<scratchMemoryThreeCount;i++){
        //printf("Brand: %s Type: %s, Size: %d\n", inputTuples[i].p_brand,
                    //inputTuples[i].p_type,
                    //inputTuples[i].p_size);
        strncpy(tempAggregatedOuputTuple.p_brand, inputTuples[i].p_brand, sizeof(inputTuples[i].p_brand));
            strncpy(tempAggregatedOuputTuple.p_type, inputTuples[i].p_type, sizeof(inputTuples[i].p_type));
            tempAggregatedOuputTuple.p_size = inputTuples[i].p_size;
            i++;
            distinctSuppkeyCount=1;
            while((i<scratchMemoryThreeCount) && 
                   ((strncmp(inputTuples[i].p_brand, inputTuples[i-1].p_brand, sizeof(inputTuples[i].p_brand)) == 0) &&
                    (strncmp(inputTuples[i].p_type, inputTuples[i-1].p_type, sizeof(inputTuples[i].p_type)) == 0) &&
                    (inputTuples[i].p_size == inputTuples[i-1].p_size))){
                
                if(inputTuples[i].ps_suppkey != inputTuples[i-1].ps_suppkey){
                    distinctSuppkeyCount++;
                }
                i++;
            }
            i--;
            tempAggregatedOuputTuple.distinct_ps_suppkey = distinctSuppkeyCount;
            aggregatedOuputTuples[aggregateCount] = tempAggregatedOuputTuple;
            aggregateCount++;
    }
    scratchMemoryOneCount = aggregateCount;
}
int compareAggregatedPartPartsuppJoinElem(const void *p1, const void *p2){
    assert(p1 != NULL);
    assert(p2 != NULL);
    aggregated_part_partsupp_join ptr1 = *(aggregated_part_partsupp_join*)p1;
    aggregated_part_partsupp_join ptr2 = *(aggregated_part_partsupp_join*)p2;
    /*Sorting is to be in desc order wrt to suppkey, so changing signs*/
    int distinct_ps_suppkey_comparison = ptr1.distinct_ps_suppkey - ptr2.distinct_ps_suppkey;
    
    if(distinct_ps_suppkey_comparison != 0){
        return -(distinct_ps_suppkey_comparison);
    }
    
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
void PG_sortFinal(){
    aggregated_part_partsupp_join *inputTuples = (aggregated_part_partsupp_join*)scratchMemoryOne;
    qsort(inputTuples, scratchMemoryOneCount, sizeof(*inputTuples), compareAggregatedPartPartsuppJoinElem);
    int i;
    for(i=0; i<scratchMemoryOneCount;i++){
//        printf("Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", inputTuples[i].p_brand,
//                    inputTuples[i].p_type,
//                    inputTuples[i].p_size, 
//                    inputTuples[i].distinct_ps_suppkey);
        if(i%100 == 0){
            printf("Final Sorted :%d\n", i);
        }
    }
}

void sortFinal(){
    aggregated_part_partsupp_join *inputTuples = (aggregated_part_partsupp_join*)scratchMemoryOne;
    aggregated_part_partsupp_join *sortedOutputTuples = (aggregated_part_partsupp_join*)scratchMemoryOne;
    printf("Tuples Before sortFinal");
    int i;
    int *positions;
//    for (i = 0; i < scratchMemoryOneCount; i++) {
//            printf("Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", sortedOutputTuples[i].p_brand,
//                    sortedOutputTuples[i].p_type,
//                    sortedOutputTuples[i].p_size,
//                    sortedOutputTuples[i].distinct_ps_suppkey);
//        }
    
    sortMultiPivotAndUndo(vmPCM, inputTuples, 
            scratchMemoryOneCount, 
            sizeof(*inputTuples), 
            compareAggregatedPartPartsuppJoinElem,
            scratchMemoryTwo, 
            40, 8000);
    int remainingSize = scratchMemoryOneCount;
    char *currOutPtr = scratchMemoryTwo;
    int currSize;
    
    
    
    
    while (remainingSize > 0) {
        currSize = *(int*) currOutPtr;
        positions = (int *)(currOutPtr + sizeof (UINT32));
        
#if 0
        printf("CurrSize: %d\n", currSize);
        printf("Set of Pos\n");
        for (i = 0; i < currSize; i++) {
            printf("Pos : %d\n", positions[i]);
        }
#endif
#if 1
//        for (i = 0; i < currSize; i++) {
//            printf("Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", sortedOutputTuples[positions[i]].p_brand,
//                    sortedOutputTuples[positions[i]].p_type,
//                    sortedOutputTuples[positions[i]].p_size,
//                    sortedOutputTuples[positions[i]].distinct_ps_suppkey);
//        }
#endif
        /* We have to jump both positions array and sortedOutputTuples array
         * One extra UINT32 size jump to jump the size parameter itself*/
        currOutPtr += (currSize * sizeof (UINT32)) + sizeof (UINT32);
        sortedOutputTuples += currSize;
        remainingSize -= currSize;
    }
    
}

void init(){
    
    pcmMemory = (char*)malloc(PCM_MEMORY_SIZE);
    assert (pcmMemory != NULL);
    
    vmPCM = vmemopen(pcmMemory, PCM_MEMORY_SIZE, 0);
    assert(vmPCM != NULL);
    
    scratchMemoryOne = (char*)vmalloc(vmPCM, SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryOne != NULL);
    
    scratchMemoryTwo = (char*)vmalloc(vmPCM, SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryTwo != NULL);
    
    scratchMemoryThree = (char*)vmalloc(vmPCM, SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryThree != NULL);
    
    scratchMemoryFour = (char*)vmalloc(vmPCM, SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryFour != NULL);
    
    PCMRange((unsigned long long)pcmMemory, (unsigned long long)(pcmMemory + PCM_MEMORY_SIZE));
    //printf("PCM Range Set to [Beg:%p End:%p]\n", pcmMemory, (pcmMemory + PCM_MEMORY_SIZE));
    
    
    
}
void flushDRAM(){
char *dram_array = (char*) malloc(DRAM_MEMORY_SIZE);
int sum = 0, i;
printf("Flushing Data\n", sum);
for(i=0; i<DRAM_MEMORY_SIZE; i++){
        sum += dram_array[i];
}
printf("Flushing Sum: %d\n", sum);
}
/*
 * 
 */

postgresQueryExecution(){
    printf("Posgres Query Execution\n");
    SimBegin();
    scanAndFilterPartTable();
    printf("scanAndFilterPartTable Over\n");
    scanAndFilterSupplierTable();
    printf("scanAndFilterSupplierTable Over\n");
    scanAndFilterPartsupplierTable();
    printf("scanAndFilterPartsupplierTable Over\n");
    joinPartAndPartsuppByPartkey(POSTGRES_QUERY);
    printf("joinPartAndPartsuppByPartkey Over\n");
    
    PG_sortSimpleAndAggregate();
    PG_sortFinal();
    flushDRAM();
    SimEnd();
    return (EXIT_SUCCESS);
}

optimizedQueryExecution(){
    printf("Optimized Query Execution\n");
    SimBegin();
    scanAndFilterPartTable();
    printf("scanAndFilterPartTable Over\n");
    scanAndFilterSupplierTable();
    printf("scanAndFilterSupplierTable Over\n");
    scanAndFilterPartsupplierTable();
    printf("scanAndFilterPartsupplierTable Over\n");
    joinPartAndPartsuppByPartkey(OPTIMIZED_QUERY);
    printf("joinPartAndPartsuppByPartkey Over\n");
    
#if 0
    PG_sortSimpleAndAggregate();
    PG_sortFinal();
#else
    //sortByBrandTypeSizeAndAggregate();
    //printf("sortByBrandTypeSizeAndAggregate Over\n");
    aggregateByGB();
    printf("aggregateByGB over\n");
    
    sortFinal();
#endif
    flushDRAM();
    SimEnd();
    return (EXIT_SUCCESS);
}
int main(int argc, char** argv) {
   init();
   printf("sizeof (aggregated_part_partsupp_join) :%d\n", sizeof(aggregated_part_partsupp_join));
   printf("sizeof (part_partsupp_join_struct) :%d\n", sizeof(part_partsupp_join_struct));
   printf("sizeof (pageHash) :%d\n", sizeof(GB_pageHash));
   printf("sizeof (hashEntry) :%d\n", sizeof(GB_hashEntry));
//   printf("sizeof () :%d\n", );
//   printf("sizeof () :%d\n", );
//   printf("sizeof () :%d\n", );
   optimizedQueryExecution();
   //optimizedQueryExecution();
   
   return (EXIT_SUCCESS);  
}

