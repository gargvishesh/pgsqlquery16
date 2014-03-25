/* 
 * File:   main.c
 * Author: root
 *
 * Created on 24 September, 2013, 9:55 AM
 */

#include "ptlcalls_ptlsim.h"/*Keep this file at the top, else results in O_LARGEFILE error*/
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
#include "sorting.h"
#include "constants.h"
#include "GB_hashTable.h"
#include "GeneralHashFunctions.h"
//#include "clock.h"

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

char part_table_file[100];
char supplier_table_file[100];
char partsupplier_table_file[100];

#ifdef VMALLOC
Vmalloc_t *vmPCM; 
#endif
UINT32 simBeginCount;
void SimBegin(){
    printf("\n\n***Starting Sim [Count%d]***\n\n", ++simBeginCount);
    //ptlcall_switch_to_sim();
}
void SimEnd(){
    printf("\n\n***Ending Sim [Count: %d]***\n\n", simBeginCount);
    //ptlcall_switch_to_native();
}
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
    int fpIn = open(part_table_file, O_RDONLY);
    assert(fpIn != -1);
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
    printf("scanAndFilterPartTable Count: %d\n", scratchMemoryOneCount);
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
    printf("scanAndFilterSupplierTable Count: %d\n", FILTERED_S_SUPPKEY_COUNT);
    
}

int scanAndFilterPartsupplierTable(){
    partsupp psitem;
    
    int fpPartsupp = open(partsupplier_table_file, O_RDONLY);
    assert(fpPartsupp != -1);
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
    printf("scanAndFilterPartsupplierTable Count: %d\n", scratchMemoryTwoCount);
    close(fpPartsupp);
}

int joinPartAndPartsuppByPartkey(UINT8 queryType){
    if(queryType == POSTGRES_QUERY){
#ifdef VMALLOC
        initHT(vmPCM, BUCKET_COUNT, PSQL_ENTRIES_PER_PAGE);
#else
          initHT(BUCKET_COUNT, PSQL_ENTRIES_PER_PAGE);
#endif
    }
    else{
#ifdef VMALLOC
        initHT(vmPCM, BUCKET_COUNT, OPT_ENTRIES_PER_PAGE);
#else
          initHT(BUCKET_COUNT, OPT_ENTRIES_PER_PAGE);
#endif
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
        
    }
    projectedPartItem *tuplePtr;
    projectedPartsuppItem *psitem = (projectedPartsuppItem*) scratchMemoryTwo;
    part_partsupp_join_struct *ppjsitem = (part_partsupp_join_struct*)scratchMemoryThree;
    scratchMemoryThreeCount = 0;
    printf("Done Inserting. Now Joins. Outer Records Count: %d\n", scratchMemoryTwoCount);
    
    for (index = 0; index < scratchMemoryTwoCount; index++) {
        
        lastPage = NULL, lastIndex = NULL;
        if(index%10000 == 0){
            printf("Completed : %d\n", index);
        }
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
                assert(ppjsitem[scratchMemoryThreeCount].p_size != 0);
                scratchMemoryThreeCount++;
            }
        }

    }
    printf("joinPartAndPartsuppByPartkey Count: %d\n", scratchMemoryThreeCount);
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
    
    int ps_suppkey_comparison = ptr1.ps_suppkey - ptr2.ps_suppkey;
    if(ps_suppkey_comparison != 0){
        return ps_suppkey_comparison;
    }
    return 0;
    
}
UINT32 hashValue_from_rid(UINT32 rid){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)inputTupleStart;
    char *p_type = inputTuples[rid].p_type; //p_type is a char pointer
    UINT32 size = sizeof(inputTuples[rid].p_type);
    return ((PJWHash(p_type, size) & HASH_MASK) >> HASH_SHIFT);
}
UINT32 hashValue_from_ptr(void *ptr){
    part_partsupp_join_struct *tuple = (part_partsupp_join_struct*)ptr;
    char *p_type = tuple->p_type;
    UINT32 size = sizeof(tuple->p_type);
    //printf("ptr:%p, hash:%d, size:%d\n", ptr, PJWHash(p_type, size), size);
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
int compareRidWithoutSuppkey(const void* rid1, const void* rid2){
    part_partsupp_join_struct ptr1 = inputTupleStart[*(UINT32*)rid1];
    part_partsupp_join_struct ptr2 = inputTupleStart[*(UINT32*)rid2];
    
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
#if 0
void aggregateByGB(){

#define NUM_PASS 1
#ifdef VMALLOC
    GB_initHT(vmPCM, BUCKET_COUNT_FOR_AGG, 64, 4192, NUM_PASS, bucketId, hashValue_from_rid, compareRidOverall);
    assert(vmPCM != NULL);
#else
    GB_initHT(BUCKET_COUNT_FOR_AGG, 64, 4192, NUM_PASS, bucketId, hashValue_from_rid, compareRidOverall);
#endif

    UINT32 pass,
            j;
    UINT32 lastCount = 0;
    /*Both the input and output areas for Full Tuples till hash group by and sort will be the same. This is because
     we just manipulate the RIDs*/
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*) scratchMemoryThree;
    part_partsupp_join_struct *outputTuples = (part_partsupp_join_struct*) scratchMemoryThree;

    /********WARNING:inputTupleStart is a global var used by other functions 
     * also like comparision, bucket and hash calculation. Don't change it**********/
    inputTupleStart = inputTuples;
    /*******************************************************************************
     * ****************************************************************************/

    aggregated_part_partsupp_join tempAggregatedOuputTuple;

    /*This stores the location where final aggregated tuples will reside*/
    aggregated_part_partsupp_join *aggregatedOuputTuples = (aggregated_part_partsupp_join*) scratchMemoryOne;

    for (pass = 0; pass < NUM_PASS; pass++) {
        for (j = 0; j < scratchMemoryThreeCount; j++) {
            GB_searchNoUpdateAndInsert(j, pass);
        }
    }
    /*This is where the RIDs will reside after the round of Hash Table insertion and output*/
    GB_printMemoryRec(scratchMemoryTwo, &scratchMemoryTwoCount);

    /*At this point, the output(or next input) area should have total number of elements 
     * followed by for each partition, the size of that partition and corresponding list of rids*/

    int i;
    int remainingSize = scratchMemoryTwoCount;
    int totalInitialSize = scratchMemoryThreeCount;
    char *currOutPtr = scratchMemoryTwo; // Points to the area where positions array starts
    int currSize;
    int *positions;
    UINT32 aggregateCount = 0;
    UINT32 distinctSuppkeyCount = 0;
    UINT32 partitionId = 0;

    printf("\n ===Final Array====\n");
    while (remainingSize > 0) {
        currSize = *(int*) currOutPtr;
        positions = (int *) (currOutPtr + sizeof (UINT32));
        /*Using compareRidWithoutSuppkey since anyway the values that are 
         * coming after Hash Table insertion are distinct suppkeys */
        qsort(positions, currSize, sizeof (UINT32), compareRidWithoutSuppkey);

        assert(currSize <= remainingSize);
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
            assert(positions[i] < totalInitialSize);
            strncpy(tempAggregatedOuputTuple.p_brand, outputTuples[positions[i]].p_brand, sizeof (outputTuples[positions[i]].p_brand));
            strncpy(tempAggregatedOuputTuple.p_type, outputTuples[positions[i]].p_type, sizeof (outputTuples[positions[i]].p_type));
            tempAggregatedOuputTuple.p_size = outputTuples[positions[i]].p_size;
            i++;
            distinctSuppkeyCount = 1;
            while ((i < currSize) &&
                    ((strncmp(outputTuples[positions[i]].p_brand, outputTuples[positions[i - 1]].p_brand, sizeof (outputTuples[positions[i]].p_brand)) == 0) &&
                    (strncmp(outputTuples[positions[i]].p_type, outputTuples[positions[i - 1]].p_type, sizeof (outputTuples[positions[i]].p_type)) == 0) &&
                    (outputTuples[positions[i]].p_size == outputTuples[positions[i - 1]].p_size))) {
                if (outputTuples[positions[i]].ps_suppkey != outputTuples[positions[i - 1]].ps_suppkey) {
                    distinctSuppkeyCount++;
                }
                i++;
            }
            i--;
            tempAggregatedOuputTuple.distinct_ps_suppkey = distinctSuppkeyCount;
            assert(tempAggregatedOuputTuple.p_size != 0);
            aggregatedOuputTuples[aggregateCount] = tempAggregatedOuputTuple;
            assert(aggregatedOuputTuples[aggregateCount].p_size != 0);
            aggregateCount++;

        }
#endif
        /* We have to jump both positions array and tuples array
         * One extra UINT32 size jump to jump the size parameter itself*/
        currOutPtr += (currSize * sizeof (UINT32)) + sizeof (UINT32);
        remainingSize -= currSize;
    }
    assert(remainingSize == 0);
    scratchMemoryOneCount = aggregateCount;
    printf("aggregateByGB Count: %d\n", scratchMemoryOneCount);
    GB_freeHashTable();
    return;
}
void sortByBrandTypeSizeAndAggregate(){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)scratchMemoryThree;
    part_partsupp_join_struct *outputTuples = (part_partsupp_join_struct*)scratchMemoryThree;
    
    aggregated_part_partsupp_join tempAggregatedOuputTuple ;
    /*This stores the location where final aggregated tuples will reside*/
    aggregated_part_partsupp_join *aggregatedOuputTuples = (aggregated_part_partsupp_join*)scratchMemoryOne;
#ifdef VMALLOC
    sortMultiPivotAndUndo(vmPCM, inputTuples, 
            scratchMemoryThreeCount, 
            sizeof(*inputTuples), 
            comparePartPartSuppJoinElem,
            scratchMemoryTwo,
            100, 6000);
#else
    sortMultiPivotAndUndo((char*)inputTuples, 
            scratchMemoryThreeCount, 
            sizeof(*inputTuples), 
            comparePartPartSuppJoinElem,
            scratchMemoryTwo,
            100, 6000);
#endif
    /**********Debug Purpose Only***************/
    
    int i;
    int remainingSize = scratchMemoryThreeCount;
    char *currOutPtr = scratchMemoryTwo;
    int currSize;
    int *positions;
    UINT32 aggregateCount = 0;
    UINT32 distinctSuppkeyCount = 0;
    
    printf("\n ===Final Array====\n");
    while (remainingSize > 0) {
        currSize = *(int*) currOutPtr;
        positions = (int *)(currOutPtr + sizeof (UINT32));
        
#if 0
        UINT32 partitionId= 0;

        printf("CurrSize: %d\n", currSize);
        printf("Set of Pos\n");
        for (i = 0; i < currSize; i++) {
            printf("Partition ID:%d Pos[%d] : %d\n", partitionId, i, positions[i]);
        }
        partitionId++;

#endif
        
#if 1   
            
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
            assert(tempAggregatedOuputTuple.p_size != 0);
            aggregatedOuputTuples[aggregateCount] = tempAggregatedOuputTuple;
            assert(aggregatedOuputTuples[aggregateCount].p_size != 0);
            aggregateCount++;
            
        }
#endif
        /* We have to jump both positions array and tuples array
         * One extra UINT32 size jump to jump the size parameter itself*/
        currOutPtr += (currSize * sizeof (UINT32)) + sizeof (UINT32);
        outputTuples += currSize;
        remainingSize -= currSize;
    }
    //SimBegin();
    //SimEnd();
    scratchMemoryOneCount = aggregateCount;
    /******************Debug End ***************/
}
#endif
void PG_sortSimpleAndAggregate(){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)scratchMemoryThree;
    aggregated_part_partsupp_join tempAggregatedOuputTuple ;
    aggregated_part_partsupp_join *aggregatedOuputTuples = (aggregated_part_partsupp_join*)scratchMemoryOne;
    UINT32 aggregateCount = 0;
    UINT32 distinctSuppkeyCount = 0;
    SimBegin();
#if 1    
    qsort(inputTuples, scratchMemoryThreeCount, sizeof(*inputTuples), comparePartPartSuppJoinElem);
#else
#ifdef VMALLOC
    sortMultiPivotAndUndo(vmPCM, inputTuples, 
            scratchMemoryThreeCount, 
            sizeof(*inputTuples), 
            comparePartPartSuppJoinElem,
            scratchMemoryTwo,
            100, 6000);
#else
    sortMultiPivotAndUndo((char*)inputTuples, 
            scratchMemoryThreeCount, 
            sizeof(*inputTuples), 
            comparePartPartSuppJoinElem,
            scratchMemoryTwo,
            100, 6000);
#endif
#endif
    SimEnd();
    SimBegin();
    int i;
    for(i=0; i<scratchMemoryThreeCount;i++){
        strncpy(tempAggregatedOuputTuple.p_brand, inputTuples[i].p_brand, sizeof (inputTuples[i].p_brand));
        strncpy(tempAggregatedOuputTuple.p_type, inputTuples[i].p_type, sizeof (inputTuples[i].p_type));
        tempAggregatedOuputTuple.p_size = inputTuples[i].p_size;
        i++;
        distinctSuppkeyCount = 1;
        while ((i < scratchMemoryThreeCount) &&
                ((strncmp(inputTuples[i].p_brand, inputTuples[i - 1].p_brand, sizeof (inputTuples[i].p_brand)) == 0) &&
                (strncmp(inputTuples[i].p_type, inputTuples[i - 1].p_type, sizeof (inputTuples[i].p_type)) == 0) &&
                (inputTuples[i].p_size == inputTuples[i - 1].p_size))) {

            if (inputTuples[i].ps_suppkey != inputTuples[i - 1].ps_suppkey) {
                distinctSuppkeyCount++;
            }
            i++;
        }
        i--;
        tempAggregatedOuputTuple.distinct_ps_suppkey = distinctSuppkeyCount;
        assert(tempAggregatedOuputTuple.p_size != 0);
        aggregatedOuputTuples[aggregateCount] = tempAggregatedOuputTuple;
        assert(aggregatedOuputTuples[aggregateCount].p_size != 0);

        aggregateCount++;
    }
    scratchMemoryOneCount = aggregateCount;
    SimEnd();
    printf("PG_sortSimpleAndAggregate Count: %d\n", scratchMemoryOneCount);
}
void aggregateByGB(){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)scratchMemoryThree;
    aggregated_part_partsupp_join tempAggregatedOuputTuple ;
    aggregated_part_partsupp_join *aggregatedOuputTuples = (aggregated_part_partsupp_join*)scratchMemoryOne;
    UINT32 aggregateCount = 0;
    UINT32 distinctSuppkeyCount = 0;
#if 0    
    qsort(inputTuples, scratchMemoryThreeCount, sizeof(*inputTuples), comparePartPartSuppJoinElem);
#else
#ifdef VMALLOC
    sortMultiPivotAndUndo(vmPCM, inputTuples, 
            scratchMemoryThreeCount, 
            sizeof(*inputTuples), 
            comparePartPartSuppJoinElem,
            scratchMemoryTwo,
            100, 6000);
#else
    sortMultiHashAndUndo((char*)inputTuples, 
            scratchMemoryThreeCount, 
            sizeof(*inputTuples), 
            comparePartPartSuppJoinElem,
            scratchMemoryTwo,
            10, 6000, hashValue_from_ptr);
#endif
#endif
    int i;
#if 0
    for(i=0; i<scratchMemoryThreeCount;i++){
        strncpy(tempAggregatedOuputTuple.p_brand, inputTuples[i].p_brand, sizeof (inputTuples[i].p_brand));
        strncpy(tempAggregatedOuputTuple.p_type, inputTuples[i].p_type, sizeof (inputTuples[i].p_type));
        tempAggregatedOuputTuple.p_size = inputTuples[i].p_size;
        i++;
        distinctSuppkeyCount = 1;
        while ((i < scratchMemoryThreeCount) &&
                ((strncmp(inputTuples[i].p_brand, inputTuples[i - 1].p_brand, sizeof (inputTuples[i].p_brand)) == 0) &&
                (strncmp(inputTuples[i].p_type, inputTuples[i - 1].p_type, sizeof (inputTuples[i].p_type)) == 0) &&
                (inputTuples[i].p_size == inputTuples[i - 1].p_size))) {

            if (inputTuples[i].ps_suppkey != inputTuples[i - 1].ps_suppkey) {
                distinctSuppkeyCount++;
            }
            i++;
        }
        i--;
        tempAggregatedOuputTuple.distinct_ps_suppkey = distinctSuppkeyCount;
        assert(tempAggregatedOuputTuple.p_size != 0);
        aggregatedOuputTuples[aggregateCount] = tempAggregatedOuputTuple;
        assert(aggregatedOuputTuples[aggregateCount].p_size != 0);

        aggregateCount++;
    }
#endif
    int *positions = (int *)(scratchMemoryTwo);
    for (i = 0; i < scratchMemoryThreeCount; i++) {
            assert(positions[i] < scratchMemoryThreeCount);
            strncpy(tempAggregatedOuputTuple.p_brand, inputTuples[positions[i]].p_brand, sizeof(inputTuples[positions[i]].p_brand));
            strncpy(tempAggregatedOuputTuple.p_type, inputTuples[positions[i]].p_type, sizeof(inputTuples[positions[i]].p_type));
            tempAggregatedOuputTuple.p_size = inputTuples[positions[i]].p_size;
            i++;
            distinctSuppkeyCount=1;
            while((i<scratchMemoryThreeCount) && 
                   ((strncmp(inputTuples[positions[i]].p_brand, inputTuples[positions[i-1]].p_brand, sizeof(inputTuples[positions[i]].p_brand)) == 0) &&
                    (strncmp(inputTuples[positions[i]].p_type, inputTuples[positions[i-1]].p_type, sizeof(inputTuples[positions[i]].p_type)) == 0) &&
                    (inputTuples[positions[i]].p_size == inputTuples[positions[i-1]].p_size))){
              if(inputTuples[positions[i]].ps_suppkey != inputTuples[positions[i-1]].ps_suppkey){
                    distinctSuppkeyCount++;
                }
                i++;
            }
            i--;
            tempAggregatedOuputTuple.distinct_ps_suppkey = distinctSuppkeyCount;
            assert(tempAggregatedOuputTuple.p_size != 0);
            aggregatedOuputTuples[aggregateCount] = tempAggregatedOuputTuple;
            assert(aggregatedOuputTuples[aggregateCount].p_size != 0);
            aggregateCount++;
            
    }
    scratchMemoryOneCount = aggregateCount;
#if 0
    for (i = 0; i < scratchMemoryOneCount; i++) {
        printf("Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", aggregatedOuputTuples[i].p_brand,
                aggregatedOuputTuples[i].p_type,
                aggregatedOuputTuples[i].p_size,
                aggregatedOuputTuples[i].distinct_ps_suppkey);
    }
#endif
    printf("aggregateByGB Count: %d\n", scratchMemoryOneCount);
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
#if 0
    for (i = 0; i < scratchMemoryOneCount; i++) {
        printf("Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", inputTuples[i].p_brand,
                inputTuples[i].p_type,
                inputTuples[i].p_size,
                inputTuples[i].distinct_ps_suppkey);
    }
#endif
    printf("PG_sortFinal Count: %d\n", scratchMemoryOneCount);
}

void sortFinal(){
    aggregated_part_partsupp_join *inputTuples = (aggregated_part_partsupp_join*)scratchMemoryOne;
    aggregated_part_partsupp_join *sortedOutputTuples = (aggregated_part_partsupp_join*)scratchMemoryOne;
    int i;
    int *positions;
    
#ifdef VMALLOC
    sortMultiPivotAndUndo(vmPCM, inputTuples, 
            scratchMemoryOneCount, 
            sizeof(*inputTuples), 
            compareAggregatedPartPartsuppJoinElem,
            scratchMemoryTwo, 
            40, 8000);
#else
    
    sortMultiPivotAndUndo((char*)inputTuples, 
            scratchMemoryOneCount, 
            sizeof(*inputTuples), 
            compareAggregatedPartPartsuppJoinElem,
            scratchMemoryTwo, 
            40, 8000);
#endif
    
    int remainingSize = scratchMemoryOneCount;
    int totalInitialSize = remainingSize;
    char *currOutPtr = scratchMemoryTwo;
    
    positions = (int *)(currOutPtr);

#if 0
    for (i = 0; i < totalInitialSize; i++) {
        assert(positions[i] < totalInitialSize);
        printf("Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", sortedOutputTuples[positions[i]].p_brand,
                sortedOutputTuples[positions[i]].p_type,
                sortedOutputTuples[positions[i]].p_size,
                sortedOutputTuples[positions[i]].distinct_ps_suppkey);
    }
#endif
    printf("sortFinal Count: %d\n", scratchMemoryOneCount);
}

void init(){
#ifdef VMALLOC
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
#else
    scratchMemoryOne = (char*)malloc(SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryOne != NULL);
    
    scratchMemoryTwo = (char*)malloc(SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryTwo != NULL);
    
    scratchMemoryThree = (char*)malloc(SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryThree != NULL);
    
    scratchMemoryFour = (char*)malloc(SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryFour != NULL);
#endif    
    //PCMRange((unsigned long long)pcmMemory, (unsigned long long)(pcmMemory + PCM_MEMORY_SIZE));
    //printf("PCM Range Set to [Beg:%p End:%p]\n", pcmMemory, (pcmMemory + PCM_MEMORY_SIZE));
    
    
    
}
void flushDRAM(){
    char *dram_array = (char*) malloc(DRAM_MEMORY_SIZE);
    int sum = 0, i;
    printf("Flushing Data\n", sum);
    for (i = 0; i < DRAM_MEMORY_SIZE; i++) {
        sum += dram_array[i];
    }
    printf("Flushing Sum: %d\n", sum);
}
/*
 * 
 */

 int postgresQueryExecution(){
    printf("\n\n\n\n\n");
    printf("***********************\n");
    printf("Posgres Query Execution\n");
    printf("*************************\n");
    printf("\n\n\n\n\n");
    scanAndFilterPartTable();
    printf("scanAndFilterPartTable Over\n");
    scanAndFilterSupplierTable();
    printf("scanAndFilterSupplierTable Over\n");
    scanAndFilterPartsupplierTable();
    printf("scanAndFilterPartsupplierTable Over\n");
    flushDRAM();
    joinPartAndPartsuppByPartkey(POSTGRES_QUERY);
    printf("joinPartAndPartsuppByPartkey Over\n");
    PG_sortSimpleAndAggregate();
    PG_sortFinal();
    SimBegin();
    flushDRAM();
    SimEnd();
    return (EXIT_SUCCESS);
}

int optimizedQueryExecution(){
    printf("\n\n\n\n\n");
    printf("*************************\n");
    printf("Optimized Query Execution\n");
    printf("*************************\n");
    printf("\n\n\n\n\n");
    
    scanAndFilterPartTable();
    printf("scanAndFilterPartTable Over\n");
    scanAndFilterSupplierTable();
    printf("scanAndFilterSupplierTable Over\n");
    scanAndFilterPartsupplierTable();
    printf("scanAndFilterPartsupplierTable Over\n");
    flushDRAM();
    joinPartAndPartsuppByPartkey(OPTIMIZED_QUERY);
    printf("joinPartAndPartsuppByPartkey Over\n");
    aggregateByGB();
    printf("aggregateByGB over\n");
    sortFinal();
    SimBegin();
    flushDRAM();
    SimEnd();
    return (EXIT_SUCCESS);
}
int main(int argc, char** argv) {
   init();
   printf("sizeof (aggregated_part_partsupp_join) :%lu\n", sizeof(aggregated_part_partsupp_join));
   printf("sizeof (part_partsupp_join_struct) :%lu\n", sizeof(part_partsupp_join_struct));
   printf("sizeof (pageHash) :%lu\n", sizeof(GB_pageHash));
   printf("sizeof (hashEntry) :%lu\n", sizeof(GB_hashEntry));
   
#if 1
   assert( argc == 3 );
   strcat(part_table_file, argv[2]);
   strcat(supplier_table_file, argv[2]);
   strcat(partsupplier_table_file, argv[2]);
   
   strcat(part_table_file, PART_TABLE_FILE);
   strcat(supplier_table_file, SUPPLIER_TABLE_FILE);
   strcat(partsupplier_table_file, PARTSUPPLIER_TABLE_FILE);
   
   if(strcmp(argv[1],"0") == 0){
       postgresQueryExecution();
   }
   else{
       optimizedQueryExecution();
   }
#else
   postgresQueryExecution();
#endif
   return (EXIT_SUCCESS);  
}

