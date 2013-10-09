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
#define POSTGRES_QUERY 0
#define OPTIMIZED_QUERY 1

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

typedef struct aggregated_part_partsupp_join{
        char p_brand[11];
        char p_type[26];
        UINT32 p_size;
        UINT32 distinct_ps_suppkey;
}aggregated_part_partsupp_join;

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

int joinPartAndPartsuppByPartkey(UINT8 queryType){
    if(queryType == POSTGRES_QUERY){
        initHT(vmPCM, BUCKET_COUNT, PSQL_ENTRIES_PER_PAGE);
    }
    else{
        initHT(vmPCM, BUCKET_COUNT, OPT_ENTRIES_PER_PAGE);
    }
    
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
    SimEnd();
    for (index = 0; index < scratchMemoryRightCount; index++) {
        lastPage = NULL, lastIndex = NULL;
//            printf("Completed: %d\n", index);
//        if(index%100 == 0){
//        }
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
void sortByBrandTypeSizeAndAggregate(){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)scratchMemoryOut;
    part_partsupp_join_struct *outputTuples = (part_partsupp_join_struct*)scratchMemoryOut;
    
    aggregated_part_partsupp_join tempAggregatedOuputTuple ;
    /*This stores the location where final aggregated tuples will reside*/
    aggregated_part_partsupp_join *aggregatedOuputTuples = (aggregated_part_partsupp_join*)scratchMemoryLeft;
    sortMultiPivotAndUndo(vmPCM, inputTuples, 
            scratchMemoryOutCount, 
            sizeof(*inputTuples), 
            comparePartPartSuppJoinElem,
            scratchMemoryRight,
            100, 6000);
    /**********Debug Purpose Only***************/
    
    int i;
    int remainingSize = scratchMemoryOutCount;
    char *currOutPtr = scratchMemoryRight;
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
    scratchMemoryLeftCount = aggregateCount;
    /******************Debug End ***************/
}
void PG_sortSimpleAndAggregate(){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)scratchMemoryOut;
    aggregated_part_partsupp_join tempAggregatedOuputTuple ;
    aggregated_part_partsupp_join *aggregatedOuputTuples = (aggregated_part_partsupp_join*)scratchMemoryLeft;
    UINT32 aggregateCount = 0;
    UINT32 distinctSuppkeyCount = 0;
    
    qsort(inputTuples, scratchMemoryOutCount, sizeof(*inputTuples), comparePartPartSuppJoinElem);
    int i;
    for(i=0; i<scratchMemoryOutCount;i++){
        //printf("Brand: %s Type: %s, Size: %d\n", inputTuples[i].p_brand,
                    //inputTuples[i].p_type,
                    //inputTuples[i].p_size);
        strncpy(tempAggregatedOuputTuple.p_brand, inputTuples[i].p_brand, sizeof(inputTuples[i].p_brand));
            strncpy(tempAggregatedOuputTuple.p_type, inputTuples[i].p_type, sizeof(inputTuples[i].p_type));
            tempAggregatedOuputTuple.p_size = inputTuples[i].p_size;
            i++;
            distinctSuppkeyCount=1;
            while((i<scratchMemoryOutCount) && 
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
    scratchMemoryLeftCount = aggregateCount;
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
    aggregated_part_partsupp_join *inputTuples = (aggregated_part_partsupp_join*)scratchMemoryLeft;
    qsort(inputTuples, scratchMemoryLeftCount, sizeof(*inputTuples), compareAggregatedPartPartsuppJoinElem);
    int i;
    for(i=0; i<scratchMemoryLeftCount;i++){
        //printf("Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", inputTuples[i].p_brand,
          //          inputTuples[i].p_type,
            //        inputTuples[i].p_size, 
              //      inputTuples[i].distinct_ps_suppkey);
        if(i%100 == 0){
            printf("Final Sorted :%d\n", i);
        }
    }
}

void sortFinal(){
    aggregated_part_partsupp_join *inputTuples = (aggregated_part_partsupp_join*)scratchMemoryLeft;
    aggregated_part_partsupp_join *sortedOutputTuples = (aggregated_part_partsupp_join*)scratchMemoryLeft;
    
    sortMultiPivotAndUndo(vmPCM, inputTuples, 
            scratchMemoryLeftCount, 
            sizeof(*inputTuples), 
            compareAggregatedPartPartsuppJoinElem,
            scratchMemoryRight, 
            40, 8000);
    int remainingSize = scratchMemoryLeftCount;
    char *currOutPtr = scratchMemoryRight;
    int currSize;
    int *positions;
    int i;
    
    
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
    sortByBrandTypeSizeAndAggregate();
    printf("sortByBrandTypeSizeAndAggregate Over\n");
    sortFinal();
    flushDRAM();
    SimEnd();
    return (EXIT_SUCCESS);
}
int main(int argc, char** argv) {
   init();
   printf("sizeof (aggregated_part_partsupp_join) :%d\n", sizeof(aggregated_part_partsupp_join));
   printf("sizeof (part_partsupp_join_struct) :%d\n", sizeof(part_partsupp_join_struct));
//   printf("sizeof () :%d\n", );
//   printf("sizeof () :%d\n", );
//   printf("sizeof () :%d\n", );
//   printf("sizeof () :%d\n", );
//   printf("sizeof () :%d\n", );
   //postgresQueryExecution();
   optimizedQueryExecution();
   
   return (EXIT_SUCCESS);  
}

