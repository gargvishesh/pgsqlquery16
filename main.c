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

#define POSTGRES_QUERY 0
#define OPTIMIZED_QUERY 1

#define max(i,j) ((i>j)?i:j)

UINT32 dummyCount;
typedef struct memory{
    char * buffer;
    UINT32 count;
}memory;

UINT32 bucketCountForAggregation;

memory *scratchMemoryOne;
memory *scratchMemoryTwo;
memory *scratchMemoryThree;
memory *scratchMemoryFour;
memory *scratchMemoryFive;
char *fileReadBufferForWritesCreation;

char *pcmMemory;
part_partsupp_join_struct *inputTupleStart;

char part_table_file[100];
char supplier_table_file[100];
char partsupplier_table_file[100];

#ifdef VMALLOC
Vmalloc_t *vmPCM; 
#endif

int scanAndFilterPartTable(memory *outputMemory) {
    part pitem;
    int fpIn = open(part_table_file, O_RDONLY);
    int totalCount=0;
    assert(fpIn != -1);
    projectedPartItem *pitemScratch = (projectedPartItem*)outputMemory->buffer;
    UINT32 *outputCount = &(outputMemory->count);
    *outputCount = 0;
    dummyCount = 0;
    while (read(fpIn, &pitem, sizeof (pitem)* 1) != 0) {
        ((part*) fileReadBufferForWritesCreation)[dummyCount++] = pitem;
        totalCount++;
/*These changed introduced in order to do Picasso style Plan Diagram 
 * instead of fixed selectivity*/
#if 1
        if (strncmp(pitem.p_brand, "Brand#35", 8) != 0 &&
                strstr(pitem.p_type, "ECONOMY BURNISHED") == NULL &&
                (pitem.p_size == 14 ||
                pitem.p_size == 7 ||
                pitem.p_size == 21 ||
                pitem.p_size == 24 ||
                pitem.p_size == 35 ||
                pitem.p_size == 33 ||
                pitem.p_size == 2 ||
                pitem.p_size == 20)) {
#else

            if (pitem.p_retailprice <= selectivityConstant) {
#endif
                strncpy(pitemScratch[(*outputCount)].p_brand, pitem.p_brand, sizeof (pitem.p_brand));
                pitemScratch[(*outputCount)].p_partkey = pitem.p_partkey;
                pitemScratch[(*outputCount)].p_size = pitem.p_size;
                strncpy(pitemScratch[(*outputCount)].p_type, pitem.p_type, sizeof (pitem.p_type));
                (*outputCount)++;
            }
        }
        printf("scanAndFilterPartTable totalCount: %d, Count: %d\n", totalCount, (*outputCount));
        close(fpIn);
    }
int scanAndFilterSupplierTable(memory *outputMemory){
    /*We have used grep to find the s_suppkey of lines matching 
     * '%Customer%Complaints' and put it into an array. This is because 
     * this saves a lot of hassles in computing such expression in C. */ 
    
    supplier sitem;
    int fpSupp = open(supplier_table_file, O_RDONLY);
    int *filtered_s_suppkey = (int*)outputMemory->buffer;
    UINT32 *outputCount = &(outputMemory->count);
    int totalCount=0;
    dummyCount = 0;
    /*Dummy reads to just increase PCM write count */ 
    while(read(fpSupp, &sitem, sizeof(sitem)) != 0){
        ((supplier*)fileReadBufferForWritesCreation)[dummyCount++] = sitem;
        totalCount++;
#if 1
        }
    filtered_s_suppkey[0] = 358;
    filtered_s_suppkey[1] = 2820;
    filtered_s_suppkey[2] = 3804;
    filtered_s_suppkey[3] = 9504;
    *outputCount = 4;
#else
        if(sitem.s_acctbal <= selectivityConstant ){
            filtered_s_suppkey[scratchMemoryThreeCount++] = sitem.s_suppkey;
        }
}
#endif
    
    close(fpSupp);
    printf("scanAndFilterSupplierTable totalCount: %d Count: %d\n", totalCount, *outputCount);
    
}
int scanPartsupplierTable(memory *outputMemory){
    int fpPartsupp = open(partsupplier_table_file, O_RDONLY);
    assert(fpPartsupp != -1);
    partsupp psitem;
    partsupp *psitemScratch = (partsupp*)outputMemory->buffer;
    UINT32 *outputCount = &(outputMemory->count);
    dummyCount = 0;
    while(read(fpPartsupp, &psitem, sizeof(psitem)*1) != 0){
        ((partsupp*)fileReadBufferForWritesCreation)[dummyCount++] = psitem;
        psitemScratch[(*outputCount)++] = psitem;
    }
    close(fpPartsupp);
    printf("scanPartsupplierTable Count: %d\n", (*outputCount));
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
int antiJoinPartsupplierSupplierTable(memory *inputMemoryLeft, memory *inputMemoryRight, memory *outputMemory){
    partsupp psitem;
    
    int *filtered_s_suppkey = (int*)inputMemoryLeft->buffer;
    int *inputCountLeft = &(inputMemoryLeft->count);
    projectedPartsuppItem *psitemScratch = (projectedPartsuppItem*)outputMemory->buffer;
    int *outputCount = &(outputMemory->count);
    UINT32 index;
    (*outputCount) = 0;
    initHT((*inputCountLeft)/ENTRIES_PER_BUCKET, OPT_ENTRIES_PER_PAGE);
    for (index = 0; index < (*inputCountLeft); index++) {
        insertHashEntry((void*) &(filtered_s_suppkey[index]),
                (char*) &(filtered_s_suppkey[index]),
                sizeof *(filtered_s_suppkey));
        
    }
    partsupp *psitemScratchInput = (partsupp*)inputMemoryRight->buffer;
    UINT32 *inputCountRight = &(inputMemoryRight->count);
    
    for(index = 0; index < (*inputCountRight); index++){
      
        psitem = psitemScratchInput[index];
        void *lastPage = NULL, *lastIndex = NULL;
        char *tuplePtr;
        if (searchHashEntry((char*) &(psitem.ps_suppkey),
                    sizeof (psitem.ps_suppkey),
                    (void**) &tuplePtr, &lastPage, &lastIndex) == 0) {
            psitemScratch[(*outputCount)].ps_suppkey = psitem.ps_suppkey;
            psitemScratch[(*outputCount)].ps_partkey = psitem.ps_partkey;
            (*outputCount)++;
        }
    }
    printf("antiJoin Partsupplier Supplier Table Count: %d\n", (*outputCount));
    freeHashTable();
}

int joinPartAndPartsuppByPartkey(UINT8 queryType, memory *inputMemoryLeft, memory *inputMemoryRight, memory *outputMemory){
    projectedPartItem *pitem = (projectedPartItem*) inputMemoryLeft->buffer;
    UINT32 *inputCountLeft = &(inputMemoryLeft->count);
    
    projectedPartsuppItem *psitem = (projectedPartsuppItem*)inputMemoryRight->buffer;
    UINT32 *inputCountRight = &(inputMemoryRight->count);
    
    part_partsupp_join_struct *ppjsitem = (part_partsupp_join_struct*)outputMemory->buffer;
    UINT32 *outputCount = &(outputMemory->count);
    
    
    if(queryType == POSTGRES_QUERY){
#ifdef VMALLOC
        initHT(vmPCM, (*inputCountLeft)/ENTRIES_PER_BUCKET, PSQL_ENTRIES_PER_PAGE);
#else
          initHT((*inputCountLeft)/ENTRIES_PER_BUCKET, PSQL_ENTRIES_PER_PAGE);
#endif
    }
    else{
#ifdef VMALLOC
        initHT(vmPCM, BUCKET_COUNT, OPT_ENTRIES_PER_PAGE);
#else
          initHT((*inputCountLeft)/ENTRIES_PER_BUCKET, OPT_ENTRIES_PER_PAGE);
#endif
    }
    
    int index;
    void *lastPage = NULL, *lastIndex = NULL;
    printf("Join:[LeftRecordCount:%d, RightRecordCount:%d]\n", 
            (*inputCountLeft),
            (*inputCountRight));
    for (index = 0; index < (*inputCountLeft); index++) {
        insertHashEntry((void*) &(pitem[index]),
                (char*) &(pitem[index].p_partkey),
                sizeof (pitem[index].p_partkey));
        
    }
    projectedPartItem *tuplePtr;
    
    (*outputCount) = 0;
    printf("Done Inserting. Now Joins. Outer Records Count: %d\n", (*inputCountRight));
    
    for (index = 0; index < (*inputCountRight); index++) {
        
        lastPage = NULL, lastIndex = NULL;
        if(index%10000 == 0){
            printf("Completed : %d\n", index);
        }
        while (searchHashEntry((char*) &(psitem[index].ps_partkey),
                sizeof (psitem[index].ps_partkey),
                (void**) &tuplePtr, &lastPage, &lastIndex) == 1) {
            
            if (tuplePtr->p_partkey == psitem[index].ps_partkey) {
                
                strncpy(ppjsitem[(*outputCount)].p_brand ,
                        tuplePtr->p_brand, 
                        sizeof(ppjsitem[(*outputCount)].p_brand));
                ppjsitem[(*outputCount)].p_size = tuplePtr->p_size;
                strncpy(ppjsitem[(*outputCount)].p_type, 
                        tuplePtr->p_type, 
                        sizeof(ppjsitem[(*outputCount)].p_type));
                ppjsitem[(*outputCount)].ps_suppkey = psitem[index].ps_suppkey;
                assert(ppjsitem[(*outputCount)].p_size != 0);
                (*outputCount)++;
            }
        }

    }
    printf("joinPartAndPartsuppByPartkey Count: %d\n", (*outputCount));
    freeHashTable();
}

void PG_aggregate_by_qsort(memory* inputMemory, memory* outputMemory){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)inputMemory->buffer;
    int *inputCount = &(inputMemory->count);
    aggregated_part_partsupp_join *aggregatedOuputTuples = (aggregated_part_partsupp_join*)outputMemory->buffer;
    UINT32 *outputCount = &(outputMemory->count);
    aggregated_part_partsupp_join tempAggregatedOuputTuple ;
    UINT32 aggregateCount = 0;
    UINT32 distinctSuppkeyCount = 0;
    
    qsort(inputTuples, (*inputCount), sizeof(*inputTuples), comparePartPartSuppJoinElem);
    int i;
    for(i=0; i<(*inputCount);i++){
        strncpy(tempAggregatedOuputTuple.p_brand, inputTuples[i].p_brand, sizeof (inputTuples[i].p_brand));
        strncpy(tempAggregatedOuputTuple.p_type, inputTuples[i].p_type, sizeof (inputTuples[i].p_type));
        tempAggregatedOuputTuple.p_size = inputTuples[i].p_size;
        i++;
        distinctSuppkeyCount = 1;
        while ((i < (*inputCount)) &&
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
    (*outputCount) = aggregateCount;
    printf("PG_aggregate_by_qsort Count: %d\n", (*outputCount));
}
void OPT_aggregate_by_hash_partitioning_and_sorting(memory* inputMemory, memory* outputMemory){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)inputMemory->buffer;
    UINT32 *inputCount = &(inputMemory->count);
    aggregated_part_partsupp_join *aggregatedOuputTuples = (aggregated_part_partsupp_join*)outputMemory->buffer;
    UINT32 *outputCount = &(outputMemory->count);
    aggregated_part_partsupp_join tempAggregatedOuputTuple ;
    UINT32 aggregateCount = 0;
    UINT32 distinctSuppkeyCount = 0;
    int *positions = (int *)(scratchMemoryTwo->buffer);
    
#if 0    
    qsort(inputTuples, (*inputCount), sizeof(*inputTuples), comparePartPartSuppJoinElem);
#else
#ifdef VMALLOC
    sortMultiPivotAndUndo(vmPCM, inputTuples, 
            (*inputCount), 
            sizeof(*inputTuples), 
            comparePartPartSuppJoinElem,
            scratchMemoryTwo,
            100, 6000);
#else
    sortMultiHashAndUndo((char*)inputTuples, 
            (*inputCount), 
            sizeof(*inputTuples), 
            comparePartPartSuppJoinElem,
            (char*)positions,
            max(1, 2*((*inputCount)/MAX_THRESHOLD)), MAX_THRESHOLD/2, hashValue_from_ptr);
#endif
#endif
    int i;
    for (i = 0; i < (*inputCount); i++) {
            assert(positions[i] < (*inputCount));
            strncpy(tempAggregatedOuputTuple.p_brand, inputTuples[positions[i]].p_brand, sizeof(inputTuples[positions[i]].p_brand));
            strncpy(tempAggregatedOuputTuple.p_type, inputTuples[positions[i]].p_type, sizeof(inputTuples[positions[i]].p_type));
            tempAggregatedOuputTuple.p_size = inputTuples[positions[i]].p_size;
            i++;
            distinctSuppkeyCount=1;
            while((i<(*inputCount)) && 
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
    (*outputCount) = aggregateCount;
#if 0
    for (i = 0; i < (*outputCount); i++) {
        printf("Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", aggregatedOuputTuples[i].p_brand,
                aggregatedOuputTuples[i].p_type,
                aggregatedOuputTuples[i].p_size,
                aggregatedOuputTuples[i].distinct_ps_suppkey);
    }
#endif
    printf("OPT_aggregate_by_hash_partitioning_and_sorting Count: %d\n", (*outputCount));
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
void PG_sortFinal(memory *ioMemory){
    aggregated_part_partsupp_join *inputTuples = (aggregated_part_partsupp_join*)ioMemory->buffer;
    int *ioCount = &(ioMemory->count);
    qsort(inputTuples, (*ioCount), sizeof(*inputTuples), compareAggregatedPartPartsuppJoinElem);
    int i;
#if 1
    for (i = 0; i < (*ioCount); i++) {
        printf("Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", inputTuples[i].p_brand,
                inputTuples[i].p_type,
                inputTuples[i].p_size,
                inputTuples[i].distinct_ps_suppkey);
    }
#endif
    printf("PG_sortFinal Count: %d\n", (*ioCount));
}

void sortFinal(memory* inputMemory, memory* outputMemory){
    aggregated_part_partsupp_join *inputTuples = (aggregated_part_partsupp_join*)inputMemory->buffer;
    UINT32 *inputCount = &(inputMemory->count);
    aggregated_part_partsupp_join *sortedOutputTuples = (aggregated_part_partsupp_join*)outputMemory->buffer;
    int i;
    int *positions = (int *)(scratchMemoryTwo->buffer);
    
#ifdef VMALLOC
    sortMultiPivotAndUndo(vmPCM, inputTuples, 
            (*inputCount), 
            sizeof(*inputTuples), 
            compareAggregatedPartPartsuppJoinElem,
            scratchMemoryTwo, 
            40, 8000);
#else
    sortMultiPivotAndUndo((char*)inputTuples, 
            (*inputCount), 
            sizeof(*inputTuples), 
            compareAggregatedPartPartsuppJoinElem,
            (char*)positions, 
            max(1,2*((*inputCount)/MAX_THRESHOLD)), MAX_THRESHOLD/2); //Twicing the number of partitions and halving the threshold to enable safe merge
#endif
    
    int remainingSize = (*inputCount);
    int totalInitialSize = remainingSize;
    char *currOutPtr = scratchMemoryTwo->buffer;
    
    

#if 1
    for (i = 0; i < totalInitialSize; i++) {
        assert(positions[i] < totalInitialSize);
        printf("Brand: %s Type: %s, Size: %d suppkeyCount:%d\n", sortedOutputTuples[positions[i]].p_brand,
                sortedOutputTuples[positions[i]].p_type,
                sortedOutputTuples[positions[i]].p_size,
                sortedOutputTuples[positions[i]].distinct_ps_suppkey);
    }
#endif
    printf("sortFinal Count: %d\n", (*inputCount));
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
    scratchMemoryOne = (memory*)malloc(sizeof(memory));
    scratchMemoryOne->buffer = (char*)malloc(SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryOne != NULL);
    
    scratchMemoryTwo = (memory*)malloc(sizeof(memory));
    scratchMemoryTwo->buffer = (char*)malloc(SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryTwo != NULL);
    
    scratchMemoryThree = (memory*)malloc(sizeof(memory));
    scratchMemoryThree->buffer = (char*)malloc(SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryThree != NULL);
    
    scratchMemoryFour = (memory*)malloc(sizeof(memory));
    scratchMemoryFour->buffer = (char*)malloc(SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryFour != NULL);
    
    scratchMemoryFive = (memory*)malloc(sizeof(memory));
    scratchMemoryFive->buffer = (char*)malloc(SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryFive != NULL);
    
    fileReadBufferForWritesCreation = (char*)malloc(SCRATCH_MEMORY_SIZE);
    assert(fileReadBufferForWritesCreation != NULL);
    
    
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
    antiJoinPartsupplierSupplierTable(scratchMemoryThree, scratchMemoryFour, scratchMemoryTwo);
    flushDRAM();
    printf("Anti Join Over\n");
    fprintf(stderr, "***Anti Join Over***\n");
    joinPartAndPartsuppByPartkey(POSTGRES_QUERY, scratchMemoryOne, scratchMemoryTwo, scratchMemoryFive);
    flushDRAM();
    printf("joinPartAndPartsuppByPartkey Over\n");
    fprintf(stderr, "***joinPartAndPartsuppByPartkey Over***\n");
    PG_aggregate_by_qsort(scratchMemoryFive, scratchMemoryOne);
    flushDRAM();
    printf("GroupBy over\n");
    fprintf(stderr, "***GroupBy over***\n");
    PG_sortFinal(scratchMemoryOne);
    return (EXIT_SUCCESS);
}
#if 0
  int postgresQueryExecution_blue(){
    printf("\n\n\n\n\n");
    printf("***********************\n");
    printf("Posgres Query Execution\n");
    printf("*************************\n");
    printf("\n\n\n\n\n");
    
    joinPartAndPartsuppByPartkey_blue(POSTGRES_QUERY);
    flushDRAM();
    printf("joinPartAndPartsuppByPartkey Over\n");
    fprintf(stderr, "***joinPartAndPartsuppByPartkey Over***\n");
    antiJoinPartsupplierSupplierTable_blue();
    flushDRAM();
    printf("Anti Join Over\n");
    fprintf(stderr, "***Anti Join Over***\n");
    PG_aggregate_by_qsort();
    flushDRAM();
    printf("GroupBy over\n");
    fprintf(stderr, "***GroupBy over***\n");
    PG_sortFinal();
    return (EXIT_SUCCESS);
}
#endif

int optimizedQueryExecution(){
    printf("\n\n\n\n\n");
    printf("*************************\n");
    printf("Optimized Query Execution\n");
    printf("*************************\n");
    printf("\n\n\n\n\n");
    
    antiJoinPartsupplierSupplierTable(scratchMemoryThree, scratchMemoryFour, scratchMemoryTwo);
    flushDRAM();
    printf("Anti Join Over\n");
    fprintf(stderr, "***Anti Join Over***\n");
    joinPartAndPartsuppByPartkey(OPTIMIZED_QUERY, scratchMemoryOne, scratchMemoryTwo, scratchMemoryFive);
    flushDRAM();
    printf("joinPartAndPartsuppByPartkey Over\n");
    fprintf(stderr, "***joinPartAndPartsuppByPartkey Over***\n");
    OPT_aggregate_by_hash_partitioning_and_sorting(scratchMemoryFive, scratchMemoryOne);
    flushDRAM();
    printf("***GroupBy over***\n");
    fprintf(stderr, "***GroupBy over***\n");
    sortFinal(scratchMemoryOne, scratchMemoryOne);
    return (EXIT_SUCCESS);
}
#if 0

int optimizedQueryExecution_blue(){
    printf("\n\n\n\n\n");
    printf("*************************\n");
    printf("Optimized Query Execution\n");
    printf("*************************\n");
    printf("\n\n\n\n\n");
    
    joinPartAndPartsuppByPartkey_blue(OPTIMIZED_QUERY);
    flushDRAM();
    printf("joinPartAndPartsuppByPartkey Over\n");
    fprintf(stderr, "***joinPartAndPartsuppByPartkey Over***\n");
    antiJoinPartsupplierSupplierTable_blue();
    flushDRAM();
    printf("Anti Join Over\n");
    fprintf(stderr, "***Anti Join Over***\n");
    OPT_aggregate_by_hash_partitioning_and_sorting();
    flushDRAM();
    printf("***GroupBy over***\n");
    fprintf(stderr, "***GroupBy over***\n");
    sortFinal();
    return (EXIT_SUCCESS);
}
#endif 

int main(int argc, char** argv) {
   if( argc != 3 ){
       printf("\n\nUsage: ./pgsqlquery16 <0/1> <tables_dir> \n\n");
       return 1;
   }
   init();
   printf("sizeof (aggregated_part_partsupp_join) :%u\n", sizeof(aggregated_part_partsupp_join));
   printf("sizeof (part_partsupp_join_struct) :%u\n", sizeof(part_partsupp_join_struct));
   printf("sizeof (pageHash) :%u\n", sizeof(GB_pageHash));
   printf("sizeof (hashEntry) :%u\n", sizeof(GB_hashEntry));
   
   fprintf(stderr, "\n\n\n*************************************\n");
   fprintf(stderr, "Binary:%s Option [1]:%s [2]:%s\n", argv[0], argv[1], argv[2]);
   fprintf(stderr, "*************************************\n\n\n");
   
#if 1
   strcat(part_table_file, argv[2]);
   strcat(supplier_table_file, argv[2]);
   strcat(partsupplier_table_file, argv[2]);
   
   strcat(part_table_file, PART_TABLE_FILE);
   strcat(supplier_table_file, SUPPLIER_TABLE_FILE);
   strcat(partsupplier_table_file, PARTSUPPLIER_TABLE_FILE);
   
   scanAndFilterPartTable(scratchMemoryOne);
   printf("scanAndFilterPartTable Over\n");
   scanAndFilterSupplierTable(scratchMemoryThree);
   printf("scanAndFilterSupplierTable Over\n");
   scanPartsupplierTable(scratchMemoryFour);
   printf("scanPartsupplierTable Over\n");
   flushDRAM();
   printf("***Scan Over***\n"); 
   fprintf(stderr, "***Scan Over***\n"); 
   
   
   if(strcmp(argv[1],"0") == 0){
       postgresQueryExecution();
   }
   else{
       optimizedQueryExecution();
   }
#else
   postgresQueryExecution();
#endif
   flushDRAM();
   return (EXIT_SUCCESS);  
}

