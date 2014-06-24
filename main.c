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
#include "btree.h"
//#include "clock.h"

#define POSTGRES_QUERY 0
#define OPTIMIZED_QUERY 1

#define max(i,j) ((i>j)?i:j)

UINT32 scratchMemoryOneCount;
UINT32 scratchMemoryTwoCount;
UINT32 scratchMemoryThreeCount;
UINT32 scratchMemoryFourCount;
UINT32 scratchMemoryFiveCount;


UINT32 bucketCountForAggregation;

char *scratchMemoryOne;
char *scratchMemoryTwo;
char *scratchMemoryThree;
char *scratchMemoryFour;
char *scratchMemoryFive;

char *pcmMemory;
part_partsupp_join_struct *inputTupleStart;

char part_table_file[100];
char supplier_table_file[100];
char partsupplier_table_file[100];

#ifdef VMALLOC
Vmalloc_t *vmPCM; 
#endif

int scanAndFilterPartTable(float selectivityConstant) {
    part pitem;
    fprintf(stderr, "Selectivity Constant [Part] : %f\n", selectivityConstant);
    printf("Selectivity Constant [Part] : %f\n", selectivityConstant);
    int fpIn = open(part_table_file, O_RDONLY);
    int totalCount=0;
    assert(fpIn != -1);
    projectedPartItem *pitemScratch = (projectedPartItem*) scratchMemoryOne;
    scratchMemoryOneCount = 0;
    while (read(fpIn, &pitem, sizeof (pitem)* 1) != 0) {
/*These changed introduced in order to do Picasso style Plan Diagram 
 * instead of fixed selectivity*/
#if 0
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
#endif
            totalCount++;
            if (pitem.p_retailprice <= selectivityConstant) {
                strncpy(pitemScratch[scratchMemoryOneCount].p_brand, pitem.p_brand, sizeof (pitem.p_brand));
                pitemScratch[scratchMemoryOneCount].p_partkey = pitem.p_partkey;
                pitemScratch[scratchMemoryOneCount].p_size = pitem.p_size;
                strncpy(pitemScratch[scratchMemoryOneCount].p_type, pitem.p_type, sizeof (pitem.p_type));
                scratchMemoryOneCount++;
            }
        }
        printf("scanAndFilterPartTable totalCount: %d, Count: %d\n", totalCount, scratchMemoryOneCount);
        close(fpIn);
    }
int scanAndFilterSupplierTable(float selectivityConstant){
    /*We have used grep to find the s_suppkey of lines matching 
     * '%Customer%Complaints' and put it into an array. This is because 
     * this saves a lot of hassles in computing such expression in C. */ 
    
    supplier sitem;
    fprintf(stderr, "Selectivity Constant [Supplier] : %f\n", selectivityConstant);
    printf("Selectivity Constant [Supplier] : %f\n", selectivityConstant);
    int fpSupp = open(supplier_table_file, O_RDONLY);
    int *filtered_s_suppkey = (int*)scratchMemoryThree;
    scratchMemoryThreeCount = 0;
    int totalCount=0;
    /*Dummy reads to just increase PCM write count */ 
    while(read(fpSupp, &sitem, sizeof(sitem)) != 0){
        totalCount++;
        if(sitem.s_acctbal <= selectivityConstant ){
            filtered_s_suppkey[scratchMemoryThreeCount++] = sitem.s_suppkey;
        }
    }
    close(fpSupp);
    printf("scanAndFilterSupplierTable totalCount: %d Count: %d\n", totalCount, scratchMemoryThreeCount);
    
}
int scanPartsupplierTable(){
    int fpPartsupp = open(partsupplier_table_file, O_RDONLY);
    assert(fpPartsupp != -1);
    partsupp psitem;
    partsupp *psitemScratch = (partsupp*)scratchMemoryFour;
    scratchMemoryFourCount = 0;
    while(read(fpPartsupp, &psitem, sizeof(psitem)*1) != 0){
        psitemScratch[scratchMemoryFourCount++] = psitem;
    }
    close(fpPartsupp);
    printf("scanPartsupplierTable Count: %d\n", scratchMemoryFourCount);
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
void PG_aggregate_by_qsort(){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)scratchMemoryFive;
    aggregated_part_partsupp_join tempAggregatedOuputTuple ;
    aggregated_part_partsupp_join *aggregatedOuputTuples = (aggregated_part_partsupp_join*)scratchMemoryOne;
    UINT32 aggregateCount = 0;
    UINT32 distinctSuppkeyCount = 0;
    
    qsort(inputTuples, scratchMemoryFiveCount, sizeof(*inputTuples), comparePartPartSuppJoinElem);
    int i;
    for(i=0; i<scratchMemoryFiveCount;i++){
        strncpy(tempAggregatedOuputTuple.p_brand, inputTuples[i].p_brand, sizeof (inputTuples[i].p_brand));
        strncpy(tempAggregatedOuputTuple.p_type, inputTuples[i].p_type, sizeof (inputTuples[i].p_type));
        tempAggregatedOuputTuple.p_size = inputTuples[i].p_size;
        i++;
        distinctSuppkeyCount = 1;
        while ((i < scratchMemoryFiveCount) &&
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
    printf("PG_aggregate_by_qsort Count: %d\n", scratchMemoryOneCount);
}
void OPT_aggregate_by_hash_partitioning_and_sorting(){
    part_partsupp_join_struct *inputTuples = (part_partsupp_join_struct*)scratchMemoryFive;
    aggregated_part_partsupp_join tempAggregatedOuputTuple ;
    aggregated_part_partsupp_join *aggregatedOuputTuples = (aggregated_part_partsupp_join*)scratchMemoryOne;
    UINT32 aggregateCount = 0;
    UINT32 distinctSuppkeyCount = 0;
#if 0    
    qsort(inputTuples, scratchMemoryFiveCount, sizeof(*inputTuples), comparePartPartSuppJoinElem);
#else
#ifdef VMALLOC
    sortMultiPivotAndUndo(vmPCM, inputTuples, 
            scratchMemoryFiveCount, 
            sizeof(*inputTuples), 
            comparePartPartSuppJoinElem,
            scratchMemoryTwo,
            100, 6000);
#else
    sortMultiHashAndUndo((char*)inputTuples, 
            scratchMemoryFiveCount, 
            sizeof(*inputTuples), 
            comparePartPartSuppJoinElem,
            scratchMemoryTwo,
            max(1, 2*(scratchMemoryFiveCount/MAX_THRESHOLD)), MAX_THRESHOLD/2, hashValue_from_ptr);
#endif
#endif
    int i;
    int *positions = (int *)(scratchMemoryTwo);
    for (i = 0; i < scratchMemoryFiveCount; i++) {
            assert(positions[i] < scratchMemoryFiveCount);
            strncpy(tempAggregatedOuputTuple.p_brand, inputTuples[positions[i]].p_brand, sizeof(inputTuples[positions[i]].p_brand));
            strncpy(tempAggregatedOuputTuple.p_type, inputTuples[positions[i]].p_type, sizeof(inputTuples[positions[i]].p_type));
            tempAggregatedOuputTuple.p_size = inputTuples[positions[i]].p_size;
            i++;
            distinctSuppkeyCount=1;
            while((i<scratchMemoryFiveCount) && 
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
    printf("OPT_aggregate_by_hash_partitioning_and_sorting Count: %d\n", scratchMemoryOneCount);
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
            max(1,2*(scratchMemoryOneCount/MAX_THRESHOLD)), MAX_THRESHOLD/2); //Twicing the number of partitions and halving the threshold to enable safe merge
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
    
    scratchMemoryFive = (char*)malloc(SCRATCH_MEMORY_SIZE);
    assert(scratchMemoryFive != NULL);
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

 int postgresQueryExecution_red(){
    printf("\n\n\n\n\n");
    printf("***********************\n");
    printf("Posgres Query Execution\n");
    printf("*************************\n");
    printf("\n\n\n\n\n");
    antiJoinPartsupplierSupplierTable_red();
    flushDRAM();
    printf("Anti Join Over\n");
    fprintf(stderr, "***Anti Join Over***\n");
    joinPartAndPartsuppByPartkey_red(POSTGRES_QUERY);
    flushDRAM();
    printf("joinPartAndPartsuppByPartkey Over\n");
    fprintf(stderr, "***joinPartAndPartsuppByPartkey Over***\n");
    PG_aggregate_by_qsort();
    flushDRAM();
    printf("GroupBy over\n");
    fprintf(stderr, "***GroupBy over***\n");
    PG_sortFinal();
    return (EXIT_SUCCESS);
}
 
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

int optimizedQueryExecution_red(){
    printf("\n\n\n\n\n");
    printf("*************************\n");
    printf("Optimized Query Execution\n");
    printf("*************************\n");
    printf("\n\n\n\n\n");
    
    antiJoinPartsupplierSupplierTable_red();
    flushDRAM();
    printf("Anti Join Over\n");
    fprintf(stderr, "***Anti Join Over***\n");
    joinPartAndPartsuppByPartkey_red(OPTIMIZED_QUERY);
    flushDRAM();
    printf("joinPartAndPartsuppByPartkey Over\n");
    fprintf(stderr, "***joinPartAndPartsuppByPartkey Over***\n");
    OPT_aggregate_by_hash_partitioning_and_sorting();
    flushDRAM();
    printf("***GroupBy over***\n");
    fprintf(stderr, "***GroupBy over***\n");
    sortFinal();
    return (EXIT_SUCCESS);
}

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
int main(int argc, char** argv) {
   if( argc != 6 ){
       printf("\n\nUsage: ./pgsqlquery16 <0/1> <tables_dir> <part_sel> <supp_sel> <plan (0(red)/1(blue))>\n\n");
       return 1;
   }
   init();
   printf("sizeof (aggregated_part_partsupp_join) :%u\n", sizeof(aggregated_part_partsupp_join));
   printf("sizeof (part_partsupp_join_struct) :%u\n", sizeof(part_partsupp_join_struct));
   printf("sizeof (pageHash) :%u\n", sizeof(GB_pageHash));
   printf("sizeof (hashEntry) :%u\n", sizeof(GB_hashEntry));
   
   fprintf(stderr, "\n\n\n*************************************\n");
   fprintf(stderr, "Binary:%s Option [1]:%s [2]:%s [3]:%s [4]:%s [5]\n", argv[0], argv[1], argv[2], argv[3], argv[4], argv[5]);
   fprintf(stderr, "*************************************\n\n\n");
   
#if 1
   strcat(part_table_file, argv[2]);
   strcat(supplier_table_file, argv[2]);
   strcat(partsupplier_table_file, argv[2]);
   
   strcat(part_table_file, PART_TABLE_FILE);
   strcat(supplier_table_file, SUPPLIER_TABLE_FILE);
   strcat(partsupplier_table_file, PARTSUPPLIER_TABLE_FILE);
   
   scanAndFilterPartTable(atof(argv[3]));
   printf("scanAndFilterPartTable Over\n");
   scanAndFilterSupplierTable(atof(argv[4]));
   printf("scanAndFilterSupplierTable Over\n");
   scanPartsupplierTable();
   printf("scanPartsupplierTable Over\n");
   flushDRAM();
   printf("***Scan Over***\n"); 
   fprintf(stderr, "***Scan Over***\n"); 
   
   
   if (strcmp(argv[1], "0") == 0) {
       if(strcmp(argv[5], "0") == 0){
           postgresQueryExecution_red();
       }else{
           postgresQueryExecution_blue();
       }
   
       
   }else{
       if(strcmp(argv[5], "0") == 0){
           optimizedQueryExecution_red();
       }else{
           optimizedQueryExecution_blue();
       }
   
       
   }
#else
   postgresQueryExecution();
#endif
   flushDRAM();
   return (EXIT_SUCCESS);  
}

