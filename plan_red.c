#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<assert.h>

#include "constants.h"
#include "globals.h"
#include "parseStructs.h"
#include "partitioning.h"

#define POSTGRES_QUERY 0
#define OPTIMIZED_QUERY 1

extern UINT32 scratchMemoryOneCount;
extern UINT32 scratchMemoryTwoCount;
extern UINT32 scratchMemoryThreeCount;
extern UINT32 scratchMemoryFourCount;
extern UINT32 scratchMemoryFiveCount;

extern char *scratchMemoryOne;
extern char *scratchMemoryTwo;
extern char *scratchMemoryThree;
extern char *scratchMemoryFour;
extern char *scratchMemoryFive;


int antiJoinPartsupplierSupplierTable_red(){
    partsupp psitem;
    
    int *filtered_s_suppkey = (int*)scratchMemoryThree;
    projectedPartsuppItem *psitemScratch = (projectedPartsuppItem*)scratchMemoryTwo;
    UINT32 index;
    scratchMemoryTwoCount = 0;
    initHT(scratchMemoryThreeCount/ENTRIES_PER_BUCKET, OPT_ENTRIES_PER_PAGE);
    for (index = 0; index < scratchMemoryThreeCount; index++) {
        insertHashEntry((void*) &(filtered_s_suppkey[index]),
                (char*) &(filtered_s_suppkey[index]),
                sizeof *(filtered_s_suppkey));
        
    }
    partsupp *psitemScratchInput = (partsupp*)scratchMemoryFour;
    for(index = 0; index < scratchMemoryFourCount; index++){
      
        psitem = psitemScratchInput[index];
        void *lastPage = NULL, *lastIndex = NULL;
        char *tuplePtr;
        if (searchHashEntry((char*) &(psitem.ps_suppkey),
                    sizeof (psitem.ps_suppkey),
                    (void**) &tuplePtr, &lastPage, &lastIndex) == 0) {
            psitemScratch[scratchMemoryTwoCount].ps_suppkey = psitem.ps_suppkey;
            psitemScratch[scratchMemoryTwoCount].ps_partkey = psitem.ps_partkey;
            scratchMemoryTwoCount++;
        }
    }
    printf("antiJoin Partsupplier Supplier Table Count: %d\n", scratchMemoryTwoCount);
    freeHashTable();
}
#if 0
int joinPartAndPartsuppByPartkey_red(UINT8 queryType){
    if(queryType == POSTGRES_QUERY){
#ifdef VMALLOC
        initHT(vmPCM, scratchMemoryOneCount/ENTRIES_PER_BUCKET, PSQL_ENTRIES_PER_PAGE);
#else
          initHT(scratchMemoryOneCount/ENTRIES_PER_BUCKET, PSQL_ENTRIES_PER_PAGE);
#endif
    }
    else{
#ifdef VMALLOC
        initHT(vmPCM, BUCKET_COUNT, OPT_ENTRIES_PER_PAGE);
#else
          initHT(scratchMemoryOneCount/ENTRIES_PER_BUCKET, OPT_ENTRIES_PER_PAGE);
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
    part_partsupp_join_struct *ppjsitem = (part_partsupp_join_struct*)scratchMemoryFive;
    scratchMemoryFiveCount = 0;
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
                
                strncpy(ppjsitem[scratchMemoryFiveCount].p_brand ,
                        tuplePtr->p_brand, 
                        sizeof(ppjsitem[scratchMemoryFiveCount].p_brand));
                ppjsitem[scratchMemoryFiveCount].p_size = tuplePtr->p_size;
                strncpy(ppjsitem[scratchMemoryFiveCount].p_type, 
                        tuplePtr->p_type, 
                        sizeof(ppjsitem[scratchMemoryFiveCount].p_type));
                ppjsitem[scratchMemoryFiveCount].ps_suppkey = psitem[index].ps_suppkey;
                assert(ppjsitem[scratchMemoryFiveCount].p_size != 0);
                scratchMemoryFiveCount++;
            }
        }

    }
    printf("joinPartAndPartsuppByPartkey_red Count: %d\n", scratchMemoryFiveCount);
    freeHashTable();
}
#else
projectedPartItem *pitem_starting;
projectedPartsuppItem *psitem_starting;
int *pos_pitem_starting_addr_for_partition;
int *pos_psitem_starting_addr_for_partition;

int compare_super_pos_pitem(const void *p1, const void *p2){
    int pos_indicated_by_super_pos_array_1 = *(int*)p1;
    int pos_indicated_by_pos_array_1 = pos_pitem_starting_addr_for_partition[pos_indicated_by_super_pos_array_1];
    projectedPartItem pitem1 = pitem_starting[pos_indicated_by_pos_array_1];
    
    int pos_indicated_by_super_pos_array_2 = *(int*)p2;
    int pos_indicated_by_pos_array_2 = pos_pitem_starting_addr_for_partition[pos_indicated_by_super_pos_array_2];
    projectedPartItem pitem2 = pitem_starting[pos_indicated_by_pos_array_2];
    
    
    return pitem1.p_partkey - pitem2.p_partkey;
}
int compare_super_pos_psitem(const void *p1, const void *p2){
    
    int pos_indicated_by_super_pos_array_1 = *(int*)p1;
    int pos_indicated_by_pos_array_1 = pos_psitem_starting_addr_for_partition[pos_indicated_by_super_pos_array_1];
    projectedPartsuppItem psitem1 = psitem_starting[pos_indicated_by_pos_array_1];
    
    int pos_indicated_by_super_pos_array_2 = *(int*)p2;
    int pos_indicated_by_pos_array_2 = pos_psitem_starting_addr_for_partition[pos_indicated_by_super_pos_array_2];
    projectedPartsuppItem psitem2 = psitem_starting[pos_indicated_by_pos_array_2];
        
    return psitem1.ps_partkey - psitem2.ps_partkey;
}
void sort_merge_pitem_psitem(char* pitem_starting_addr, int* pos_pitem_starting_addr, int start_pos_pitem, int  end_pos_pitem, 
        char* psitem_starting_addr, int* pos_psitem_starting_addr, int start_pos_psitem, int end_pos_psitem,
        int *super_pos_array_start_pitem, int *super_pos_array_start_psitem){
    int i, j;
    pitem_starting = (projectedPartItem*)pitem_starting_addr;
    psitem_starting = (projectedPartsuppItem*)psitem_starting_addr;
    
    pos_pitem_starting_addr_for_partition = pos_pitem_starting_addr + start_pos_pitem;
    pos_psitem_starting_addr_for_partition = pos_psitem_starting_addr + start_pos_psitem;
    
    for(i=0; i<end_pos_psitem-start_pos_psitem; i++){
        printf("Before sort psitem partkey: %d\n", ((projectedPartsuppItem*)psitem_starting_addr)[pos_psitem_starting_addr_for_partition[super_pos_array_start_psitem[i]]].ps_partkey);
    }
    qsort(super_pos_array_start_pitem, end_pos_pitem-start_pos_pitem, sizeof(int), compare_super_pos_pitem);
    qsort(super_pos_array_start_psitem, end_pos_psitem-start_pos_psitem, sizeof(int), compare_super_pos_psitem);
    
    for(i=0; i<end_pos_psitem-start_pos_psitem; i++){
        printf("After sort psitem partkey: %d\n", ((projectedPartsuppItem*)psitem_starting_addr)[pos_psitem_starting_addr_for_partition[super_pos_array_start_psitem[i]]].ps_partkey);
    }
}

int compare_pitem_by_partkey(const void *p1, const void *p2){
    return ((projectedPartItem*)p1)->p_partkey - ((projectedPartItem*)p2)->p_partkey;
}
int compare_psitem_by_partkey(const void *p1, const void *p2){
    return ((projectedPartsuppItem*)p1)->ps_partkey - ((projectedPartsuppItem*)p2)->ps_partkey;
}

UINT32 hashValue_pitem_by_partkey(void *ptr){
    return ((projectedPartItem*)ptr)->p_partkey;
}

UINT32 hashValue_psitem_by_partkey(void *ptr){
    return ((projectedPartsuppItem*)ptr)->ps_partkey;
}
int joinPartAndPartsuppByPartkey_red(UINT8 queryType){
    int index;
    projectedPartItem *pitem = (projectedPartItem*) scratchMemoryOne;
    projectedPartsuppItem *psitem = (projectedPartsuppItem*) scratchMemoryTwo;
    
    printf("Join:[LeftRecordCount:%d, RightRecordCount:%d]\n", 
            scratchMemoryOneCount,
            scratchMemoryTwoCount);
    int numPartitions_pitem = ((2*sizeof(int)+sizeof(*pitem))* scratchMemoryOneCount) * 3/ DRAM_MEMORY_SIZE;
    int numPartitions_psitem = 3*((2*sizeof(int)+sizeof(*psitem))* scratchMemoryTwoCount)/ DRAM_MEMORY_SIZE;
    
    int *partBeg_pitem = (int*)malloc((numPartitions_pitem + 1)*sizeof(int));
    int *partBeg_psitem = (int*)malloc((numPartitions_psitem + 1)*sizeof(int));
    
    int *pos_pitem = (int*)malloc(scratchMemoryOneCount * sizeof(int));
    int *pos_psitem = (int*)malloc(scratchMemoryTwoCount * sizeof(int));
    
    assert(partBeg_pitem != NULL && partBeg_psitem != NULL && pos_pitem != NULL && pos_psitem != NULL);
    
    for(index = 0; index<scratchMemoryOneCount; index++){
        pos_pitem[index] = index;
    }
    for(index = 0; index<scratchMemoryTwoCount; index++){
        pos_psitem[index] = index;
    }

    if (numPartitions_pitem != 0) {
        numPartitions_pitem = partition_with_hash((char*) pitem, scratchMemoryOneCount, sizeof (*pitem), compare_pitem_by_partkey, numPartitions_pitem, partBeg_pitem, pos_pitem, scratchMemoryOneCount / (2 * numPartitions_pitem), hashValue_pitem_by_partkey);

    }
    if (numPartitions_psitem != 0) {
    numPartitions_psitem = partition_with_hash((char*)psitem,scratchMemoryTwoCount, sizeof(*psitem), compare_psitem_by_partkey, numPartitions_psitem, partBeg_psitem, pos_psitem, scratchMemoryTwoCount/(2*numPartitions_psitem), hashValue_psitem_by_partkey); 
    }

    int max_partition_size_pitem = 0, max_partition_size_psitem = 0;
    
    for(index=0; index<numPartitions_pitem; index ++){
        if(partBeg_pitem[index+1] - partBeg_pitem[index] > max_partition_size_pitem){
            max_partition_size_pitem = partBeg_pitem[index+1] - partBeg_pitem[index];
        }
    }
    for(index=0; index<numPartitions_psitem; index ++){
        if(partBeg_psitem[index+1] - partBeg_psitem[index] > max_partition_size_psitem){
            max_partition_size_psitem = partBeg_psitem[index+1] - partBeg_psitem[index];
        }
    }
    int *super_pos_beg_pitem = (int*)malloc(max_partition_size_pitem* sizeof(int));
    int *super_pos_beg_psitem = (int*)malloc(max_partition_size_psitem* sizeof(int));
    
    for(index = 0; index<max_partition_size_pitem; index++){
        super_pos_beg_pitem[index] = index;
    }
    for(index = 0; index<max_partition_size_psitem; index++){
        super_pos_beg_psitem[index] = index;
    }
    sort_merge_pitem_psitem((char*)pitem, pos_pitem, partBeg_pitem[0], partBeg_pitem[1], (char*)psitem, pos_psitem, partBeg_psitem[0], partBeg_psitem[1],super_pos_beg_pitem, super_pos_beg_psitem);

#if 0    
    projectedPartItem *tuplePtr;
    part_partsupp_join_struct *ppjsitem = (part_partsupp_join_struct*)scratchMemoryFive;
    scratchMemoryFiveCount = 0;
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
                
                strncpy(ppjsitem[scratchMemoryFiveCount].p_brand ,
                        tuplePtr->p_brand, 
                        sizeof(ppjsitem[scratchMemoryFiveCount].p_brand));
                ppjsitem[scratchMemoryFiveCount].p_size = tuplePtr->p_size;
                strncpy(ppjsitem[scratchMemoryFiveCount].p_type, 
                        tuplePtr->p_type, 
                        sizeof(ppjsitem[scratchMemoryFiveCount].p_type));
                ppjsitem[scratchMemoryFiveCount].ps_suppkey = psitem[index].ps_suppkey;
                assert(ppjsitem[scratchMemoryFiveCount].p_size != 0);
                scratchMemoryFiveCount++;
            }
        }

    }
    
    printf("joinPartAndPartsuppByPartkey_red Count: %d\n", scratchMemoryFiveCount);
    freeHashTable();
#endif
}
#endif
