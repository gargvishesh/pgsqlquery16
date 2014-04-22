#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include <assert.h>

#include "constants.h"
#include "globals.h"
#include "parseStructs.h"

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


int joinPartAndPartsuppByPartkey_blue(UINT8 queryType){
    if (queryType == POSTGRES_QUERY) {
#ifdef VMALLOC
        initHT(vmPCM, scratchMemoryOneCount / ENTRIES_PER_BUCKET, PSQL_ENTRIES_PER_PAGE);
#else
        initHT(scratchMemoryOneCount / ENTRIES_PER_BUCKET, PSQL_ENTRIES_PER_PAGE);
#endif
    } else {
#ifdef VMALLOC
        initHT(vmPCM, BUCKET_COUNT, OPT_ENTRIES_PER_PAGE);
#else
        initHT(scratchMemoryOneCount / ENTRIES_PER_BUCKET, OPT_ENTRIES_PER_PAGE);
#endif
    }

    int index;
    void *lastPage = NULL, *lastIndex = NULL;
    projectedPartItem *pitem = (projectedPartItem*) scratchMemoryOne;
    printf("Join:[LeftRecordCount:%d, RightRecordCount:%d]\n",
            scratchMemoryOneCount,
            scratchMemoryFourCount);
    for (index = 0; index < scratchMemoryOneCount; index++) {
        insertHashEntry((void*) &(pitem[index]),
                (char*) &(pitem[index].p_partkey),
                sizeof (pitem[index].p_partkey));

    }
    projectedPartItem *tuplePtr;
    partsupp *psitem = (partsupp*) scratchMemoryFour;
    part_partsupp_join_struct *ppjsitem = (part_partsupp_join_struct*) scratchMemoryTwo;
    scratchMemoryTwoCount = 0;
    printf("Done Inserting. Now Joins. Outer Records Count: %d\n", scratchMemoryFourCount);

    for (index = 0; index < scratchMemoryFourCount; index++) {

        lastPage = NULL, lastIndex = NULL;
        if (index % 10000 == 0) {
            printf("Completed : %d\n", index);
        }
        while (searchHashEntry((char*) &(psitem[index].ps_partkey),
                sizeof (psitem[index].ps_partkey),
                (void**) &tuplePtr, &lastPage, &lastIndex) == 1) {

            if (tuplePtr->p_partkey == psitem[index].ps_partkey) {

                strncpy(ppjsitem[scratchMemoryTwoCount].p_brand,
                        tuplePtr->p_brand,
                        sizeof (ppjsitem[scratchMemoryTwoCount].p_brand));
                ppjsitem[scratchMemoryTwoCount].p_size = tuplePtr->p_size;
                strncpy(ppjsitem[scratchMemoryTwoCount].p_type,
                        tuplePtr->p_type,
                        sizeof (ppjsitem[scratchMemoryTwoCount].p_type));
                ppjsitem[scratchMemoryTwoCount].ps_suppkey = psitem[index].ps_suppkey;
                assert(ppjsitem[scratchMemoryTwoCount].p_size != 0);
                scratchMemoryTwoCount++;
            }
        }

    }
    printf("joinPartAndPartsuppByPartkey_red Count: %d\n", scratchMemoryTwoCount);
    freeHashTable();
}
int antiJoinPartsupplierSupplierTable_blue(){
    part_partsupp_join_struct ppjsitem;
    
    int *filtered_s_suppkey = (int*)scratchMemoryThree;
    part_partsupp_join_struct *ppjsitemScratch = (part_partsupp_join_struct*)scratchMemoryFive;
    UINT32 index;
    scratchMemoryFiveCount = 0;
    initHT(scratchMemoryThreeCount/ENTRIES_PER_BUCKET, OPT_ENTRIES_PER_PAGE);
    for (index = 0; index < scratchMemoryThreeCount; index++) {
        insertHashEntry((void*) &(filtered_s_suppkey[index]),
                (char*) &(filtered_s_suppkey[index]),
                sizeof *(filtered_s_suppkey));
        
    }
    part_partsupp_join_struct *ppjsitemScratchInput = (part_partsupp_join_struct*)scratchMemoryTwo;
    for(index = 0; index < scratchMemoryTwoCount; index++){
      
        ppjsitem = ppjsitemScratchInput[index];
        void *lastPage = NULL, *lastIndex = NULL;
        char *tuplePtr;
        if (searchHashEntry((char*) &(ppjsitem.ps_suppkey),
                    sizeof (ppjsitem.ps_suppkey),
                    (void**) &tuplePtr, &lastPage, &lastIndex) == 0) {
            ppjsitemScratch[scratchMemoryFiveCount] = ppjsitem;
            scratchMemoryFiveCount++;
        }
    }
    printf("Antijoin Part-Partsupp Join with Supplier Count: %d\n", scratchMemoryFiveCount);
    freeHashTable();
}

