/* 
 * File:   main.c
 * Author: root
 *
 * Created on 24 September, 2013, 9:55 AM
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/fcntl.h>
#include <sys/types.h>

#include "globals.h"
#include "parseStructs.h"
#include "vmalloc.h"
#include "hashTable.h"
#include "pcm_ptlsim.h"

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

int scanAndFilterPartTable(){
    part *pitem = (part*)vmalloc(vmPCM, sizeof(part));
    FILE *fpIn = fopen(PART_TABLE_FILE, "r");
    part *pitemScratch = (part*)scratchMemoryLeft;
    printf("pitem Add:%p\n", pitem);
    scratchMemoryLeftCount = 0;
    int ret=fread(pitem, sizeof(*pitem), 1, fpIn);
    while(ret != 0){
        if( strncmp(pitem->p_brand, "Brand#35", 8) != 0 &&
                strstr(pitem->p_type, "ECONOMY BURNISHED") == NULL &&
                (pitem->p_size == 14 ||
                pitem->p_size == 7 ||
                pitem->p_size == 21 ||
                pitem->p_size == 24 ||
                pitem->p_size == 35 ||
                pitem->p_size == 33 ||
                pitem->p_size == 2 ||
                pitem->p_size == 20)){
            
                pitemScratch[scratchMemoryLeftCount++] = *pitem; 
        }
        ret=fread(pitem, sizeof(*pitem), 1, fpIn);
    }
    fclose(fpIn);
    vmfree(vmPCM, pitem);
}
int scanAndFilterSupplierTable(){
    /*We have used grep to find the s_suppkey of lines matching 
     * '%Customer%Complaints' and put it into an array. This is because 
     * this saves a lot of hassles in computing such expression in C. */ 
    
    supplier *sitem = (supplier*)vmalloc(vmPCM, sizeof(supplier));
    FILE *fpSupp = fopen(SUPPLIER_TABLE_FILE, "r");
    
    /*Dummy reads to just increase PCM write count */ 
    while(fread(sitem, sizeof(*sitem), 1, fpSupp) != 0);
    
    filtered_s_suppkey[0] = 358;
    filtered_s_suppkey[1] = 2820;
    filtered_s_suppkey[2] = 3804;
    filtered_s_suppkey[3] = 9504;
}

int scanAndFilterPartsupplierTable(){
    partsupp *psitem = (partsupp*)vmalloc(vmPCM, sizeof(partsupp));
    FILE *fpPartsupp = fopen(PARTSUPPLIER_TABLE_FILE, "r");
    partsupp *psitemScratch = (partsupp*)scratchMemoryRight;
    UINT32 index;
    BOOL discard;
    scratchMemoryRightCount = 0;
    int ret=fread(psitem, sizeof(*psitem), 1, fpPartsupp);
    while(ret != 0){
        discard = 0;
        for(index=0; index<FILTERED_S_SUPPKEY_COUNT; index++){
            if(psitem->ps_suppkey == filtered_s_suppkey[index]){
                discard = 1;
                break;
            } 
        }
        if (!discard){
            psitemScratch[scratchMemoryRightCount++] = *psitem; 
        }
            
      ret=fread(psitem, sizeof(*psitem), 1, fpPartsupp);  
    }
#if 0
    part * pitemScratch = (part*)scratchMemoryLeft;
    for(index=0; index<scratchMemoryLeftCount; index++){
        printf("%d\n ", pitemScratch[index].p_partkey);
    }
#endif
    fclose(fpPartsupp);
    vmfree(vmPCM, psitem);
}

int joinPartAndPartsuppByPartkey(){
    initHT(vmPCM, BUCKET_COUNT, OPT_ENTRIES_PER_PAGE);
    int index;
    void *page = NULL, *lastIndex = NULL;
    part *pitem = (part*) scratchMemoryLeft;
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
    part *tuplePtr;
    partsupp *psitem = (partsupp*) scratchMemoryRight;
    part_partsupp_join_struct *ppjsitem = (part_partsupp_join_struct*)scratchMemoryOut;
    scratchMemoryOutCount = 0;
    printf("Done Inserting. Now Joins\n");
    /*Ending Simulation because simulator crashing at some instruction here*/
    SimEnd();
    for (index = 0; index < scratchMemoryRightCount; index++) {
        page = NULL, lastIndex = NULL;
        if(index%100 == 0){
            printf("Completed: %d\n", index);
        }
        while (searchHashEntry((char*) &(psitem[index].ps_partkey),
                sizeof (psitem[index].ps_partkey),
                (void**) &tuplePtr, &page, &lastIndex) == 1) {
            
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
}

void init(){
    scratchMemoryLeft = (char*)malloc(SCRATCH_MEMORY_SIZE);
    scratchMemoryRight = (char*)malloc(SCRATCH_MEMORY_SIZE);
    scratchMemoryOut = (char*)malloc(SCRATCH_MEMORY_SIZE);
    
    pcmMemory = (char*)malloc(PCM_MEMORY_SIZE);
    vmPCM = vmemopen(pcmMemory, PCM_MEMORY_SIZE, 0);
    PCMRange((unsigned long long)pcmMemory, (unsigned long long)(pcmMemory + PCM_MEMORY_SIZE));
    printf("PCM Range Set to [Beg:%p End:%p]\n", pcmMemory, (pcmMemory + PCM_MEMORY_SIZE));
    
    
    
}

/*
 * 
 */
int main(int argc, char** argv) {
    init();
    SimBegin();
    scanAndFilterPartTable();
    printf("scanAndFilterPartTable Over\n");
    scanAndFilterSupplierTable();
    printf("scanAndFilterSupplierTable Over\n");
    scanAndFilterPartsupplierTable();
    printf("scanAndFilterPartsupplierTable Over\n");
    joinPartAndPartsuppByPartkey();
    printf("joinPartAndPartsuppByPartkey Over\n");
    SimEnd();
    return (EXIT_SUCCESS);
}

