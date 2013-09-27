/* 
 * File:   main.c
 * Author: root
 *
 * Created on 24 September, 2013, 9:55 AM
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "globals.h"
#include "parseStructs.h"
#include "vmalloc.h"
#include "hashTable.h"

#define FILTERED_S_SUPPKEY_COUNT 4
UINT32 scratchMemoryLeftCount;
UINT32 scratchMemoryRightCount;

char *scratchMemoryLeft;
char *scratchMemoryRight;
char *pcmMemory;
int filtered_s_suppkey[FILTERED_S_SUPPKEY_COUNT];

Vmalloc_t *vmPCM; 

int scanAndFilterPartTable(){
    part *pitem = (part*)vmalloc(vmPCM, sizeof(part));
    FILE *fpIn = fopen(PART_TABLE_FILE, "r");
    part *pitemScratch = (part*)scratchMemoryLeft;
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
    initHT(vmPCM, 256);
    int index;
    void *page = NULL, *lastIndex = NULL;
    part *pitem = (part*)scratchMemoryLeft;
    part *tuplePtr;
    for(index=0; index<4; index++){
        insertHashEntry((void*)&(pitem[index]), 
                        (char*)&(pitem[index].p_partkey), 
                        sizeof(pitem[index].p_partkey));
        printf("[index:%d partkey:%d] Inserted at :%p\n", index, pitem[index].p_partkey
                                                       ,&(pitem[index]));
        page = NULL, lastIndex = NULL;
        while(searchHashEntry((char*)&(pitem[index].p_partkey), 
                        sizeof(pitem[index].p_partkey),
                        (void**)&tuplePtr, &page, &lastIndex) == 1){
            printf("Found at :%p\n", tuplePtr);
        }
        
            //printf("Not found \n");
        
    }
    
    
}

void init(){
    scratchMemoryLeft = (char*)malloc(SCRATCH_MEMORY_SIZE);
    scratchMemoryRight = (char*)malloc(SCRATCH_MEMORY_SIZE);
    
    pcmMemory = (char*)malloc(PCM_MEMORY_SIZE);
    vmPCM = vmemopen(pcmMemory, PCM_MEMORY_SIZE, 0);
    //PCMRange((int)pcmMemory, (int)(pcmMemory + PCM_MEMORY_SIZE));
    
    
    
}

/*
 * 
 */
int main(int argc, char** argv) {
    init();
    scanAndFilterPartTable();
    scanAndFilterSupplierTable();
    scanAndFilterPartsupplierTable();
    joinPartAndPartsuppByPartkey();
    //SimBegin();
    //SimEnd();
    return (EXIT_SUCCESS);
}

