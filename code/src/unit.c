#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "patterns.h"
#include <stdio.h>
#include "seq_patterns.h"
#include "debug.h"
#include "unit.h"

//=======================================================
// TYPE DOUBLE
//=======================================================


#define TYPE double
#define PRINT(SRC, N, STRING) printDouble(SRC, N, STRING)
#define FMT "%lf"

int TYPE_compare(const void* a, const void* b) {
    TYPE v1 = *(TYPE *)a;
    TYPE v2 = *(TYPE *)b;

    return v1 == v2;
}


//=======================================================
// TYPE LONG
//=======================================================

/*
#define TYPE long
#define PRINT(SRC, N, STRING) printLong(SRC, N, STRING)
#define FMT "%ld"

int TYPE_compare(const void* a, const void* b) {
    TYPE v1 = *(TYPE *)a;
    TYPE v2 = *(TYPE *)b;

    return v1 == v2;
}
*/

//=======================================================
// TYPE INT
//=======================================================

/*
#define TYPE int
#define PRINT(SRC, N, STRING) printInt(SRC, N, STRING)
#define FMT "%d"

int TYPE_compare(const void* a, const void* b) {
    TYPE v1 = *(TYPE *)a;
    TYPE v2 = *(TYPE *)b;

    return v1 == v2;
}
*/

//=======================================================
// TYPE STRING
//=======================================================

/*
#define TYPE char *
#define PRINT(SRC, N, STRING) printString(SRC, N, STRING)
#define FMT "%s"

int TYPE_compare(const void* a, const void* b) {
    const char *a1 = *(char **)a;
    const char *b1 = *(char **)b;
    return strcmp (a1, b1);
}

*/


//=======================================================
// Workers
//=======================================================

/*
static void workerMax(void* a, const void* b, const void* c) {
    // a = max (b, c)
    *(TYPE *)a = *(TYPE *)b;
    if (*(TYPE *)c > *(TYPE *)a)
        *(TYPE *)a = *(TYPE *)c;
}

static void workerMin(void* a, const void* b, const void* c) {
    // a = min (b, c)
    *(TYPE *)a = *(TYPE *)b;
    if (*(TYPE *)c < *(TYPE *)a)
        *(TYPE *)a = *(TYPE *)c;
}
static void workerSubtract(void* a, const void* b, const void* c) {
    // a = n - c
    *(TYPE *)a = *(TYPE *)b - *(TYPE *)c;
}

static void workerMultiply(void* a, const void* b, const void* c) {
    // a = b * c
    *(TYPE *)a = *(TYPE *)b + *(TYPE *)c;
}
*/

static void workerAdd(void* a, const void* b, const void* c) {
    // a = b + c
    *(TYPE *)a = *(TYPE *)b + *(TYPE *)c;
}


static void workerAddOne(void* a, const void* b) {
    // a = b + 1
    *(TYPE *)a = *(TYPE *)b + 1;
}

static void workerMultTwo(void* a, const void* b) {
    // a = b * 2
    *(TYPE *)a = *(TYPE *)b * 2;
}

static void workerDivTwo(void* a, const void* b) {
    // a = b / 2
    *(TYPE *)a = *(TYPE *)b / 2;
}

/*
static void workerBig(void* a, const void* b) {
    for(int i = 0; i < 1000000; i++){;}
     *(TYPE *)a = *(TYPE *)b + 1;
}
*/


/*
static void workerConcat(void* a, const void* b) {
    // a = a concat b
    *(TYPE *)a = realloc(*(TYPE *)a, strlen(*(TYPE *)a) + strlen(*(TYPE *)b));
    strcat(*(TYPE *)a, *(TYPE *)b);
}

static void workerReplaceFirstWithX(void* a, const void* b) {
    // replaces first char with x

    strcpy((TYPE) a, (TYPE) b);
    ((TYPE) a)[0] = "x";
 
    printf ("STRING FINAL: %s\n ", (TYPE )a);
}
*/



//=======================================================
// Validating testing funtions
//=======================================================


void validateMap (void *src, size_t n, size_t size) {
    TYPE *seq_dest = malloc (n * size);
    seq_map (seq_dest, src, n, size, workerAddOne);

    TYPE *dest = malloc (n * size);
    map (dest, src, n, size, workerAddOne);
    
    int error = 0;
    for(int i = 0 ;  i < n  && !error;i++){
        //if (strcmp(seq_dest[i], dest[i]) != 0){
        if (seq_dest[i] != dest[i]){
            error = 1;
            printf("ERROR in MAP %d \n", i);
        }
    }

    free (dest);
    free(seq_dest);
}

void validateReduce (void *src, size_t n, size_t size) {
    TYPE *seq_dest = malloc (n * size);
    seq_reduce (seq_dest, src, n, size, workerAdd);


    TYPE *dest = malloc (n * size);
    reduce (dest, src, n, size, workerAdd);
    
    int error = 0;
    for(int i = 0 ;  i < n  && !error;i++){
        if ( seq_dest[i] != dest[i]){
            error = 1;
            printf("ERROR in REDUCE %d \n",i);
        }
    }

    free (dest);
    free(seq_dest);
}


void validateScan (void *src, size_t n, size_t size) {
    TYPE *seq_dest = malloc (n * size);
    seq_scan (seq_dest, src, n, size, workerAdd);


    TYPE *dest = malloc (n * size);
    scan (dest, src, n, size, workerAdd);
    
    int error = 0;
    for(int i = 0 ;  i < n  && !error;i++){
        if ( seq_dest[i] != dest[i]){
            error = 1;
            printf("ERROR in SCAN %d \n",i);
        }
    }

    free (dest);
    free(seq_dest);
}


void validatePack (void *src, size_t n, size_t size) {
    int nFilter = 3;
    TYPE *dest = malloc (nFilter * size);
    TYPE *seq_dest = malloc (nFilter * size);
    int *filter = calloc(n,sizeof(*filter));
    for (int i = 0;  i < n;  i++)
        filter[i] = (i == 0 || i == n/2 || i == n-1);

    int newN = pack (dest, src, n, size, filter);    
    int valN = seq_pack(seq_dest, src, n, size, filter);   
    
    int error = 0;
    if ( newN != valN)
        error = 1;
        
    for(int i = 0 ;  i < nFilter  && !error;i++){
        if ( seq_dest[i] != dest[i]){
            error = 1;
            printf("ERROR in PACK %d \n",i);
        }
    }

    free (dest);
    free(seq_dest); 
    free(filter);
}


void validateGather (void *src, size_t n, size_t size) {
    int nFilter = 3;
    TYPE *dest = malloc (nFilter * size);
    TYPE *seq_dest = malloc (nFilter * size);
    int filter[nFilter];
    for (int i = 0;  i < nFilter;  i++)
        filter[i] = rand() % n;

    gather (dest, src, n, size, filter, nFilter);    
    seq_gather (seq_dest, src, n, size, filter, nFilter);    

    
    int error = 0;
    for(int i = 0 ;  i < nFilter  && !error;i++){
        if ( seq_dest[i] != dest[i]){
            error = 1;
            printf("ERROR in GATHER %d \n",i);
        }
    }

    free (dest);
    free(seq_dest);
}

void validateScatter (void *src, size_t n, size_t size) {
    int nDest = 6;
    TYPE *dest = malloc (nDest * size);
    TYPE *seq_dest = malloc (nDest * size);
    memset (dest, 0, nDest * size);
    memset (seq_dest, 0, nDest * size);
    int *filter = calloc(n,sizeof(*filter));
    for (int i = 0;  i < n;  i++)
        filter[i] = rand() % nDest;

    scatter (dest, src, n, size, filter);    
    seq_scatter (seq_dest, src, n, size, filter);    

    
    int error = 0;
    for(int i = 0 ;  i < nDest  && !error;i++){
        if (seq_dest[i] != dest[i]){
            error = 1;
            printf("ERROR in SCATTER %d \n",i);
        }
    }

    free (dest);
    free(seq_dest);
    free(filter);
}

void validatePipeline (void *src, size_t n, size_t size) {
    void (*pipelineFunction[])(void*, const void*) = {
        workerMultTwo,
        workerAddOne,
        workerDivTwo    };
    int nPipelineFunction = sizeof (pipelineFunction)/sizeof(pipelineFunction[0]);
    TYPE *dest = malloc (n * size);
    TYPE *seq_dest = malloc (n * size);
    pipeline (dest, src, n, size, pipelineFunction, nPipelineFunction);
    seq_pipeline (seq_dest, src, n, size, pipelineFunction, nPipelineFunction);



    int error = 0;
    for(int i = 0 ;  i < n  && !error;i++){
        if ( seq_dest[i] != dest[i]){
            error = 1;
            printf("ERROR in PipeLine %d \n",i);
        }
    }

    free (dest);
    free(seq_dest);
}


void validateFarm (void *src, size_t n, size_t size) {
    
    TYPE *seq_dest = malloc (n * size);
    seq_farm (seq_dest, src, n, size, workerAddOne, 3);

    TYPE *dest = malloc (n * size);
    farm (dest, src, n, size, workerAddOne, 3);
    
    int error = 0;
    for(int i = 0 ;  i < n  && !error;i++){
        if ( seq_dest[i] != dest[i]){
            error = 1;
            printf("ERROR in FARM %d \n",i);
        }
    }

    free (dest);
    free(seq_dest);
}


typedef void (*VALIDATEFUNCTION)(void *, size_t, size_t);

VALIDATEFUNCTION validateFunction[] = {
    validateMap,
    validateReduce,
    validateScan,
    validatePack,
    validateGather,
    validateScatter,
    validatePipeline,
    validateFarm,
};

char *validateNames[] = {
    "validateMap",
    "validateReduce",
    "validateScan",
    "validatePack",
    "validateGather",
    "validateScatter",
    "validatePipeline",
    "validateFarm",
};


int nValidateFunction = sizeof (validateFunction)/sizeof(validateFunction[0]);




//=======================================================
// Unit testing funtions
//=======================================================

void testMap (void *src, size_t n, size_t size) {
    TYPE *dest = malloc (n * size);
    map (dest, src, n, size, workerAddOne);
    PRINT (dest, n, __FUNCTION__);
    free (dest);
}

void testReduce (void *src, size_t n, size_t size) {
    TYPE *dest = malloc (size);
    reduce (dest, src, n, size, workerAdd);
    PRINT (dest, 1, __FUNCTION__);
    free (dest);
}

void testScan (void *src, size_t n, size_t size) {
    TYPE *dest = malloc (n * size);
    scan (dest, src, n, size, workerAdd);
    PRINT (dest, n, __FUNCTION__);
    free (dest);
}

void testPack (void *src, size_t n, size_t size) {
    int nFilter = 3;
    TYPE *dest = malloc (nFilter * size);
    int *filter = calloc(n,sizeof(*filter));
    for (int i = 0;  i < n;  i++)
        filter[i] = (i == 0 || i == n/2 || i == n-1);
    int newN = pack (dest, src, n, size, filter);    
    printInt (filter, n, "filter");    
    PRINT (dest, newN, __FUNCTION__);
    free(filter);
    free (dest);
}

void testGather (void *src, size_t n, size_t size) {
    int nFilter = 3;
    TYPE *dest = malloc (nFilter * size);
    int filter[nFilter];
    for (int i = 0;  i < nFilter;  i++)
        filter[i] = rand() % n;
    printInt (filter, nFilter, "filter");    
    gather (dest, src, n, size, filter, nFilter);    
    PRINT (dest, nFilter, __FUNCTION__);
    free (dest);
}

void testScatter (void *src, size_t n, size_t size) {
    int nDest = 6;
    TYPE *dest = malloc (nDest * size);
    memset (dest, 0, nDest * size);
    int *filter = calloc(n,sizeof(*filter));
    for (int i = 0;  i < n;  i++)
        filter[i] = rand() % nDest;
    printInt (filter, n, "filter");    
    scatter (dest, src, n, size, filter);    
    PRINT (dest, nDest, __FUNCTION__);
    free(filter);
    free (dest);
}

void testPipeline (void *src, size_t n, size_t size) {
    void (*pipelineFunction[])(void*, const void*) = {
        workerMultTwo,
        workerAddOne,
        workerDivTwo
    };
    int nPipelineFunction = sizeof (pipelineFunction)/sizeof(pipelineFunction[0]);
    TYPE *dest = malloc (n * size);
    pipeline (dest, src, n, size, pipelineFunction, nPipelineFunction);
    PRINT (dest, n, __FUNCTION__);    
    free (dest);
}

void testFarm (void *src, size_t n, size_t size) {
    TYPE *dest = malloc (n * size);
    farm (dest, src, n, size, workerAddOne, 3);
    PRINT (dest, n, __FUNCTION__);
    free (dest);
}


//=======================================================
// List of unit test functions
//=======================================================


typedef void (*TESTFUNCTION)(void *, size_t, size_t);

TESTFUNCTION testFunction[] = {
    testMap,
    testReduce,
    testScan,
    testPack,
    testGather,
    testScatter,
    testPipeline,
    testFarm,
};

char *testNames[] = {
    "testMap",
    "testReduce",
    "testScan",
    "testPack",
    "testGather",
    "testScatter",
    "testPipeline",
    "testFarm",
};




int nTestFunction = sizeof (testFunction)/sizeof(testFunction[0]);


