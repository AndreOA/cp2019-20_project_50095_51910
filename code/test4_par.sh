#!/bin/bash
SIZES=(100000)
THREADS=(2 4 8)


for SZ in "${SIZES[@]}"; do
    for TH in "${THREADS[@]}"; do
	echo $SZ $TH
        OMP_NUM_THREADS=$TH ./main -t 4 $SZ
    done
done | tee output_t4_par.csv