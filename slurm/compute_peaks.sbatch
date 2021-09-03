#!/bin/bash
# Compute K-factors for all days across the state
# request many cores: https://login.scg.stanford.edu/faqs/cores/
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --mem=12G
#SBATCH --time=36:00:00

DIR=/pine/scr/m/w/mwbc/pems

module load julia/1.6.2
cd ~/peak-spreading/code
julia --project=@. --threads=1 calculate_peaks.jl $DIR 2>&1 > ~/peak-spreading/calculate_peaks_${SLURM_JOB_ID}.log
julia --project=@. --threads=1 combine_peaks.jl $DIR ~/peak-spreading/all_district_peaks.parquet