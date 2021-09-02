#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --time=2:00:00
#SBATCH --mem=512

cd /pine/scr/m/w/mwbc/pems
shasum -a 256 -c checksums.txt