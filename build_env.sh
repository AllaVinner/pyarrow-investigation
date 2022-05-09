#! /bin/bash 

conda env create -f environment.yml
conda activate pyarrow-investigation
pip install -e .
