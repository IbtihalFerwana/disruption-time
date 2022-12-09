# Disruption over time
This repo for analysing the disruption measure over time

To run the disruption calculation, follow the instructions:
1. Run the notebook of `extract_top_5_pernc.ipynb` to get a files named `preprocessed_data/oc_yrcleaned.csv` from Open Citations and `preprocessed_data/top5percn_cited_aminer.csv` from the articles dataset, which is AMiner in this repo
2. Run the example `python3 calc_disruption7.py --w 2 --aminer_file 'data/top5percn_cited_aminer.csv' --filename 'top5'`
```
--w: window size from publication, e.g. 2 or 4
--aminer_file: the dataset file
--filename: the output file name to save the disruption values for each article in aminer_file
```
