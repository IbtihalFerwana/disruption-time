import pandas as pd
import multiprocessing
from multiprocessing import Pool, freeze_support, Manager
from functools import partial
from itertools import repeat
import tqdm 
import json
import argparse
from multiprocessing.pool import ThreadPool as Pool

import time
pub_year_map = {}
yrs_window = 0
cites4 = ''
# def calc_disruption(doi, cites4, pub_year_map, yrs_window):
def calc_disruption(doi):
    ni = 0
    nj = 0
    nk = 0
    refs_df1 = pd.DataFrame()
    cits_df1 = pd.DataFrame()
    doi = doi[0]

    # global cites4
    # global pub_year_map
    # global yrs_window
    
    if pub_year_map[doi] <= 2021-yrs_window:
        pub_yr = pub_year_map[doi]
    
        if yrs_window == 0:
            pub_yr = 2021
        refs = set(cites4[cites4['citing'].values==doi]['cited']) # focal references
        if len(refs) > 0:
            ref_df1 = cites4[cites4['cited'].isin(refs)]
        else:
            return None
        cits = set(cites4[(cites4['cited'].values==doi)&(cites4['citing_pub_year']<=pub_yr+yrs_window)]['citing']) # focal citations
        if len(cits) > 0:
            cits_df1 = cites4[cites4['citing'].isin(cits)]
            cits = cits_df1['citing'].unique()
            cits_df1 = cits_df1[cits_df1['cited'].isin(refs)]
            dftmp = cits_df1.groupby('citing').agg({'cited':'count'}).reset_index()
            ni = len(dftmp[dftmp['cited'].values == 1])
        
        nj = len(cits) - ni
        
        ref_df2 = ref_df1[~ref_df1['citing'].isin(cits)]
        ref_df2 = ref_df2[ref_df2['citing_pub_year'] <= pub_yr+yrs_window]
        nk = len(ref_df2)

        if (ni+nj+nk)==0:
            D = 0
        else:
            D = (ni-nj)/(ni+nj+nk)
        # with open(f'ds_results/ds_w{yrs_window}_1026.txt','a') as f:
            # f.write(f"{doi},{D}\n")
        # row.append({doi,D})
        return doi,D
    else:
        return None

def write_to_file(txt):
    global fout
    for x in txt:
        fout.write(f"{x[0]},{x[1]}\n")

def read_data():
    global cites4
    cites4 = pd.read_csv('preprocessed_data/oc_yrcleaned.csv')
def w_value(w):
    global yrs_window
    yrs_window = w
def yr_map(local_year_map):
    global pub_year_map
    pub_year_map = local_year_map
def initializer(cites2, send_pub_year_map, w):
    global cites4
    cites4 = cites2
    global pub_year_map
    pub_year_map = send_pub_year_map
    global yrs_window
    yrs_window = w
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--w", help='window size from publication', default=0, type=int)
    parser.add_argument("--aminer_file", type=str)
    parser.add_argument("--filename",type=str)

    args = parser.parse_args() 
    output_file = f"ds_results/ds_{args.filename}_w{args.w}.txt"
    print("Args")
    print(f"\taminer file path: {args.aminer_file} ")
    print(f"\twindow size: {args.w}")
    print(f"\tresults saved at: {output_file}")
    print()

    cites2 = pd.read_csv('data/oc_yrcleaned.csv')
    print("read open citations")

    #top5 = pd.read_csv("top5percent_aminer.csv")
    top5 = pd.read_csv(args.aminer_file,index_col=0) 
    # row = Manager().list()
    file_name = args.aminer_file.split(".")[0]
    print("read aminer dois")
    local_year_map = dict(zip(top5['doi'],top5['year']))
    dois = top5['doi'].values
    tups = [tuple([n]) for n in dois]
    print("cpu counts: ", multiprocessing.cpu_count())
    st = time.time()
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count(),initializer=initializer,initargs=(cites2,local_year_map,args.w,))
    fout = open(output_file, 'a')
    for res in pool.imap(calc_disruption, tqdm.tqdm(tups,total=len(tups))):
        # i = 0
        # print(res)
        if res == None:
            continue
        fout.write(f"{res[0]},{res[1]}\n")
    
    pool.close()
    pool.join()
    print("End of pool")
    et = time.time()
    print(et-st)
