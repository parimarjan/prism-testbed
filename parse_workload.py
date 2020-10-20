import pandas as pd
import pickle
import os
import pdb
import numpy as np
import sys
import pdb
from collections import defaultdict

def make_dir(directory):
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def save_object(file_name, data):
    with open(file_name, "wb") as f:
        res = f.write(pickle.dumps(data))

def load_object(file_name):
    res = None
    if os.path.exists(file_name):
        with open(file_name, "rb") as f:
            res = pickle.loads(f.read())
    return res

COST_TYPE = "cm1_jerr"
LOG_DIR = sys.argv[1] + "/"
OUT_DIR = "./results/"
print("log directory: ", LOG_DIR)
payload_dirs = os.listdir(LOG_DIR)
all_rts = defaultdict(list)

for pdir in payload_dirs:
    print(pdir)
    result_dir = LOG_DIR + pdir + "/results"
    if not os.path.exists(result_dir):
        print("skipping ", pdir)
        continue
    for alg_dir in os.listdir(result_dir):
        costs_fn = result_dir + "/" + alg_dir + "/" + COST_TYPE + ".pkl"
        rt_fn = result_dir + "/" + alg_dir + "/" + "runtimes_" + COST_TYPE + ".csv"
        costs = load_object(costs_fn)
        if not os.path.exists(rt_fn):
            continue
        # rts = load_object(rt_fn)
        rts = pd.read_csv(rt_fn)
        # divide it into num_payload parts
        # print("total data at {}, len: {}".format(costs_fn, len(costs)))

        if rts is not None:
            # print("total data at {}, len: {}".format(rt_fn, len(rts)))
            all_rts[alg_dir].append(rts)


for alg, vals in all_rts.items():
    # df = pd.concat(vals, ignore_index=True)
    old_fn = OUT_DIR + "/" + alg + "/" + "runtimes_" + COST_TYPE + ".csv"
    old_dir = OUT_DIR + "/" + alg
    if not os.path.exists(old_dir):
        print(old_dir + " does not exist, so skipping these logs")
        continue

    if os.path.exists(old_fn):
        old_df = pd.read_csv(old_fn)
    else:
        old_df = pd.DataFrame()

    all_dfs = [old_df]
    all_dfs += vals
    df = pd.concat(all_dfs, ignore_index=True)
    df = df.drop_duplicates("sql_key")
    df = df[["sql_key", "runtime", "exp_analyze"]]
    print(alg)
    print(df.describe())
    pdb.set_trace()
    df.to_csv(old_fn, index=False)

    # save_object(old_fn, df)

## combine all alg results seen so far...
# cur_df = None
# for alg, vals in all_rts.items():
    # if alg == "postgres":
        # continue
    # df = pd.concat(vals, ignore_index=True)
    # df = df.rename(columns={"runtime":alg})
    # df = df.drop("exp_analyze",1)
    # print(alg)
    # print(df[alg].describe())
    # if cur_df is None:
        # cur_df = df
    # else:
        # cur_df = cur_df.merge(df, on="sql_key")
    # pdb.set_trace()

# print(cur_df.mean())
# print(cur_df.describe())
# pdb.set_trace()


