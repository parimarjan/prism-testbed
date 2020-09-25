import pandas as pd
import pickle
import os
import pdb
import numpy as np
import sys
import errno

RESULTS_DIR = "./results/"
PAYLOAD_DIR = "./payload/"
ONLY_TEST = True
ONLY_JOB = True
ONLY_TRAIN = False
TIMEOUT_CONSTANT = 909.0
ONLY_RUNTIME_KEYS = False
# RES_FNS = ["nested_loop_index7_jerr.pkl", "cm1_jerr.pkl"]
RES_FNS = ["cm1_jerr.pkl"]

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

payload_dirs = os.listdir(PAYLOAD_DIR)

for res_fn in RES_FNS:
    # rt_keys
    rt_keys = None
    if res_fn == "plan_pg_err.pkl":
        rt_keys = list(load_object("plan_pg_err_keys.pkl"))
    elif res_fn == "cm1_jerr.pkl":
        rt_keys = list(load_object("cm1_jerr_keys.pkl"))
    elif res_fn == "nested_loop_index7_jerr.pkl":
        rt_keys = list(load_object("nested_loop_index7_jerr_keys.pkl"))
    else:
        assert False
    for alg_dir in os.listdir(RESULTS_DIR):
        args_fn = RESULTS_DIR + "/" + alg_dir + "/" + "args.pkl"
        costs_fn = RESULTS_DIR + "/" + alg_dir + "/" + res_fn
        print(costs_fn)
        costs = load_object(costs_fn)
        exp_args = load_object(args_fn)
        rt_fn = RESULTS_DIR + "/" + alg_dir + "/" + "runtimes_" + res_fn
        rt_fn = rt_fn.replace(".pkl", ".csv")
        # rts = load_object(rt_fn)
        if os.path.exists(rt_fn):
            rts = pd.read_csv(rt_fn)
        else:
            rts = None

        if rts is not None:
            # rts = rts[rts["runtime"] != TIMEOUT_CONSTANT]
            exclude_keys = set(rts["sql_key"])
            len_before = len(costs)
            costs = costs[~costs["sql_key"].isin(exclude_keys)]
            print("#queries: {}, after removing known runtimes: {}"\
                    .format(len_before, len(costs)))

        if rt_keys is not None and ONLY_RUNTIME_KEYS:
            len_before = len(costs)
            costs = costs[costs["sql_key"].isin(rt_keys)]
            print(("#queries: {}, only considering rt queries: {}")\
                    .format(len_before, len(costs)))

        sample_types = []
        if ONLY_TEST:
            sample_types.append("test")
        if ONLY_TRAIN:
            sample_types.append("train")
        if ONLY_JOB:
            sample_types.append("job")

        costs = costs[costs["samples_type"].isin(sample_types)]

        # if ONLY_TEST and ONLY_JOB:
            # costs = costs[costs["samples_type"].isin(["test", "job"])]

        # elif ONLY_TEST:
            # costs = costs[costs["samples_type"].isin(["test"])]

        # elif ONLY_JOB:
            # costs = costs[costs["samples_type"].isin(["job"])]

        # elif ONLY_TRAIN:
            # costs = costs[costs["samples_type"] == "train"]

        print(set(costs["samples_type"]))

        if len(costs) == 0:
            print("no queries for: ", alg_dir)
            continue

        # divide it into num_payload parts
        print("total queries for {}: {}".format(costs_fn, len(costs)))
        shuffled = costs.sample(frac=1, random_state=10)
        splits = np.array_split(shuffled, len(payload_dirs))

        for i,pdir in enumerate(payload_dirs):
            p_costs_dir = PAYLOAD_DIR + pdir + "/" + "results/" + alg_dir
            make_dir(p_costs_dir)
            p_costs_fn = p_costs_dir + "/" + res_fn
            p_costs_args = p_costs_dir + "/args.pkl"
            print("saving {} of len {}".format(p_costs_fn, len(splits[i])))
            save_object(p_costs_fn, splits[i])
            save_object(p_costs_args, exp_args)
