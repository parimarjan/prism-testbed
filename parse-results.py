import pickle
import argparse
import glob
import pdb
import os
from collections import defaultdict
import matplotlib.pyplot as plt

# pari: try to generalize these helper functions / reduce redundancies with
# others.
def save_object(file_name, data):
    with open(file_name, "wb") as f:
        res = f.write(pickle.dumps(data))

def load_object(file_name):
    res = None
    if os.path.exists(file_name):
        with open(file_name, "rb") as f:
            res = pickle.loads(f.read())
    return res

def deterministic_hash(string):
    return int(hashlib.sha1(str(string).encode("utf-8")).hexdigest(), 16)

def read_flags():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log_dir", type=str, required=False,
            default="./test-logs")
            # default="/Users/parimarjann/qopt-results/results/onlyRuntime1")
    return parser.parse_args()

def handle_file(file_name, combined_data):
    assert os.path.exists(file_name)
    if "final" not in file_name:
        return
    data = load_object(file_name)
    # print(data.keys())
    # for k in data:
        # print(k, data[k][0]["dbmsAllRuntimes"])

    # pdb.set_trace()
    # if data is not None:
        # print(data.keys())
    # else:
        # print(data)
    if data is None:
        return
    for ep in data:
        if ep not in combined_data:
            combined_data[ep] = {}
        for query in data[ep]:
            # for planner, rt in query["dbmsRuntimes"].items():
            for planner, rts in query["dbmsAllRuntimes"].items():
                if "CM" in file_name:
                    planner = planner + "-CM"
                else:
                    planner = planner + "-RT"

                if planner not in combined_data[ep]:
                    combined_data[ep][planner] = []
                # combined_data[ep][planner].append(rt)
                combined_data[ep][planner].append(sum(rts) / len(rts))

def main():
    combined_data = {}
    for fn in glob.iglob(args.log_dir + "/**/*.dict", 
            recursive=True):
        if "final" in fn and "test" in fn:
            handle_file(fn, combined_data)
    
    vals = defaultdict(list)
    episodes = sorted(combined_data.keys())
    print(episodes)
    # for ep in episodes:
    # plt.plot(episodes, 
    rl_cm = []
    cmx = []
    rl_rt = []
    rtx = []
    pg = []

    for ep in episodes:
        if ep > 40:
            continue

        if "RL-CM" in combined_data[ep]:
            rl_cm.append(combined_data[ep]["RL-CM"][0])
            cmx.append(ep)
        if "RL-RT" in combined_data[ep]:
            rl_rt.append(combined_data[ep]["RL-RT"][0])
            rtx.append(ep)
        if "postgres-RT" in combined_data[ep]:
            pg.append(combined_data[ep]["postgres-RT"][0])

    print(pg)
    plt.plot(cmx, rl_cm, 'b', label="RL Cost Model")
    plt.plot(rtx, rl_rt, 'r', label="RL Runtime")
    plt.plot(rtx, pg, 'g', label="Postgres")
    plt.legend(fontsize="x-large")
    plt.savefig("test.png")

    # for ep in episodes:
        # rl_all = len(combined_data[ep]["RL"])
        # rl_avg = sum(combined_data[ep]["RL"]) / rl_all
        # pg_avg = sum(combined_data[ep]["postgres"]) / rl_all
        # vals["ep"].append(ep)
        # vals["rl"].append(rl_avg)
        # vals["postgres"].append(pg_avg)
        # assert len(combined_data[ep]["RL"])==len(combined_data[ep]["postgres"])
        # print("ep: {}, rl avg: {}, pg avg: {}".format(\
                # ep, rl_avg, pg_avg))

    # plt.plot(vals["ep"], vals["rl"], 'r') # plotting t, a separately 
    # plt.plot(vals["ep"], vals["postgres"], 'b') # plotting t, a separately 
    # plt.savefig("test.png")

args = read_flags()
main()
