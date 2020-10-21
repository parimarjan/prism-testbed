source ~/.bashrc_exports
cd ~/learned-cardinalities

python3 get_runtimes.py --results_dir /home/ubuntu/payload/results/ \
  --results_fn cm1_jerr.pkl --cost_model cm1 --materialize 0 \
  --db_name so --timeout 900000 --rerun_timeouts 0

