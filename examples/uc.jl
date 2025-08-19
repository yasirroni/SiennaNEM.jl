using Revise
using SiennaNEM

data_dir = "data/nem12"

data = read_data_csv(data_dir)
sys = create_system!(data)
add_ts!(sys, data, scenario_name=1)
