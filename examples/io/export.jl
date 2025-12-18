using SiennaNEM

using PowerSystems
using PowerSimulations

using HiGHS
using Dates

system_data_dir = "data/nem12/arrow"
ts_data_dir = joinpath(system_data_dir, "schedule-1w")
scenario_name = 1

data = read_system_data(system_data_dir)
read_ts_data!(data, ts_data_dir)
add_tsf_data!(data; scenario_name=scenario_name)
update_system_data_bound!(data)

sys = create_system!(data)
add_ts!(sys, data; scenario_name=scenario_name)

to_json(sys, "examples/result/nem12/json/schedule-1w.json", force=true)
