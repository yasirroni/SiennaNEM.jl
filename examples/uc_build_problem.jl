using Revise
using SiennaNEM

using PowerSimulations

using HiGHS

system_data_dir = "data/nem12/arrow"
ts_data_dir = joinpath(system_data_dir, "schedule-1w")
scenario_name = 1

data = read_system_data(system_data_dir)
read_ts_data!(data, ts_data_dir)
add_tsf_data!(data, scenario_name=scenario_name)
update_system_data_bound!(data)
clean_ts_data!(data)

sys = create_system!(data)

template_uc = SiennaNEM.build_problem_base_uc()
solver = optimizer_with_attributes(HiGHS.Optimizer, "mip_rel_gap" => 0.01)
