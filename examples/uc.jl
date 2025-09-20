using Revise
using SiennaNEM

using HiGHS
using Dates

system_data_dir = "data/nem12/arrow"
ts_data_dir = joinpath(system_data_dir, "schedule-1w")

data = read_system_data(system_data_dir)
read_ts_data!(data, ts_data_dir)

sys = create_system!(data)
add_ts!(sys, data, scenario_name=1)

template_uc = build_problem_base_uc()
solver = optimizer_with_attributes(HiGHS.Optimizer, "mip_rel_gap" => 0.5)

hours = Hour(24)
problem = DecisionModel(template_uc, sys; optimizer=solver, horizon=hours)
build!(problem; output_dir=mktempdir())
solve!(problem)
res = OptimizationProblemResults(problem)
