using Revise
using SiennaNEM

using PowerSimulations

using HiGHS

# NOTE:
#   The example `horizon = Hour(48)` here means that each study will span for 46
# hours. The `interval = Hour(1)` here means that the model only support
# selecting `initial_time` at every 1 hours, starting from the minimum date in
# `minimum(data["demand_l_ts"][!, :date])`.
#   Ideally, we can use as wide as possible for the horizon here and as small as
# possible interval here to support more flexible initial_time selection in the
# decision model. However, higher horizon will reduce the number of time slices
# that can be used for the rolling forecast.

system_data_dir = "data/nem12/arrow"
schedule_name = "schedule-1w"
ts_data_dir = joinpath(system_data_dir, schedule_name)
scenario_name = 1
horizon = Hour(48)
interval = Hour(1)

data = read_system_data(system_data_dir)
read_ts_data!(data, ts_data_dir)
add_tsf_data!(data, scenario_name=scenario_name)
update_system_data_bound!(data)
clean_ts_data!(data)

sys = create_system!(data)

template_uc = SiennaNEM.build_problem_base_uc()
solver = optimizer_with_attributes(HiGHS.Optimizer, "mip_rel_gap" => 0.01)

add_ts!(
    sys, data;
    scenario_name=scenario_name,  # scenario number
    horizon=horizon,  # horizon of each time slice that will be used in the study
    interval=interval,  # interval within each time slice, not the resolution of the time series
)
