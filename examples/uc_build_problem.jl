using Revise
using SiennaNEM

using PowerSimulations

using HiGHS

# setup optimizer
solver = optimizer_with_attributes(HiGHS.Optimizer, "mip_rel_gap" => 0.01)

# NOTE:
#   The example `horizon = Hour(T)` here means that each study will span for T
# hours. The `interval = Hour(I)` here means that the model only support
# selecting `initial_time` at every I hours, starting from the minimum date in
# `minimum(data["demand_l_ts"][!, :date])`.
#   Ideally, we can use as wide as possible for the horizon here and as small as
# possible interval here to support more flexible `initial_time` selection in
# the decision model. However, higher horizon will reduce the number of time
# slices that can be used for the rolling forecast. Selecting `schedule_horizon`
# later in `run_decision_model_loop` with a smaller value than this `horizon`
# will cause the last `(horizon - schedule_horizon)` hours of time series data
# not be solved.

# input variables parameters
system_data_dir = "data/nem12/arrow"
schedule_name = "schedule-1w"
ts_data_dir = joinpath(system_data_dir, schedule_name)
scenario_name = 1
horizon = Hour(24)
interval = Hour(1)

# data and system
data = SiennaNEM.get_data(system_data_dir, ts_data_dir)
sys = SiennaNEM.create_system!(data)
SiennaNEM.add_ts!(
    sys, data;
    horizon=horizon,  # horizon of each time slice that will be used in the study
    interval=interval,  # interval within each time slice, not the resolution of the time series
    scenario_name=scenario_name,  # scenario number
)

# problem template
template_uc = SiennaNEM.build_problem_base_uc()
