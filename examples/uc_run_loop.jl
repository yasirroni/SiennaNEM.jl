using Dates
using PowerSystems

# NOTE:
#   This script require `uc_build_problem.jl` to be run first to setup the
# system. This script sets up and solves a multiple time unit decision model by
# iterating over time window. You can specify the horizon and initial_time for
# the decision model.

# Parameters for the decision model
horizon = Hour(24)
interval = Hour(1)
window_shift = Hour(24)
initial_time = minimum(data["demand_l_ts"][!, "date"])
scenario_name = 1  # Assuming single scenario for simplicity

# Create time series slices data
# NOTE:
#   This function is too heavy, maybe don't slice all at once.
# TODO:
#   Benchmark on single week, compare with slicing on the fly.

df_demand = filter_value_from_df(data["demand_l_ts"], :scenario, scenario_name)
df_generator = filter_value_from_df(data["generator_pmax_ts"], :scenario, scenario_name)

demand_time_slices = get_time_slices_iterator(
    df_demand,
    initial_time = initial_time,
    horizon = horizon,
    window_shift = window_shift,
)
generator_time_slices = get_time_slices_iterator(
    df_generator,
    initial_time = initial_time,
    horizon = horizon,
    window_shift = window_shift,
)

# Loop through each time slice
res_dict = Dict{DateTime, OptimizationProblemResults}()
clear_time_series!(sys)  # This command is very time consuming, need a refactor

for ((time_slice, df_demand_ts), (_, df_generator_ts)) in zip(demand_time_slices, generator_time_slices)
    # TODO: use Deterministic directly to avoid removing and adding
    add_ts!(
        sys,
        df_demand_ts,
        df_generator_ts,
        data["components"]["demands"],
        data["components"]["renewable_dispatch_generators"],
        data["components"]["renewable_nondispatch_generators"],
        horizon=horizon,
        interval=interval,
    )

    # Create and solve the decision model with the current time slice
    problem = DecisionModel(
        template_uc, sys;
        optimizer=solver,
        horizon=horizon,
        initial_time=time_slice,
    )

    build!(problem; output_dir=mktempdir())
    solve!(problem)
    res_dict[time_slice] = OptimizationProblemResults(problem)

    clear_time_series!(sys)  # This command is very time consuming, need a refactor
end
