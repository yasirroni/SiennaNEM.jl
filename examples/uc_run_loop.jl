using Dates

# NOTE:
#   This script require `uc_build_problem.jl` to be run first to setup the
# system. This script sets up and solves a single time unit decision model. You
# can specify the horizon and initial_time for the decision model.

# Parameters for the decision model
horizon = Hour(24)
interval = Hour(1)
window_shift = Hour(24)
initial_time = minimum(data["demand_l_ts"][!, "date"])


# Create time series slices data
demand_time_slices = create_time_slices(
    data["demand_l_ts"],
    initial_time = DateTime("2025-01-07T00:00:00"),
    horizon = horizon,
    window_shift = window_shift,
)
generator_time_slices = create_time_slices(
    data["generator_pmax_ts"],
    initial_time = DateTime("2025-01-07T00:00:00"),
    horizon = horizon,
    window_shift = window_shift,
)

# Loop through each time slice
res_dict = Dict{DateTime, OptimizationProblemResults}()
clear_time_series!(sys)
for time_slice in collect(keys(demand_time_slices))
    df_demand_ts = demand_time_slices[time_slice]
    df_generator_ts = generator_time_slices[time_slice]

    # TODO: use Deterministic directly to avoid removing and adding
    add_ts!(
        sys,
        df_demand_ts,
        df_generator_ts,
        data["components"]["renewable_dispatch_generators"],
        data["components"]["renewable_nondispatch_generators"],
        data["components"]["demands"],
        horizon=horizon,
        interval=interval,
        scenario_name=1,
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

    objective_value = get_objective_value(res)
    println("Time slice: $time_slice, Objective value: $objective_value")

    clear_time_series!(sys)
end