using Dates
using PowerSystems

# NOTE:
#   This script require `uc_build_problem.jl` to be run first to setup the
# system. This script sets up and solves a multiple time unit decision model by
# iterating over time window. You can specify the horizon, initial_time, and
# window_shift.

# Parameters for the decision model
schedule_horizon = Hour(24)  # must be lower than or equal to the horizon used in add_ts!
window_shift = Hour(24)

# Loop through each time slice
res_dict = Dict{DateTime, OptimizationProblemResults}()
minimum_initial_time = minimum(data["demand_l_ts"][!, :date])
maximum_initial_time = maximum(data["demand_l_ts"][!, :date]) - horizon

for initial_time_slice in minimum_initial_time:window_shift:maximum_initial_time
    # TODO: use Deterministic directly to avoid removing and adding
    # Create and solve the decision model with the current time slice
    problem = DecisionModel(
        template_uc, sys;
        optimizer=solver,
        horizon=schedule_horizon,  # must be lower than or equal to the horizon used in add_ts!
        initial_time=initial_time_slice,
    )
    build!(problem; output_dir=mktempdir())
    solve!(problem)
    res_dict[initial_time_slice] = OptimizationProblemResults(problem)
end
