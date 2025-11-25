using Dates

# NOTE:
#   This script require `uc_build_problem.jl` to be run first to setup the
# system. This script sets up and solves a single time unit decision model. You
# can specify the horizon and initial_time for the decision model.

# Parameters for the decision model
initial_date = DateTime("2025-01-07T00:00:00")
horizon = Hour(24)

# Add time series data to the system
add_ts!(sys, data, scenario_name=scenario_name)

# Create and solve the decision model
problem = DecisionModel(
    template_uc, sys;
    optimizer=solver,
    horizon=horizon,
)
build!(problem; output_dir=mktempdir())
solve!(problem)
res = OptimizationProblemResults(problem)

objective_value = get_objective_value(res)
