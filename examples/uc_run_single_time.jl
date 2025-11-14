using Dates

# NOTE:
#   This script require `uc_build_problem.jl` to be run first to setup the
# system. This script sets up and solves a single time unit decision model. You
# can specify the horizon and initial_time for the decision model.

# Parameters for the decision model
horizon = Hour(24)
initial_time = minimum(data["demand_l_ts"][!, "date"])

# Add time series data to the system
add_ts!(sys, data, scenario_name=scenario_name)

# Create and solve the decision model
problem = DecisionModel(
    template_uc, sys;
    optimizer=solver,
    horizon=horizon,
    initial_time=initial_time,
)
build!(problem; output_dir=mktempdir())
solve!(problem)
res = OptimizationProblemResults(problem)

objective_value = get_objective_value(res)
