using Dates

# NOTE:
#   This script require `uc_build_problem.jl` to be run first to setup the
# system. This script sets up and solves a single time unit decision model.

# Create and solve the decision model
problem = DecisionModel(
    template_uc, sys;
    optimizer=solver,
    horizon=Hour(24),  # must be lower than or equal to the horizon used in add_ts!
    initial_time=DateTime("2025-01-08T00:00:00"),  # must be aligned with start_time + (k * interval)
)
build!(problem; output_dir=mktempdir())
solve!(problem)
res = OptimizationProblemResults(problem)

objective_value = get_objective_value(res)
