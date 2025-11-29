using Dates
using PowerSystems

"""
    run_decision_model_loop(
        template::ProblemTemplate,
        sys::System,
        optimizer;
        schedule_horizon::Union{Period, Nothing}=nothing,
        window_shift::Period=Hour(24),
        minimum_initial_time::Union{DateTime, Nothing}=nothing,
        maximum_initial_time::Union{DateTime, Nothing}=nothing
    )

Set up and solve multiple unit commitment decision models by iterating over time windows.

This function creates and solves a series of decision models, each covering a specified
time horizon (schedule_horizon) with a rolling window approach. The window shifts forward
by `window_shift` for each iteration.

# Arguments
- `template::ProblemTemplate`: The problem template defining the UC formulation.
- `sys::System`: The PowerSystems system object containing the network and time series data.
- `solver`: The optimization solver (e.g., HiGHS.Optimizer).

# Keyword Arguments
- `schedule_horizon::Union{Period, Nothing}=nothing`: Time horizon for each decision model.
  Must be less than or equal to the horizon used in `add_ts!`. If `nothing`, automatically
  detects from the system's forecast horizon.
- `window_shift::Period=Hour(24)`: Time step between consecutive decision model windows.
- `minimum_initial_time::Union{DateTime, Nothing}=nothing`: Starting time for the first window.
  If `nothing`, uses the first available forecast initial time from the system.
- `maximum_initial_time::Union{DateTime, Nothing}=nothing`: Ending time for the last window.
  If `nothing`, uses the last available forecast initial time from the system.

# Returns
- `Dict{DateTime, OptimizationProblemResults}`: Dictionary mapping initial times to their
  corresponding optimization results.

# Notes
- The function requires the system to be pre-built with time series data (see `uc_build_problem.jl`).
- If `schedule_horizon` is less than the horizon used in `add_ts!`, the last
  `(horizon - schedule_horizon)` hours of time series data will not be solved.
- Each iteration creates a temporary directory for problem output that is automatically cleaned up.
- The function will stop before running out of time series data to avoid assertion errors.

# TODO
- Use Deterministic time series directly to avoid removing and re-adding time series data.
"""
function run_decision_model_loop(
    template::ProblemTemplate,
    sys::System,
    optimizer;
    schedule_horizon::Union{Period, Nothing}=nothing,
    window_shift::Period=Hour(24),
    minimum_initial_time::Union{DateTime, Nothing}=nothing,
    maximum_initial_time::Union{DateTime, Nothing}=nothing
)
    # Determine the horizon
    if schedule_horizon === nothing
        schedule_horizon = InfrastructureSystems.get_forecast_horizon(sys.data)
    end

    # Set time range
    if minimum_initial_time === nothing || maximum_initial_time === nothing
        initial_times = collect(InfrastructureSystems.get_forecast_initial_times(sys.data))
        if minimum_initial_time === nothing
            minimum_initial_time = first(initial_times)
        else
            minimum_initial_time = minimum_initial_time
        end
        if maximum_initial_time === nothing
            maximum_initial_time = last(initial_times)
        else
            maximum_initial_time = maximum_initial_time
        end
    end

    # Initialize results dictionary
    res_dict = Dict{DateTime, OptimizationProblemResults}()

    # Loop through each time slice
    for initial_time_slice in minimum_initial_time:window_shift:maximum_initial_time
        # Create and solve the decision model with the current time slice
        problem = DecisionModel(
            template, sys;
            horizon=schedule_horizon,
            initial_time=initial_time_slice,
            optimizer=optimizer,
        )

        build!(problem; output_dir=mktempdir())
        solve!(problem)
        res_dict[initial_time_slice] = OptimizationProblemResults(problem)
    end

    return res_dict
end

results = run_decision_model_loop(
    template_uc, sys,
    solver;
    schedule_horizon=Hour(24),
    window_shift=Hour(24)
)
