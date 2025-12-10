using Dates
using PowerSystems
using PowerSimulations

"""
    run_decision_model(
        template::ProblemTemplate,
        sys::System;
        kwargs...
    )

Set up and solve a single unit commitment decision model.

# Arguments
- `template::ProblemTemplate`: The problem template defining the UC formulation.
- `sys::System`: The PowerSystems system object containing the network and time series data.

# Keyword Arguments
- `kwargs...`: Additional keyword arguments to pass to `DecisionModel` constructor
  (e.g., `optimizer`, `horizon`, `initial_time`, `warm_start`, etc.)

# Returns
- `OptimizationProblemResults`: The optimization results containing objective value,
  variable values, and other solution information.

# Note
- The function requires the system to be pre-built with time series data (see `uc_build_problem.jl`).
- The `initial_time` must align with the time series data timestamps.
- A temporary directory is created for problem output and automatically cleaned up.
"""
function run_decision_model(
    template::ProblemTemplate,
    sys::System;
    kwargs...
)
    # Create and solve the decision model
    problem = DecisionModel(
        template, sys;
        kwargs...
    )

    build!(problem; output_dir=mktempdir())
    solve!(problem)

    return OptimizationProblemResults(problem)
end

"""
    run_decision_model_loop(
        template::ProblemTemplate,
        sys::System;
        simulation_name::Union{String, Nothing}=nothing,
        simulation_folder::Union{String, Nothing}=nothing,
        simulation_steps::Union{Int, Nothing}=nothing,
        decision_model_kwargs::NamedTuple=(;),
        simulation_kwargs::NamedTuple=(;),
    )

Set up and solve multiple unit commitment decision models using PowerSimulations'
`Simulation` infrastructure with moving horizon optimization.

This function leverages PowerSimulations' `Simulation` and `InterProblemChronology()`
to automatically handle:
- Initial conditions propagation between windows (generator status, power, MUT/MDT, SoC)
- Time window advancement based on system's forecast interval
- Proper sequencing of optimization problems

# Arguments
- `template::ProblemTemplate`: The problem template defining the UC formulation.
- `sys::System`: The PowerSystems system object containing the network and time series data.
  Must be pre-built with time series using `add_ts!` with desired `horizon` and `interval`.

# Keyword Arguments
- `simulation_name::Union{String, Nothing}=nothing`: Name for the simulation and decision model.
  If `nothing`, defaults to "DA-UC".
- `simulation_folder::Union{String, Nothing}=nothing`: Directory to store simulation results.
  If `nothing`, creates a temporary directory that is automatically cleaned up.
- `simulation_steps::Union{Int, Nothing}=nothing`: Number of simulation steps to execute.
  If `nothing`, auto-detects from system's forecast window count.
- `decision_model_kwargs::NamedTuple=(;)`: Keyword arguments for `DecisionModel` constructor
  (e.g., `optimizer`, `warm_start`, `initial_time`, etc.)
- `simulation_kwargs::NamedTuple=(;)`: Keyword arguments for `Simulation` constructor
  (e.g., `initial_time`, etc. Note: `steps` is handled by `simulation_steps` parameter)

# Returns
- `SimulationResults`: PowerSimulations simulation results object containing all decision
  problem results across all time windows. Use `get_decision_problem_results()` and
  `read_realized_variables()` to access specific results.

# Notes
- The system's time series structure (horizon and interval from `add_ts!`) determines:
  * Optimization window size (horizon)
  * How far the window advances each step (interval, not resolution)
- Initial conditions (P, status, MUT/MDT, SoC) automatically propagate between windows
  via `InterProblemChronology()`.
- Use `read_realized_variables()` to extract only committed decisions (first interval of each window).

# See Also
- PowerSimulations.jl documentation on Simulations and Chronologies
- `run_decision_model()` for single-window optimization
"""
function run_decision_model_loop(
    template::ProblemTemplate,
    sys::System;
    simulation_name::Union{String,Nothing}=nothing,
    simulation_folder::Union{String,Nothing}=nothing,
    simulation_steps::Union{Int,Nothing}=nothing,
    decision_model_kwargs::NamedTuple=(;),
    simulation_kwargs::NamedTuple=(;),
)

    if simulation_name === nothing
        simulation_name = "DA-UC"
    end

    if simulation_folder === nothing
        simulation_folder = mktempdir()
    else
        mkpath(simulation_folder)
    end

    if simulation_steps === nothing
        simulation_steps = IS.get_forecast_window_count(sys.data)
    end

    # NOTE:
    #   Currently, there is a bug in Sienna that make SimulationSequence didn't
    # work with StorageSystemsSimulations. Thus, we temporarily use manual loop
    # and return dict of OptimizationProblemResults(problem).

    # # Create decision model with the provided template
    # decision_model = DecisionModel(
    #   template, sys;
    #   decision_model_kwargs...
    # )

    # # Create simulation models container
    # sim_models = SimulationModels(
    #   decision_models=[decision_model]
    # )

    # # Define sequence with InterProblemChronology for automatic initial condition propagation
    # sequence = SimulationSequence(
    #   models=sim_models,
    #   ini_cond_chronology=InterProblemChronology(),
    # )

    # # Create and build simulation
    # sim = Simulation(;
    #   models=sim_models,
    #   sequence=sequence,
    #   name=simulation_name,
    #   simulation_folder=simulation_folder,
    #   steps=simulation_steps,
    #   simulation_kwargs...
    # )

    # build!(sim)
    # execute!(sim)

    # return SimulationResults(sim)

    # Loop through each time slice
    # TODO
    # - Use Chronology, that is SimulationSequence.
    # - Support passing storage last state of charge into initial state of charge on the next loop.
    # - Support schedule_horizon wider than window_shift to have overlapping time slices.
    res_dict = Dict{DateTime,OptimizationProblemResults}()
    for initial_time_slice in InfrastructureSystems.get_forecast_initial_times(sys.data)
        # Create and solve the decision model with the current time slice
        problem = DecisionModel(
            template, sys;
            horizon=InfrastructureSystems.get_forecast_horizon(sys.data),
            initial_time=initial_time_slice,
            decision_model_kwargs...
        )

        build!(problem; output_dir=mktempdir())
        solve!(problem)
        res_dict[initial_time_slice] = OptimizationProblemResults(problem)
    end
    return res_dict
end
