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

Set up and solve multiple unit commitment decision models in a sequential loop
across forecast time windows.

This function iterates through each forecast initial time in the system and solves
independent optimization problems. Currently implements a manual loop approach.

**Note:** Initial conditions (generator status, power, MUT/MDT, storage SoC) are NOT
automatically propagated between windows in this implementation. Each window is solved
independently. Future implementation will use PowerSimulations' `SimulationSequence` with
`InterProblemChronology()` for proper chronology handling.

# Arguments
- `template::ProblemTemplate`: The problem template defining the UC formulation.
- `sys::System`: The PowerSystems system object containing the network and time series data.
  Must be pre-built with time series using `add_ts!` with desired `horizon` and `interval`.

# Keyword Arguments
- `simulation_name::Union{String, Nothing}=nothing`: Currently unused. Reserved for future
  Simulation-based implementation.
- `simulation_folder::Union{String, Nothing}=nothing`: Currently unused. Reserved for future
  Simulation-based implementation.
- `simulation_steps::Union{Int, Nothing}=nothing`: Currently unused. Loop runs for all
  forecast windows in the system.
- `decision_model_kwargs::NamedTuple=(;)`: Keyword arguments for `DecisionModel` constructor
  (e.g., `optimizer`, `warm_start`, etc.)
- `simulation_kwargs::NamedTuple=(;)`: Currently unused. Reserved for future implementation.

# Returns
- `Dict{DateTime, OptimizationProblemResults}`: Dictionary mapping each forecast initial time
  to its corresponding optimization results. Each result is independent (no chronology).

# Notes
- Each time window is solved independently without initial condition propagation.
- Storage state of charge does NOT carry over between windows.
- Generator status, power levels, and MUT/MDT counters reset each window.
- For proper multi-period optimization with chronology, this will be replaced with
  PowerSimulations' `Simulation` infrastructure (see commented code).

# See Also
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
    for (step, initial_time_slice) in enumerate(InfrastructureSystems.get_forecast_initial_times(sys.data))
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

        # Break if we've reached the desired number of simulation steps
        if step >= simulation_steps
            break
        end
    end
    return res_dict
end
