using Test
using Dates
using HiGHS

using PowerSystems
using PowerSimulations
using InfrastructureSystems

using SiennaNEM


function test_system_creation(system_data_dir::String, ts_data_dir::String, backend::String)
    """
    Helper function to test system creation from data directory.

    Args:
        system_data_dir: Path to system data directory
        ts_data_dir: Path to time series data directory
        backend: Backend type, "arrow" or "csv"
    """

    horizon = Hour(48)
    interval = Hour(24)
    scenario = 1
    simulation_output_folder = "test/sienna-files"
    simulation_name = "test_system_creation_$backend"
    simulation_steps = 2  # number of rolling horizon steps

    # Read data
    data = nothing
    @testset "[$(backend)] Get data" verbose = true begin
        data = SiennaNEM.get_data(system_data_dir, ts_data_dir; file_format=backend)
        @test data !== nothing
    end

    # Create system
    sys_sienna = nothing
    @testset "[$(backend)] Create system" verbose = true begin
        sys_sienna = SiennaNEM.create_system!(data)
        @test sys_sienna !== nothing
        @test typeof(sys_sienna) <: PowerSystems.System
    end

    # Add time series
    @testset "[$(backend)] Add time series" verbose = true begin
        SiennaNEM.add_ts!(
            sys_sienna, data;
            horizon=horizon,
            interval=interval,
            scenario_name=scenario,
        )
        @test InfrastructureSystems.get_forecast_horizon(sys_sienna.data) == horizon
    end

    # Build problem template
    template_uc = nothing
    @testset "[$(backend)] Build problem template" verbose = true begin
        template_uc = SiennaNEM.build_problem_base_uc()
        @test template_uc !== nothing
        @test typeof(template_uc) <: PowerSimulations.ProblemTemplate
    end

    # Run decision model loop
    @testset "[$(backend)] Run decision model loop" verbose = true begin
        decision_models = SiennaNEM.run_decision_model_loop(
            template_uc, sys_sienna;
            simulation_folder=simulation_output_folder,
            simulation_name=simulation_name,
            simulation_steps=simulation_steps,
            decision_model_kwargs=(
                optimizer=optimizer_with_attributes(HiGHS.Optimizer, "mip_rel_gap" => 0.01),
            ),
        )
        @test decision_models !== nothing
        @test length(decision_models) <= simulation_steps
        @test all(dm -> typeof(dm) <: PowerSimulations.DecisionModel, values(decision_models))
    end

    return data, sys_sienna
end

# Test with NEM reliability data
nem_reliability_data_dir = joinpath(@__DIR__, "../..", "NEM-reliability-suite")
if isdir(nem_reliability_data_dir)
    @testset "NEM reliability data - Arrow" verbose = true begin
        test_system_creation(
            joinpath(nem_reliability_data_dir, "data", "arrow"),
            joinpath(nem_reliability_data_dir, "data", "arrow", "schedule-1w"),
            "arrow",
        )
    end

    @testset "NEM reliability data - CSV" verbose = true begin
        test_system_creation(
            joinpath(nem_reliability_data_dir, "data", "csv"),
            joinpath(nem_reliability_data_dir, "data", "csv", "schedule-1w"),
            "csv",
        )
    end
end

# Test with PISP data
pisp_data_dir = joinpath(@__DIR__, "../..", "data/pisp-datasets/out-ref4006-poe10")
if isdir(pisp_data_dir)
    # Test Arrow format if available
    arrow_dir = joinpath(pisp_data_dir, "arrow")
    if isdir(arrow_dir)
        schedule_names = filter(
            name -> startswith(name, "schedule-"),
            readdir(arrow_dir)
        )
        if !isempty(schedule_names)
            @testset "PISP data - Arrow" verbose = true begin
                test_system_creation(
                    arrow_dir,
                    joinpath(arrow_dir, schedule_names[1]),
                    "arrow",
                )
            end
        end
    end
    
    # Test CSV format if available
    csv_dir = joinpath(pisp_data_dir, "csv")
    if isdir(csv_dir)
        schedule_names = filter(
            name -> startswith(name, "schedule-"),
            readdir(csv_dir)
        )
        if !isempty(schedule_names)
            @testset "PISP data - CSV" verbose = true begin
                test_system_creation(
                    csv_dir,
                    joinpath(csv_dir, schedule_names[1]),
                    "csv",
                )
            end
        end
    end
end
