using Test
using SiennaNEM
using PowerSystems

const PSY = PowerSystems

function test_system_creation(system_data_dir::String, test_keys::Bool = true)
    """
    Helper function to test system creation from data directory.
    
    Args:
        system_data_dir: Path to system data directory
        test_keys: Whether to perform detailed key validation tests
    """
    # read system data
    ts_data_dir = joinpath(system_data_dir, "schedule-1w")
    data = read_system_data(system_data_dir)
    system_data_keys = [
        "bus",
        "storage",
        "generator",
        "line",
        "demand",
    ]
    missing_keys_system = filter(k -> !haskey(data, k), system_data_keys)
    @test isempty(missing_keys_system) || error("Missing main keys: $(missing_keys_system)")

    # read ts data
    read_ts_data!(data, ts_data_dir)
    timeseries_data_keys = [
        "der_p_ts",
        "storage_lmax_ts",
        "storage_n_ts",
        "line_tmax_ts",
        "line_tmin_ts",
        "generator_n_ts",
        "storage_emax_ts",
        "demand_l_ts",
        "storage_pmax_ts",
        "generator_pmax_ts"
    ]
    missing_keys_ts = filter(k -> !haskey(data, k), timeseries_data_keys)
    @test isempty(missing_keys_ts) || error("Missing timeseries keys: $(missing_keys_ts)")

    # create system
    sys = create_system!(data)
    system_data_keys = [
        "sys",
        "baseMVA",
        "components"
    ]
    missing_keys_system = filter(k -> !haskey(data, k), system_data_keys)
    @test isempty(missing_keys_system) || error("Missing main keys: $(missing_keys_system)")
    @test typeof(data["sys"]) == PSY.System

    # add ts
    add_ts!(sys, data, scenario_name=1)
    return data, sys
end

@testset "Create System Tests from CSV" begin
    system_data_dir = "../data/nem12/csv"
    test_system_creation(system_data_dir, true)
end

@testset "Create System Tests from Arrow" begin
    system_data_dir = "../data/nem12/arrow"
    test_system_creation(system_data_dir, true)
end
