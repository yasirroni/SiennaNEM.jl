using Test
using SiennaNEM
using PowerSystems
using Dates

const PSY = PowerSystems

function test_system_creation(system_data_dir::String, backend::String)
    """
    Helper function to test system creation from data directory.
    
    Args:
        system_data_dir: Path to system data directory
    """
    data = read_system_data(system_data_dir)
    @testset "[$(backend)] Read system data" begin
        system_data_keys = [
            "bus",
            "storage",
            "generator",
            "line",
            "demand",
        ]
        missing_keys_system_data = filter(k -> !haskey(data, k), system_data_keys)
        @test isempty(missing_keys_system_data) || error("Missing main data keys: $(missing_keys_system_data)")
    end

    ts_data_dir = joinpath(system_data_dir, "schedule-1w")
    read_ts_data!(data, ts_data_dir)
    @testset "[$(backend)] Read timeseries data" begin
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
            "generator_pmax_ts",
        ]
        missing_keys_ts_data = filter(k -> !haskey(data, k), timeseries_data_keys)
        @test isempty(missing_keys_ts_data) || error("Missing timeseries data keys: $(missing_keys_ts_data)")
    end

    scenario_name = 1
    date_start = DateTime("2025-01-07T00:00:00")
    date_end = DateTime("2025-01-23T00:00:00")
    add_tsf_data!(data, scenario_name=scenario_name, date_start=date_start, date_end=date_end)
    update_system_data_bound!(data)
    @testset "[$(backend)] Add tsf and update bound" begin
        # TODO: test add tsf and update bound
    end

    sys = create_system!(data)
    @testset "[$(backend)] Create system" begin
        system_keys = [
            "sys",
            "baseMVA",
            "components"
        ]
        missing_keys_system = filter(k -> !haskey(data, k), system_keys)
        @test isempty(missing_keys_system) || error("Missing system keys: $(missing_keys_system)")
        @test typeof(data["sys"]) == PSY.System
    end

    add_ts!(sys, data, scenario_name=1)
    @testset "[$(backend)] Add timeseries" begin
        # TODO: test add ts
    end

    return data, sys
end

test_system_creation("../data/nem12/csv", "csv")
test_system_creation("../data/nem12/arrow", "arrow")
