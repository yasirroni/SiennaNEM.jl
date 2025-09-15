function read_system_data_csv(data_dir)
    bus_path = joinpath(data_dir, "Bus.csv")
    generator_path = joinpath(data_dir, "Generator.csv")
    line_path = joinpath(data_dir, "Line.csv")
    demand_path = joinpath(data_dir, "Demand.csv")
    storage_path = joinpath(data_dir, "ESS.csv")

    data = Dict{String, Any}()
    data["bus"] = CSV.read(bus_path, DataFrame)
    data["generator"] = CSV.read(generator_path, DataFrame)
    data["line"] = CSV.read(line_path, DataFrame)
    data["demand"] = CSV.read(demand_path, DataFrame)
    data["storage"] = CSV.read(storage_path, DataFrame)
    
    add_fuel_col!(data["generator"])
    add_primemover_col!(data["generator"])
    add_datatype_col!(data["generator"])

    add_primemover_col!(data["storage"])
    add_datatype_col!(data["storage"])
    return data
end

function read_ts_data_csv!(data, data_dir)
    demand_ts_path = joinpath(data_dir, "Demand_load_sched.csv")
    generator_ts_path = joinpath(data_dir, "Generator_pmax_sched.csv")
    data["demand_ts"] = add_day!(CSV.read(demand_ts_path, DataFrame))
    data["generator_ts"] = add_day!(CSV.read(generator_ts_path, DataFrame))
    return data
end

function add_fuel_col!(df)
    transform!(df, :tech => ByRow(t -> get(tech_to_fuel, t, missing)) => :ThermalFuels)
end
function add_primemover_col!(df)
    transform!(df, :tech => ByRow(t -> tech_to_primemover[t]) => :PrimeMovers)
end
function add_datatype_col!(df)
    transform!(df, :tech => ByRow(t -> tech_to_datatype[t]) => :DataType)
end
