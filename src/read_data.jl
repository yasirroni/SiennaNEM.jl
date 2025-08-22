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

    return data
end

function read_ts_data_csv!(data, data_dir)
    demand_ts_path = joinpath(data_dir, "Demand_load_sched.csv")
    generator_ts_path = joinpath(data_dir, "Generator_pmax_sched.csv")
    data["demand_ts"] = preprocess_date!(CSV.read(demand_ts_path, DataFrame))
    data["generator_ts"] = preprocess_date!(CSV.read(generator_ts_path, DataFrame))
    return data
end
