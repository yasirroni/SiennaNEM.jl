function read_system_data(data_dir::AbstractString)
    files = Dict(
        "bus"       => "Bus",
        "generator" => "Generator",
        "line"      => "Line",
        "demand"    => "Demand",
        "storage"   => "ESS"
    )

    data = Dict{String,Any}()
    for (k, fname) in files
        path = joinpath(data_dir, fname)
        if isfile(path * ".arrow")
            df = DataFrame(Arrow.Table(path * ".arrow"))
        else
            df = CSV.read(path * ".csv", DataFrame)
        end
        data[k] = df
    end

    add_fuel_col!(data["generator"])
    add_primemover_col!(data["generator"])
    add_datatype_col!(data["generator"])

    add_primemover_col!(data["storage"])
    add_datatype_col!(data["storage"])
    return data
end

function read_ts_data!(data::Dict{String,Any}, data_dir::AbstractString)
    files = Dict(
        "demand_l_ts"    => "Demand_load_sched",
        "generator_pmax_ts" => "Generator_pmax_sched",
        "generator_n_ts" => "Generator_n_sched",
        "der_p_ts"=> "DER_pred_sched",
        "storage_emax_ts"=> "ESS_emax_sched",
        "storage_lmax_ts"=> "ESS_lmax_sched",
        "storage_n_ts"=> "ESS_n_sched",
        "storage_pmax_ts"=> "ESS_pmax_sched",
        "line_tmax_ts"=> "Line_tmax_sched",
        "line_tmin_ts"=> "Line_tmin_sched",
    )

    for (k, fname) in files
        path = joinpath(data_dir, fname)
        if isfile(path * ".arrow")
            df = DataFrame(Arrow.Table(path * ".arrow"))
        else
            df = CSV.read(path * ".csv", DataFrame)
        end
        data[k] = add_day!(df)
    end
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
