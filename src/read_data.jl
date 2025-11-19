function read_system_data(data_dir::AbstractString)
    files = Dict(
        "bus" => "Bus",
        "demand" => "Demand",
        "der" => "DER",
        "storage" => "ESS",
        "generator" => "Generator",
        "line" => "Line",
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

    # add columns
    bus_to_area = get_map_from_df(data["bus"], :id_bus, :id_area)

    add_fuel_col!(data["generator"])
    add_primemover_col!(data["generator"])
    add_datatype_col!(data["generator"])
    add_id_area_col!(data["generator"], bus_to_area)

    add_primemover_col!(data["storage"])
    add_datatype_col!(data["storage"])
    add_id_area_col!(data["storage"], bus_to_area)

    # add dataframes
    add_area_df!(data)
    add_generator_extended_df!(data)

    # NOTE:
    #   add_maps! is optional and is used in post-processing, except for
    # bus_to_area. Should we store the map since read data?
    # 
    # add maps
    # add_maps!(data)

    return data
end

function read_ts_data!(data::Dict{String,Any}, data_dir::AbstractString)
    files = Dict(
        "demand_l_ts" => "Demand_load_sched",
        "generator_pmax_ts" => "Generator_pmax_sched",
        "generator_n_ts" => "Generator_n_sched",
        "der_p_ts" => "DER_pred_sched",
        "storage_emax_ts" => "ESS_emax_sched",
        "storage_lmax_ts" => "ESS_lmax_sched",
        "storage_n_ts" => "ESS_n_sched",
        "storage_pmax_ts" => "ESS_pmax_sched",
        "line_tmax_ts" => "Line_tmax_sched",
        "line_tmin_ts" => "Line_tmin_sched",
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

function add_id_area_col!(df, bus_to_area)
    transform!(df, :id_bus => ByRow(b -> bus_to_area[b]) => :id_area)
end

function add_tsf_data!(
    data::Dict{String,Any};
    scenario_name=1,
    date_start=nothing,
    date_end=nothing,
)
    # NOTE:
    # This function create and add time series forward-filled DataFrames to `data` Dict.
    # This is required because the status data from the raw data is not in full format.

    # TODO:
    #   1. Request data["generator_pmax_ts"] to change only the capacity, not trace of RE
    #   2. Implement DER

    # NOTE: we assume row number equal with id_ for speed

    df_generator = data["generator"]
    df_storage = data["storage"]
    df_line = data["line"]

    df_demand_l_ts = data["demand_l_ts"]
    df_generator_pmax_ts = data["generator_pmax_ts"]
    df_generator_n_ts = data["generator_n_ts"]
    # df_der_p_ts = data["der_p_ts"]
    df_storage_emax_ts = data["storage_emax_ts"]
    df_storage_lmax_ts = data["storage_lmax_ts"]
    df_storage_n_ts = data["storage_n_ts"]
    df_storage_pmax_ts = data["storage_pmax_ts"]
    df_line_tmax_ts = data["line_tmax_ts"]
    df_line_tmin_ts = data["line_tmin_ts"]

    if date_start === nothing
        date_start = minimum(df_demand_l_ts.date)
    end
    if date_end === nothing
        date_end = maximum(df_demand_l_ts.date)
    end

    data["generator_n_tsf"] = get_full_ts_df(
        df_generator, df_generator_n_ts, "id_gen", "n", scenario_name, date_start, date_end
    )
    data["generator_pmax_tsf"] = get_full_ts_df(
        df_generator, df_generator_pmax_ts, "id_gen", "pmax", scenario_name, date_start, date_end
    )

    data["storage_emax_tsf"] = get_full_ts_df(
        df_storage, df_storage_emax_ts, "id_ess", "emax", scenario_name, date_start, date_end
    )
    data["storage_lmax_tsf"] = get_full_ts_df(
        df_storage, df_storage_lmax_ts, "id_ess", "lmax", scenario_name, date_start, date_end
    )
    data["storage_n_tsf"] = get_full_ts_df(
        df_storage, df_storage_n_ts, "id_ess", "n", scenario_name, date_start, date_end
    )
    data["storage_pmax_tsf"] = get_full_ts_df(
        df_storage, df_storage_pmax_ts, "id_ess", "pmax", scenario_name, date_start, date_end
    )

    data["line_tmax_tsf"] = get_full_ts_df(
        df_line, df_line_tmax_ts, "id_lin", "tmax", scenario_name, date_start, date_end
    )
    data["line_tmin_tsf"] = get_full_ts_df(
        df_line, df_line_tmin_ts, "id_lin", "tmin", scenario_name, date_start, date_end
    )
end

function update_system_data_bound!(data::Dict{String,Any})
    # TODO: update to capacity and profile separately if the new data available
    df_generator = data["generator"]
    df_storage = data["storage"]
    df_line = data["line"]

    ids_gen_nvre = findall(x -> x ∉ ["Solar", "Wind"], data["generator"].fuel)
    df_generator[!, "n"] = Matrix(data["generator_n_tsf"][!, Not(:date)])[end, :]
    df_generator[!, "pmax"] = Vector(data["generator"].pmax)
    df_generator[ids_gen_nvre, "pmax"] = Matrix(data["generator_pmax_tsf"][!, string.(ids_gen_nvre)])[end, :]

    df_storage[!, "emax"] = Matrix(data["storage_emax_tsf"][!, Not(:date)])[end, :]
    df_storage[!, "lmax"] = Matrix(data["storage_lmax_tsf"][!, Not(:date)])[end, :]
    df_storage[!, "emax"] = Matrix(data["storage_emax_tsf"][!, Not(:date)])[end, :]
    df_storage[!, "lmax"] = Matrix(data["storage_lmax_tsf"][!, Not(:date)])[end, :]
    df_storage[!, "n"] = Matrix(data["storage_n_tsf"][!, Not(:date)])[end, :]
    df_storage[!, "pmax"] = Matrix(data["storage_pmax_tsf"][!, Not(:date)])[end, :]

    df_line[!, "tmax"] = Matrix(data["line_tmax_tsf"][!, Not(:date)])[end, :]
    df_line[!, "tmin"] = Matrix(data["line_tmin_tsf"][!, Not(:date)])[end, :]
end

function extend_generator_data(df::DataFrame)
    """
    Extend generator DataFrame by creating individual units for each generator.
    Each generator with `n` units will be expanded into `n` rows, each representing
    a single unit with its own `id_unit` and `id_gen_unit`.
    """
    return vcat([
        let
            n_units = row.n
            id_gen = row.id_gen
            df_temp = DataFrame(fill(NamedTuple(row), n_units))
            df_temp.id_unit = 1:n_units
            df_temp.id_gen_unit = ["$(id_gen)_$(i)" for i in 1:n_units]
            df_temp
        end
        for row in eachrow(df)
    ]...)
end

function add_area_df!(data)
    # TODO: properly calculate peak_active_power and peak_reactive_power columns
    data["area"] = unique(data["bus"][!, [:id_area]])
    data["area"].name = [area_to_name[id] for id in data["area"].id_area]
    data["area"].peak_active_power .= 0.0
    data["area"].peak_reactive_power .= 0.0

    area_to_max_pmax = get_group_max(data["generator"], :id_area, :pmax)
    data["area"].max_pmax = [area_to_max_pmax[id] for id in data["area"].id_area]
end

function add_generator_extended_df!(data)
    data["generator_extended"] = extend_generator_data(data["generator"])
end

function add_maps!(data)
    bus_to_area = get_map_from_df(data["bus"], :id_bus, :id_area)
    gen_unit_to_pmax = get_map_from_df(data["generator_extended"], :id_gen_unit, :pmax)
    gen_unit_to_pfrmax = get_map_from_df(data["generator_extended"], :id_gen_unit, :pfrmax)
    gen_to_bus = get_map_from_df(data["generator"], :id_gen, :id_bus)
    gen_unit_to_gen = get_map_from_df(data["generator_extended"], :id_gen_unit, :id_gen)

    area_to_bus = get_grouped_map_from_df(data["bus"], :id_area, :id_bus)
    gen_to_units = get_grouped_map_from_df(data["generator_extended"], :id_gen, :id_unit)

    data["map"] = Dict(
        "bus_to_area" => bus_to_area,
        "gen_unit_to_pmax" => gen_unit_to_pmax,
        "gen_unit_to_pfrmax" => gen_unit_to_pfrmax,
        "gen_to_bus" => gen_to_bus,
        "gen_unit_to_gen" => gen_unit_to_gen,
        "area_to_bus" => area_to_bus,
        "gen_to_units" => gen_to_units,
    )
end

function get_group_max(df::DataFrame, group_col::Symbol, value_col::Symbol)
    """
    Get maximum value for each group.

    # Arguments
    - `df::DataFrame`: Input DataFrame
    - `group_col::Symbol`: Column to group by (e.g., :id_area, :id_bus)
    - `value_col::Symbol`: Column to aggregate (e.g., :pmax, :capacity)

    # Returns
    - `Dict`: Dictionary mapping group IDs to maximum values

    # Examples
    ```julia
    area_to_max_pmax = get_group_max(data["generator"], :id_area, :pmax)
    bus_to_max_pmax = get_group_max(data["generator"], :id_bus, :capacity)
    area_to_max_emax = get_group_max(data["storage"], :id_area, :emax)
    ```
    """
    df_agg = combine(groupby(df, group_col), value_col => maximum => :max_value)
    Dict(row[group_col] => row.max_value for row in eachrow(df_agg))
end
