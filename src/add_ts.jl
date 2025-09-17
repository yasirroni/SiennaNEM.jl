function add_ts!(
    sys, data; 
    horizon=nothing,    # auto-detect from data["demand_ts"] if nothing
    interval=nothing,   # auto-detect from data["demand_ts"] if nothing
    scenario_name=1
)
    # TODO: use multiple scenarios as mentioned in
    #   https://nrel-sienna.github.io/PowerSystems.jl/stable/explanation/time_series/#Forecasts
    df_generator = data["generator"]
    df_demand_ts = data["demand_ts"]
    df_generator_ts = data["generator_ts"]
    renewable_dispatch_generators = data["components"]["renewable_dispatch_generators"]
    renewable_nondispatch_generators = data["components"]["renewable_nondispatch_generators"]
    demands = data["components"]["demands"]

    # read generator ts
    # NOTE: remove missing due to id_gen == 78
    unique_gen_id_ts = unique(df_generator_ts[:, :id_gen]) 
    df_generator_ts_unique_gen_id = DataFrame(id_gen=unique_gen_id_ts)
    df_generator_ts_unique_gen_id = leftjoin(
        df_generator_ts_unique_gen_id,
        df_generator[:, [:id_gen, :name, :tech, :DataType]], on=:id_gen => :id_gen
    )
    df_generator_ts_unique_gen_id = dropmissing(df_generator_ts_unique_gen_id, :name)
    unique_gen_id_ts = df_generator_ts_unique_gen_id[:, :id_gen]
    df_generator_ts = filter(row -> row.id_gen in unique_gen_id_ts, df_generator_ts)

    # add demand st
    dfs_demand_ts = groupbyd(df_demand_ts, :scenario)
    dfs_demand_ts_s = groupbyd(dfs_demand_ts[scenario_name], :id_dem)
    add_sts!(sys, demands, dfs_demand_ts_s, :id_dem)

    # add generator st
    dfs_generator_ts = groupbyd(df_generator_ts, :scenario)
    dfs_generator_ts_s = groupbyd(dfs_generator_ts[scenario_name], :id_gen)
    add_sts!(sys, renewable_dispatch_generators, dfs_generator_ts_s, :id_gen)
    add_sts!(sys, renewable_nondispatch_generators, dfs_generator_ts_s, :id_gen)

    # add forecast time series from StaticTimeSeries
    if horizon === nothing || interval === nothing
        first_key = first(keys(dfs_demand_ts_s))
        dates = dfs_demand_ts_s[first_key][!, :date]
        if horizon === nothing
            horizon = dates[end] - dates[1]
        end
        if interval === nothing
            interval = dates[2] - dates[1]
        end
    end
    transform_single_time_series!(
        sys,
        horizon, # horizon, for example Dates.Hour(24)
        interval, # interval, for example Dates.Minute(60)
    );

    data["generator_ts"] = df_generator_ts
    data["demand_ts"] = df_demand_ts
end

# TODO:
#   1. Support multiple ts
#   2. Normalize data in p.u. not in here, but in reading data
function add_sts!(
    sys::PSY.System,
    instances::Dict{Int, T},
    dfs::Dict{Int, S},
    col::Symbol,
) where {T <: PSY.PowerLoad, S <: SubDataFrame}
    units_base_system = get_units_base(sys)
    set_units_base_system!(sys, "NATURAL_UNITS")
    for (sym_id, instance) in instances
        df = copy(dfs[sym_id][:, [col, :date, :value]])
        df.value .= df.value ./ get_max_active_power(instance)
        add_st!(sys, instance, df)
    end
    set_units_base_system!(sys, units_base_system)
end

function add_st!(
    sys::PSY.System,
    instance::PSY.PowerLoad,
    df::Union{DataFrame, SubDataFrame},
)
    # NOTE: df.value must be already normalized
    ta = TimeArray(df[:, [:date, :value]], timestamp=:date)
    sts = SingleTimeSeries(;
        name="max_active_power",  # 1 means first row
        data=ta,
        scaling_factor_multiplier=get_max_active_power,
    )
    add_time_series!(sys, instance, sts);
end

function add_sts!(
    sys::PSY.System,
    nested_instances::Dict{Int,Dict{Int,T}},
    dfs::Dict{Int, S},
    col::Symbol,
) where {T <: PSY.RenewableGen, S <: SubDataFrame}
    units_base_system = get_units_base(sys)
    set_units_base_system!(sys, "NATURAL_UNITS")
    for (sym_id, instances) in nested_instances
        df = copy(dfs[sym_id][:, [col, :date, :value]])
        max_active_power = get_max_active_power(first(values(instances)))
        df.value .= df.value ./ max_active_power
        add_st!(sys, instances, df)
    end
    set_units_base_system!(sys, units_base_system)
end

function add_st!(
    sys::PSY.System,
    instances::Dict{Int,T},
    df::Union{SubDataFrame, DataFrame},
) where {T <: PSY.RenewableGen}
    # NOTE: df must be already normalized
    ta = TimeArray(df[:, [:date, :value]], timestamp=:date)
    sts = SingleTimeSeries(;
        name="max_active_power",  # 1 means first row
        data=ta,
        scaling_factor_multiplier=get_max_active_power,
    )
    add_time_series!(sys, values(instances), sts);
end
