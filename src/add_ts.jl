function add_ts!(sys, data; scenario_name=1)
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

    # add demand st (init_day)
    dfs_demand_ts = groupby_scenario_at_init_day(df_demand_ts)
    dfs_demand_ts_s = groupbyd(dfs_demand_ts[scenario_name], :id_dem)
    add_sts!(sys, demands, dfs_demand_ts_s, :id_dem)

    # add generator st (init_day)
    dfs_generator_ts = groupby_scenario_at_init_day(df_generator_ts)
    dfs_generator_ts_s = groupbyd(dfs_generator_ts[scenario_name], :id_gen)
    add_sts!(sys, renewable_dispatch_generators, dfs_generator_ts_s, :id_gen)
    add_sts!(sys, renewable_nondispatch_generators, dfs_generator_ts_s, :id_gen)

    # add forecast time series from StaticTimeSeries
    transform_single_time_series!(
        sys,
        Dates.Hour(24), # horizon
        Dates.Minute(60), # interval
    );

    data["generator_ts"] = df_generator_ts
    data["demand_ts"] = df_demand_ts
end