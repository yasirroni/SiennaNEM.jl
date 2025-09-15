using DataFrames, Dates, TimeSeries

function add_day!(df)
    transform!(df, :date => ByRow(x -> Date(x)) => :day)
end

function date_as_datetime!(df)
    if !(eltype(df.date) <: DateTime)
        df.date = DateTime.(df.date, DateFormat("yyyy-mm-dd HH:MM:SS"))
    end
end

function preprocess_date!(df)
    date_as_datetime!(df)
    add_day!(df)
end

function groupbyd(df::Union{DataFrame, SubDataFrame}, col::Symbol)
    return Dict(g[!, col][1] => g for g in groupby(df, col))
end

function groupby_scenario_at_init_day(df::DataFrame)
    init_time = minimum(df[!, :date])
    init_day = Date(init_time)
    return groupby_scenario_at_day(df, init_day)
end

function groupby_scenario_at_day(df::DataFrame, day::Date)
    df_init_day = subset(df, :day => ByRow(==(day)))
    return groupbyd(df_init_day, :scenario)
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

# NOTE: example usages
# 
# scenario = 1
# data_dir = "data/arrow/"
# path = joinpath(data_dir, "Demand_load_sched.arrow")
# df_ts = DataFrame(Arrow.Table(path))
# dfs_ts = groupby_scenario_at_init_day(df_ts)
# df_ts_s = dfs_ts[scenario]

function show_parameter(df_parameter)
    # NOTE: I don't know why it require println()
    show(df_parameter, allrows=true, allcols=true)
    println()
end

function sort_cols(df)
    """
    Sort nested columns while moving the DateTime in the first position.
    Sorting order: ascending order.
    """
    datetime_col = "DateTime"
    other_cols = filter(!=(datetime_col), names(df))
    sorted_cols = sort(other_cols; by = name -> parse(Int, name))
    return select(df, [datetime_col; sorted_cols])
end

function sort_nested_cols(df)
    """
    Sort nested columns while moving the DateTime in the first position.
    Sorting order: for each M value, sort all N values in ascending order.
    """
    datetime_col = "DateTime"
    m_n_cols = filter(!=(datetime_col), names(df))
    sorted_cols = sort(m_n_cols; by = name -> parse.(Int, split(name, "_")))
    return select(df, [datetime_col; sorted_cols])
end

function diff_df(df1::DataFrame, df2::DataFrame; timecol::Symbol = :DateTime)
    numeric_names = names(df1, Not(timecol))
    numeric_diff = DataFrame(
        Matrix(select(df1, Not(timecol))) .- Matrix(select(df2, Not(timecol))),
        numeric_names
    )
    return hcat(DataFrame(timecol => df1[!, timecol]), numeric_diff)
end
