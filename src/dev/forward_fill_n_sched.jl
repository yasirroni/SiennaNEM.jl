using SiennaNEM
using DataFrames
using CSV
using Dates

function forward_fill!(df, exclude_cols=[:date])
    for col_name in names(df)
        if !(col_name in exclude_cols)
            col_values = df[!, col_name]
            last_valid = col_values[1]  # Start with first value
            
            for i in 2:length(col_values)
                if ismissing(col_values[i])
                    col_values[i] = last_valid
                else
                    last_valid = col_values[i]  # Update last valid value
                end
            end
        end
    end
end

# Load data
system_data_dir = "data/nem12/arrow"
ts_data_dir = joinpath(system_data_dir, "schedule-1w")
data = read_system_data(system_data_dir)
read_ts_data!(data, ts_data_dir)

df_bus = data["bus"]
df_generator = data["generator"]
df_line = data["line"]
df_demand = data["demand"]
df_storage = data["storage"]

df_demand_l_ts = data["demand_l_ts"]
df_generator_pmax_ts = data["generator_pmax_ts"]
df_generator_n_ts = data["generator_n_ts"]
df_der_p_ts = data["der_p_ts"]
df_storage_emax_ts = data["storage_emax_ts"]
df_storage_lmax_ts = data["storage_lmax_ts"]
df_storage_n_ts = data["storage_n_ts"]
df_storage_pmax_ts = data["storage_pmax_ts"]
df_line_tmax_ts = data["line_tmax_ts"]
df_line_tmin_ts = data["line_tmin_ts"]

function create_timeseries_df(df_static, df_ts, id_col, static_col_ref, scenario, date_start, date_end)
    """
    Create a time series DataFrame with forward-filled values.
    
    Parameters:
    - df_static: Static data DataFrame
    - df_ts: Time series data DataFrame
    - id_col: Column name for ID (e.g., "id_gen")
    - static_col_ref: Column name for static reference (e.g., "pmax", "n")
    - scenario: Scenario number
    - date_start: Start date
    - date_end: End date
    
    Returns:
    - df_ts_out: Output time series DataFrame
    - df_ts_before_selected: DataFrame with data before date_start (for debugging)
    - df_ts_selected: DataFrame with data in the selected period (for debugging)
    """
    
    # Create initial data
    id_col_val = string.(df_static[!, id_col])
    static_col_ref_val = df_static[!, static_col_ref]
    df_init = DataFrame(Dict(zip(id_col_val, static_col_ref_val)))

    # Get data before selected period
    df_ts_before_selected = filter(
        row -> row.date < date_start && row.scenario == scenario,
        df_ts
    )

    # Update df_init with latest values before selected period
    if nrow(df_ts_before_selected) > 0
        # Get latest values before date
        df_ts_before_selected = combine(groupby(df_ts_before_selected, id_col)) do group
            subset(group, :date => x -> x .== maximum(x))
        end

        # Update init using latest values
        ids_update = string.(df_ts_before_selected[!, id_col])
        gen_values_update = df_ts_before_selected.value
        df_init[1, ids_update] .= gen_values_update
    end

    # Pre-allocate output DataFrame
    # TODO: use interval
    date_range = collect(date_start:Hour(1):date_end)
    df_ts_out = DataFrame(date=date_range)
    for col_name in names(df_init)
        df_ts_out[!, col_name] = [df_init[1, col_name]; fill(missing, length(date_range) - 1)]
    end

    # Get data in selected period and inject values
    df_ts_selected = filter(
        row -> row.date >= date_start
            && row.date <= date_end
            && row.scenario == scenario,
        df_ts
    )
    
    for row in eachrow(df_ts_selected)
        gen_id_col = string(row[id_col])  # Convert id to string (column name)
        target_datetime = row.date       # Use full DateTime
        
        # Find the row index where datetime matches exactly
        date_idx = findfirst(==(target_datetime), df_ts_out.date)
        
        # Update the value if both column and row exist
        if !isnothing(date_idx) && gen_id_col in names(df_ts_out)
            df_ts_out[date_idx, gen_id_col] = row.value
        end
    end

    # Forward fill missing values
    forward_fill!(df_ts_out)

    return df_ts_out, df_ts_before_selected, df_ts_selected
end

# NOTE:
# Sienna does'nt support change of emax and lmax, we need to add that as extra constraints straight to JuMP data
# Sienna also does'nt support change of n, that is number of available unit
# When we add constraints to JuMP model, check first is the timeseries data changes or not

# ================================
# DEBUG SECTION
# ================================

# Configuration for debugging n
# df_static = df_generator
# id_col = "id_gen"
# static_col_ref = "n"
# df_ts = df_generator_n_ts
# target_datetime = DateTime("2024-02-01T00:00:00")
# columns_to_check = ["date", "1", "84", "69"]
# date_start = DateTime(2024, 1, 1)
# date_end = DateTime(2025, 1, 1)

# Configuration for debugging pmax
df_static = df_generator
id_col = "id_gen"
static_col_ref = "pmax"
df_ts = df_generator_pmax_ts
target_datetime = DateTime("2044-06-30T00:00:00")
columns_to_check = ["date", "78", "79"]
date_start = DateTime(2044, 6, 28)
date_end = DateTime(2044, 7, 2)

# Scenario
scenario = 1

# Create time series
df_ts_out, df_ts_before_selected, df_ts_selected = create_timeseries_df(
    df_static, df_ts, id_col, static_col_ref, scenario, date_start, date_end
)

# Debug output
println("Before selected period:")
println(df_ts_before_selected)
println("\nSelected period:")
println(df_ts_selected)

# Calculate the datetime range for display
window_hours = 48
start_datetime_show = target_datetime - Hour(window_hours)
end_datetime_show = target_datetime + Hour(window_hours)
date_filter_show = (df_ts_out.date .>= start_datetime_show) .& (df_ts_out.date .<= end_datetime_show)

println("\nTime series output (around target datetime):")
show(df_ts_out[date_filter_show, columns_to_check], allrows=true)