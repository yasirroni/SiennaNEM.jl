using SiennaNEM
using DataFrames
using CSV
using Dates

"""
    forward_fill!(df; col_names)

Performs forward fill (last observation carried forward) in a DataFrame.

# Arguments
- `df`: DataFrame to modify in-place
- `col_names`: Collection of column names (Symbols) to forward fill

# Assumptions
- First row values are never missing (pre-allocated/known values)
- Specified columns exist in the DataFrame

# Example
```julia
numeric_cols = [:1_1, :1_2, :2_1, :3_1]
forward_fill!(df; col_names=numeric_cols)
```
"""
function forward_fill!(df::DataFrame; col_names::AbstractVector{String})::Nothing
    for col_name in col_names
        col = df[!, col_name]

        @inbounds begin
            last_valid = col[1]
            for i in 2:length(col)
                if ismissing(col[i])
                    col[i] = last_valid
                else
                    last_valid = col[i]
                end
            end
        end
    end
end

"""
    get_full_ts_df(df_static, df_ts, id_col, col_ref, scenario, date_start, date_end, interval=Dates.Hour(1))

Create a time series DataFrame with forward-filled values by combining static and time series data.

# Arguments
- `df_static`: Static data DataFrame containing baseline values
- `df_ts`: Time series data DataFrame with columns: date, scenario, id_col, value
- `id_col`: Column name for ID (String, e.g., "id_gen", "id_storage")
- `col_ref`: Column name for static reference (String, e.g., "pmax", "n", "emax")
- `scenario`: Scenario number (Integer) to filter time series data
- `date_start`: Start date (DateTime) for the output time series
- `date_end`: End date (DateTime) for the output time series
- `interval`: Time interval for the output series (default: Dates.Hour(1))

# Returns
- `(df_ts_full, col_names_affected)`: Tuple containing:
  - `df_ts_full`: Output time series DataFrame with date column and forward-filled values
  - `col_names_affected`: Vector of all column names that were affected by time series updates

# Process
1. Initialize with static baseline values from df_static
2. Update with latest available values before date_start (if any)
3. Create hourly time series from date_start to date_end
4. Inject specific time series values from df_ts where available
5. Forward-fill missing values to ensure complete coverage

# Example
```julia
df_ts_full, col_names_affected = get_full_ts_df(
    df_generator, df_generator_pmax_ts, "id_gen", "pmax", 1,
    DateTime(2044, 6, 28), DateTime(2044, 7, 2)
)
```
"""
function get_full_ts_df(
    df_static::DataFrame,
    df_ts::DataFrame,
    id_col::String,
    col_ref::String,
    scenario::Integer,
    date_start::DateTime,
    date_end::DateTime,
    interval::Period=Dates.Hour(1)
)::Tuple{DataFrame, Vector{String}}

    # Create initial data
    col_names::Vector{String} = string.(df_static[!, id_col])
    col_ref_val = df_static[!, col_ref]'
    df_init = DataFrame(col_ref_val, col_names)

    # Get data before selected period
    df_ts_before_selected::DataFrame = filter(
        row -> row.date < date_start && row.scenario == scenario,
        df_ts
    )
    col_names_affected_before = unique(string.(df_ts_before_selected[!, id_col]))

    # Update df_init with latest values before selected period
    if nrow(df_ts_before_selected) > 0
        # Get latest values before date
        df_ts_before_selected = combine(groupby(df_ts_before_selected, id_col)) do group
            subset(group, :date => x -> x .== maximum(x))
        end

        # Update init using latest values
        ids_update::Vector{String} = string.(df_ts_before_selected[!, id_col])
        gen_values_update = df_ts_before_selected.value
        df_init[1, ids_update] .= gen_values_update
    end

    # Get data in selected period
    df_ts_selected::DataFrame = filter(
        row -> row.date >= date_start
                    && row.date <= date_end
                    && row.scenario == scenario,
        df_ts
    )
    col_names_affected_selected = unique(string.(df_ts_selected[!, id_col]))

    # Pre-allocate output DataFrame with smart initialization
    date_range::Vector{DateTime} = collect(date_start:interval:date_end)
    df_ts_full::DataFrame = DataFrame(date=date_range)
    
    for col_name in col_names
        init_value = df_init[1, col_name]
        if col_name in col_names_affected_selected
            # This column will be changed - fill with init for first row, missing for rest
            df_ts_full[!, col_name] = [init_value; fill(missing, length(date_range) - 1)]
        else
            # This column won't change - fill all rows with init value
            df_ts_full[!, col_name] = fill(init_value, length(date_range))
        end
    end

    # Inject values to output DataFrame
    for row in eachrow(df_ts_selected)
        id_col_str::String = string(row[id_col])  # Convert id to string (column name)

        # Find the row index where datetime matches exactly
        date_idx = findfirst(==(row.date), df_ts_full.date)

        # Update the value if both column and row exist
        if !isnothing(date_idx) && id_col_str in col_names
            df_ts_full[date_idx, id_col_str] = row.value
        end
    end

    # Forward fill missing values only for columns that were affected in selected period
    forward_fill!(df_ts_full; col_names=col_names_affected_selected)

    # Combine all affected columns
    col_names_affected = unique([col_names_affected_before; col_names_affected_selected])

    return df_ts_full, col_names_affected
end