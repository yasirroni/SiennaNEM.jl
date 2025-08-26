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

system_data_dir = "data/nem12"
ts_data_dir = joinpath(system_data_dir, "schedule-1w")
generator_n_sched_path = joinpath(ts_data_dir, "Generator_n_sched.csv")
generator_path = joinpath(system_data_dir, "Generator.csv")

df_gen = CSV.read(generator_path, DataFrame)
df_ts = CSV.read(generator_n_sched_path, DataFrame)
preprocess_date!(df_ts)
sort!(df_ts, :date)

# initial n data
df_gen[:, [:id, :n]]
gen_ids = string.(df_gen.id)
gen_ns = df_gen.n
df_init = DataFrame(Dict(zip(gen_ids, gen_ns)))

date_start = DateTime(2024, 1, 1)
date_end = DateTime(2025, 1, 1)
scenario = 1

# NOTE: I haven't test the select last before selected
df_ts_before_selected = filter(
    row -> row.date < date_start
        && row.scenario == scenario,
    df_ts
)
df_ts_before_selected = combine(groupby(df_ts_before_selected, :gen_id)) do group
    group[end, :]
end
df_ts_selected = filter(
    row -> row.date >= date_start
        && row.date <= date_end
        && row.scenario == scenario,
    df_ts
)
println(df_ts_before_selected)
println(df_ts_selected)

# update df_init with df_ts_before_selected
gen_ids_update = string.(df_ts_before_selected.gen_id)
gen_values_update = df_ts_before_selected.value
df_init[1, gen_ids_update] .= gen_values_update

date_range = collect(date_start:Hour(1):date_end)
df_ts_out = DataFrame(date=date_range)
for col_name in names(df_init)
    df_ts_out[!, col_name] = [df_init[1, col_name]; fill(missing, length(date_range) - 1)]
end

# inject df_ts_selected values into specific row/column locations
for row in eachrow(df_ts_selected)
    gen_id_col = string(row.gen_id)  # Convert gen_id to string (column name)
    target_datetime = row.date       # Use full DateTime, not just Date
    
    # Find the row index where datetime matches exactly
    date_idx = findfirst(==(target_datetime), df_ts_out.date)
    
    # Update the value if both column and row exist
    if !isnothing(date_idx) && gen_id_col in names(df_ts_out)
        df_ts_out[date_idx, gen_id_col] = row.value
    end
end

# forward fill
forward_fill!(df_ts_out)

# Calculate the datetime range for display
target_datetime = DateTime("2024-02-01T00:00:00")
window_hours = 48

start_datetime_show = target_datetime - Hour(window_hours)
end_datetime_show = target_datetime + Hour(window_hours)

date_filter_show = (df_ts_out.date .>= start_datetime_show) .& (df_ts_out.date .<= end_datetime_show)
columns_to_check = ["date", "1", "84", "69"]
show(df_ts_out[date_filter_show, columns_to_check], allrows=true)
