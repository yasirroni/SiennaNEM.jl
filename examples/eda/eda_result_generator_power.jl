using DataFrames, OrderedCollections


## Generator Output Power by Bus
# Create mapping from bus ID to component columns
# NOTE: currently, the Hydro is included as ThermalStandard
timecol = :DateTime
df_gen_pg_list = [
    dfs_res["variable"]["ActivePowerVariable__ThermalStandard"],
    dfs_res["variable"]["ActivePowerVariable__RenewableDispatch"],
    dfs_res["parameter"]["ActivePowerTimeSeriesParameter__RenewableNonDispatch"],
]

df_gen_pg =  hcat(
    df_gen_pg_list[1],
    [select(df, Not(timecol)) for df in df_gen_pg_list[2:end]]...
)
data_cols = get_component_columns(df_gen_pg; timecol=timecol)
gen_to_bus = get_gen_to_bus(data["generator"])  # use gen_id
col_to_bus = get_col_to_bus(data_cols, gen_to_bus)  # use gen_id + unit_id
bus_to_col = get_bus_to_col(col_to_bus)  # map bus to columns

# Sum columns for each bus
df_res_bus_pg = sum_by_bus(df_gen_pg, bus_to_col)
df_res_bus_pg = hcat(DataFrame(timecol => df_gen_pg[!, timecol]), df_res_bus_pg)
dfs_res["post"] = Dict{String, Any}()
dfs_res["post"]["bus_pg"] = df_res_bus_pg

## Generator Primary Frequency Response by Bus
df_gen_uc = dfs_res["variable"]["OnVariable__ThermalStandard"]
df_gen_pg_thermal = dfs_res["variable"]["ActivePowerVariable__ThermalStandard"]

gen_to_pmax = Dict(row.id_gen => row.pmax for row in eachrow(data["generator"]))
gen_to_pfrmax = Dict(row.id_gen => row.pfrmax for row in eachrow(data["generator"]))

gen_to_unit = OrderedDict{Int64, Vector{Int64}}()
for col in get_component_columns(df_gen_pg_thermal; timecol=timecol)
    parts = split(col, "_")
    gen_id = parse(Int, parts[1])
    unit_id = parse(Int, parts[2])
    
    if !haskey(gen_to_unit, gen_id)
        gen_to_unit[gen_id] = Int64[]
    end
    push!(gen_to_unit[gen_id], unit_id)
end

# Update data["generator"] with extended version
data["generator_extended"] = extend_generator_data(data["generator"])
gen_unit_to_pmax = Dict(
    row.gen_unit_id => row.pmax 
    for row in eachrow(data["generator_extended"])
)
gen_unit_to_pfrmax = Dict(
    row.gen_unit_id => row.pfrmax 
    for row in eachrow(data["generator_extended"])
)

# Check for violations against pmax
thermal_cols = get_component_columns(df_gen_pg_thermal; timecol=timecol)
pmax_vector = [gen_unit_to_pmax[col] for col in thermal_cols if haskey(gen_unit_to_pmax, col)]
power_matrix = Matrix(df_gen_pg_thermal[!, thermal_cols])
threshold = 1e-6
violation_mask = (power_matrix .- pmax_vector') .> threshold
any(violation_mask)  # check if any violations exist

# Calculate PFR allocation
uc_matrix = Matrix(df_gen_uc[!, thermal_cols])
pfrmax_vector = [gen_unit_to_pfrmax[col] for col in thermal_cols if haskey(gen_unit_to_pfrmax, col)]
available_capacity = (pmax_vector' .* uc_matrix) .- power_matrix
pfr_limit = pfrmax_vector' .* uc_matrix
pfr_allocation = max.(min.(available_capacity, pfr_limit), 0.0)

# Create PFR allocation per bus
df_gen_pfr = hcat(
    DataFrame(timecol => df_gen_pg_thermal[!, timecol]),
    DataFrame(pfr_allocation, thermal_cols),
)
df_res_bus_pfr = sum_by_bus(df_gen_pfr, bus_to_col)
df_res_bus_pfr = hcat(DataFrame(timecol => df_gen_pfr[!, timecol]), df_res_bus_pfr)
dfs_res["post"]["bus_pfr"] = df_res_bus_pfr
