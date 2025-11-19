using DataFrames, OrderedCollections


## Generator Output Power by Bus
# Create mapping from bus ID to component columns
# NOTE: currently, the Hydro is included as ThermalStandard
dfs_res["post"] = Dict{String, Any}()  # for storing post processing results
add_maps!(data)

timecol = :DateTime
df_datetime = DataFrame(
    timecol => dfs_res["variable"]["ActivePowerVariable__ThermalStandard"][!, timecol]
)

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
gen_to_bus = data["map"]["gen_to_bus"]  # use id_gen
gen_col_to_bus = get_col_to_group(data_cols, gen_to_bus)  # use id_gen + id_unit
bus_to_gen_col = get_group_to_col(gen_col_to_bus)  # map bus to columns

# Sum columns for each bus
df_bus_pg = sum_by_group(df_gen_pg, bus_to_gen_col, df_datetime)
dfs_res["post"]["bus_pg"] = df_bus_pg

## Generator Primary Frequency Response by Bus
df_gen_uc = dfs_res["variable"]["OnVariable__ThermalStandard"]
df_gen_pg_thermal = dfs_res["variable"]["ActivePowerVariable__ThermalStandard"]

# Update data["generator"] with extended version
gen_unit_to_pmax = data["map"]["gen_unit_to_pmax"] 
gen_unit_to_pfrmax = data["map"]["gen_unit_to_pfrmax"]

# Calculate PFR allocation
thermal_cols = get_component_columns(df_gen_pg_thermal; timecol=timecol)
power_matrix = Matrix(df_gen_pg_thermal[!, thermal_cols])
uc_matrix = Matrix(df_gen_uc[!, thermal_cols])
pmax_vector = [gen_unit_to_pmax[col] for col in thermal_cols if haskey(gen_unit_to_pmax, col)]
pfrmax_vector = [gen_unit_to_pfrmax[col] for col in thermal_cols if haskey(gen_unit_to_pfrmax, col)]
available_capacity = (pmax_vector' .* uc_matrix) .- power_matrix
pfr_limit = pfrmax_vector' .* uc_matrix
pfr_allocation = max.(min.(available_capacity, pfr_limit), 0.0)
df_gen_pfr = hcat(df_datetime, DataFrame(pfr_allocation, thermal_cols))

# Create PFR allocation per bus
df_bus_pfr = sum_by_group(df_gen_pfr, bus_to_gen_col, df_datetime)
dfs_res["post"]["bus_pfr"] = df_bus_pfr

## Generator Output Power and PFR by Area
area_to_bus = data["map"]["area_to_bus"]

# Area-wise Generation
df_area_pg = sum_by_group(df_bus_pg, area_to_bus, df_datetime)
dfs_res["post"]["area_pg"] = df_area_pg

# Area-wise PFR Allocation
df_area_pfr = sum_by_group(df_bus_pfr, area_to_bus, df_datetime)
dfs_res["post"]["area_pfr"] = df_area_pfr

## Check violation
# Check for violations against pmax
threshold = 1e-6
violation_mask = (power_matrix .- pmax_vector') .> threshold
if any(violation_mask)  # check if any violations exist
    println("Warning: Violations against pmax detected.")
end
