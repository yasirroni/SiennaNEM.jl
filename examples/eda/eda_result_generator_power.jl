using DataFrames, OrderedCollections

## Generator Post Processing
# Create mapping from bus ID to component columns
# NOTE: currently, the Hydro is included as ThermalStandard
dfs_res["post"] = Dict{String, Any}()  # for storing post processing results
add_maps!(data)

timecol = :DateTime

if results isa PowerSimulations.SimulationProblemResults
    variable_key = "realized_variable"
    parameter_key = "realized_parameter"
elseif results isa PowerSimulations.OptimizationProblemResults
    variable_key = "variable"
    parameter_key = "parameter"
end

## Part 1: All Generators (df_gen_pg)
# Combine all generation data
df_gen_pg = vcat(
    dfs_res[variable_key]["ActivePowerVariable__ThermalStandard"],
    dfs_res[variable_key]["ActivePowerVariable__RenewableDispatch"],
    dfs_res[parameter_key]["ActivePowerTimeSeriesParameter__RenewableNonDispatch"],
)
dfs_res["post"]["gen_pg"] = df_gen_pg

# Create mapping from generator names to buses
gen_to_bus = data["map"]["gen_to_bus"]  # use id_gen
gen_n_to_bus = get_col_to_group(unique(df_gen_pg[:, :name]), gen_to_bus)  # use id_gen + id_unit

# Sum by bus
df_bus_gen_pg = sum_by_group(df_gen_pg, name_to_group=gen_n_to_bus)
dfs_res["post"]["bus_gen_pg"] = df_bus_gen_pg

# Sum by area
bus_to_area = data["map"]["bus_to_area"]
df_area_gen_pg = sum_by_group(df_bus_gen_pg, name_to_group=bus_to_area)
dfs_res["post"]["area_gen_pg"] = df_area_gen_pg

## Part 2: Thermal Standard Generators (df_tgen)
# Start with thermal generator active power
df_tgen = copy(dfs_res[variable_key]["ActivePowerVariable__ThermalStandard"])
rename!(df_tgen, :value => :active_power)

# Add unit commitment
df_gen_unit_commitment = dfs_res[variable_key]["OnVariable__ThermalStandard"]
df_tgen = leftjoin(
    df_tgen,
    rename(df_gen_unit_commitment, :value => :unit_commitment),
    on=[timecol, :name],
    order=:left,
)

# Add generator parameters (pmax and pfrmax)
gen_unit_to_pmax = data["map"]["gen_unit_to_pmax"] 
gen_unit_to_pfrmax = data["map"]["gen_unit_to_pfrmax"]
df_tgen.maximum_active_power = [get(gen_unit_to_pmax, name, 0.0) for name in df_tgen.name]
df_tgen.maximum_primary_frequency_response = [get(gen_unit_to_pfrmax, name, 0.0) for name in df_tgen.name]

# Calculate available capacity and PFR allocation
df_tgen.available_capacity = (df_tgen.maximum_active_power .* df_tgen.unit_commitment) .- df_tgen.active_power
df_tgen.pfr_limit = df_tgen.maximum_primary_frequency_response .* df_tgen.unit_commitment
df_tgen.primary_frequency_response = max.(min.(df_tgen.available_capacity, df_tgen.pfr_limit), 0.0)

# Store df_tgen
dfs_res["post"]["tgen"] = df_tgen

# Create df_tgen_pg by selecting columns from df_tgen
df_tgen_pg = select(df_tgen, timecol, :name, :primary_frequency_response => :value)

# Sum by bus
df_bus_tgen_pfr = sum_by_group(df_tgen_pg, name_to_group=gen_n_to_bus)
dfs_res["post"]["bus_tgen_pfr"] = df_bus_tgen_pfr

# Sum by Area
df_area_tgen_pfr = sum_by_group(df_bus_tgen_pfr, name_to_group=bus_to_area)
dfs_res["post"]["area_tgen_pfr"] = df_area_tgen_pfr

## Check violation for thermal generators
threshold = 1e-6
df_tgen.violation = (df_tgen.active_power .- df_tgen.maximum_active_power) .> threshold

if any(df_tgen.violation)
    println("Warning: Violations against pmax detected.")
    
    # Show which generators violated and when
    println(filter(row -> row.violation, df_tgen))
end
