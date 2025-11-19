using DataFrames, OrderedCollections


# Create mapping from bus ID to component columns
# Create post-processing dictionary if it doesn't exist
if !haskey(dfs_res, "post")
    dfs_res["post"] = Dict{String, Any}()
end
timecol = :DateTime
df_datetime = DataFrame(
    timecol => dfs_res["variable"]["EnergyVariable__EnergyReservoirStorage"][!, timecol]
)

# There must be no shortage and surplus energy to ensure the problem feasible
row_sums_shortage = sum.(eachrow(select(dfs_res["variable"]["StorageEnergyShortageVariable__EnergyReservoirStorage"])))
row_sums_surplus = sum.(eachrow(select(dfs_res["variable"]["StorageEnergySurplusVariable__EnergyReservoirStorage"])))

## Create Aggregated Energy Storage Energy
# Get df
df_ess_e = dfs_res["variable"]["EnergyVariable__EnergyReservoirStorage"]
df_ess_ch = dfs_res["variable"]["ActivePowerInVariable__EnergyReservoirStorage"]
df_ess_dch = dfs_res["variable"]["ActivePowerOutVariable__EnergyReservoirStorage"]

# Get map
data_cols = get_component_columns(df_ess_e; timecol=timecol)
ess_to_bus = get_map_from_df(data["storage"], :id_ess, :id_bus)  # use id_gen
ees_col_to_bus = get_col_to_group(data_cols, ess_to_bus)  # use id_ess + id_unit
bus_to_ess_col = get_group_to_col(ees_col_to_bus)  # map bus to columns

# Sum columns for each bus
df_bus_ess_e = sum_by_group(df_ess_e, bus_to_ess_col, df_datetime)
df_bus_ess_ch = sum_by_group(df_ess_ch, bus_to_ess_col, df_datetime)
df_bus_ess_dch = sum_by_group(df_ess_dch, bus_to_ess_col, df_datetime)
df_bus_ess_chdch = diff_df(df_bus_ess_ch, df_bus_ess_dch; timecol=:DateTime)
dfs_res["post"]["bus_ess_e"] = df_bus_ess_e
dfs_res["post"]["bus_ess_ch"] = df_bus_ess_ch
dfs_res["post"]["bus_ess_dch"] = df_bus_ess_dch
dfs_res["post"]["bus_ess_chdch"] = df_bus_ess_chdch

# Sum columns for each area
area_to_bus = data["map"]["area_to_bus"]
df_area_ess_e = sum_by_group(df_bus_ess_e, area_to_bus, df_datetime)
df_area_ess_ch = sum_by_group(df_bus_ess_ch, area_to_bus, df_datetime)
df_area_ess_dch = sum_by_group(df_bus_ess_dch, area_to_bus, df_datetime)
df_area_ess_chdch = sum_by_group(df_bus_ess_chdch, area_to_bus, df_datetime)
dfs_res["post"]["area_ess_e"] = df_area_ess_e
dfs_res["post"]["area_ess_ch"] = df_area_ess_ch
dfs_res["post"]["area_ess_dch"] = df_area_ess_dch
dfs_res["post"]["area_ess_chdch"] = df_area_ess_chdch
