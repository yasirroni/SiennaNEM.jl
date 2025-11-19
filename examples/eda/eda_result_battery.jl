using DataFrames, OrderedCollections


# Create mapping from bus ID to component columns
timecol = :DateTime
df_datetime = DataFrame(
    timecol => dfs_res["variable"]["EnergyVariable__EnergyReservoirStorage"][!, timecol]
)

dfs_res["variable"]["ActivePowerInVariable__EnergyReservoirStorage"]
dfs_res["variable"]["ActivePowerOutVariable__EnergyReservoirStorage"]

# There must be no shortage and surplus energy to ensure the problem feasible
row_sums_shortage = sum.(eachrow(select(dfs_res["variable"]["StorageEnergyShortageVariable__EnergyReservoirStorage"])))
row_sums_surplus = sum.(eachrow(select(dfs_res["variable"]["StorageEnergySurplusVariable__EnergyReservoirStorage"])))

df_ess_e = dfs_res["variable"]["EnergyVariable__EnergyReservoirStorage"]
data_cols = get_component_columns(df_ess_e; timecol=timecol)
ess_to_bus = get_map_from_df(data["storage"], :id_ess, :id_bus)  # use id_gen
ees_col_to_bus = get_col_to_group(data_cols, ess_to_bus)  # use id_ess + id_unit
bus_to_ess_col = get_group_to_col(col_to_bus)  # map bus to columns

# Sum columns for each bus
df_bus_pg = sum_by_group(df_gen_pg, bus_to_gen_col)
df_bus_pg = hcat(df_datetime, df_bus_pg)
dfs_res["post"] = Dict{String, Any}()
dfs_res["post"]["bus_pg"] = df_bus_pg
