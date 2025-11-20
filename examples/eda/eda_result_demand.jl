using DataFrames, OrderedCollections


# Create mapping from bus ID to component columns
# Create post-processing dictionary if it doesn't exist
if !haskey(dfs_res, "post")
    dfs_res["post"] = Dict{String, Any}()
end
timecol = :DateTime
df_datetime = DataFrame(
    timecol => dfs_res["variable"]["ActivePowerTimeSeriesParameter__PowerLoad"][!, timecol]
)

## Create Aggregated Energy Storage Energy
# Get df
df_dem_pd = dfs_res["variable"]["ActivePowerTimeSeriesParameter__PowerLoad"]

# Get map
data_cols = get_component_columns(df_dem_pd; timecol=timecol)
dem_to_bus = get_map_from_df(data["storage"], :id_ess, :id_bus)  # use id_gen
ees_col_to_bus = get_col_to_group(data_cols, dem_to_bus)  # use id_ess + id_unit
bus_to_dem_col = get_group_to_col(ees_col_to_bus)  # map bus to columns

# Sum columns for each bus
df_bus_dem_e = sum_by_group(df_dem_e, bus_to_dem_col, df_datetime)
df_bus_dem_ch = sum_by_group(df_dem_ch, bus_to_dem_col, df_datetime)
df_bus_dem_dch = sum_by_group(df_dem_dch, bus_to_dem_col, df_datetime)
df_bus_dem_chdch = diff_df(df_bus_dem_ch, df_bus_dem_dch; timecol=:DateTime)
dfs_res["post"]["bus_dem_e"] = df_bus_dem_e
dfs_res["post"]["bus_dem_ch"] = df_bus_dem_ch
dfs_res["post"]["bus_dem_dch"] = df_bus_dem_dch
dfs_res["post"]["bus_dem_chdch"] = df_bus_dem_chdch

# Sum columns for each area
area_to_bus = data["map"]["area_to_bus"]
df_area_dem_e = sum_by_group(df_bus_dem_e, area_to_bus, df_datetime)
df_area_dem_ch = sum_by_group(df_bus_dem_ch, area_to_bus, df_datetime)
df_area_dem_dch = sum_by_group(df_bus_dem_dch, area_to_bus, df_datetime)
df_area_dem_chdch = sum_by_group(df_bus_dem_chdch, area_to_bus, df_datetime)
dfs_res["post"]["area_dem_e"] = df_area_dem_e
dfs_res["post"]["area_dem_ch"] = df_area_dem_ch
dfs_res["post"]["area_dem_dch"] = df_area_dem_dch
dfs_res["post"]["area_dem_chdch"] = df_area_dem_chdch
