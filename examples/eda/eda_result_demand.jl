using DataFrames, OrderedCollections

## Demand Post Processing
timecol = :DateTime
if !haskey(dfs_res, "post")
    dfs_res["post"] = Dict{String, Any}()
end
if !haskey(dfs_res, "map")
    add_maps!(data)
end

if results isa PowerSimulations.SimulationProblemResults
    dfs_res_ = dfs_res["realized"]
elseif results isa PowerSimulations.OptimizationProblemResults
    dfs_res_ = dfs_res
end

## Part 1: All Demand (df_dem_pd)
# Get demand data
df_dem_pd = dfs_res_["parameter"]["ActivePowerTimeSeriesParameter__PowerLoad"]

# Store individual demand data
dfs_res["post"]["dem_pd"] = df_dem_pd

# Create mapping from demand names to buses
dem_to_bus = get_map_from_df(data["demand"], :id_dem, :id_bus)  # use id_dem
dem_n_to_bus = get_col_to_group(unique(df_dem_pd[:, :name]), dem_to_bus)  # use id_dem + id_unit

# Sum by bus
df_bus_dem_pd = sum_by_group(df_dem_pd, name_to_group=dem_n_to_bus)
dfs_res["post"]["bus_dem_pd"] = df_bus_dem_pd

# Sum by area
bus_to_area = data["map"]["bus_to_area"]
df_area_dem_pd = sum_by_group(df_bus_dem_pd, name_to_group=bus_to_area)
dfs_res["post"]["area_dem_pd"] = df_area_dem_pd
