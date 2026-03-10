using DataFrames, CSV

"""
This script require `get_data`:

data = SiennaNEM.get_data(system_data_dir, ts_data_dir)
"""

cols = [:tech, :type, :DataType, :PrimeMovers, :ThermalFuels]
df_bus = data["bus"]
show(df_bus[:, [:id_bus, :name, :latitude, :longitude]], allrows=true)

function fill_latlong_from_bus!(df::DataFrame, df_bus::DataFrame)
    # Create a lookup dict from bus id to lat/long
    bus_latlong = Dict(row.id_bus => (row.latitude, row.longitude) for row in eachrow(df_bus))

    # Copy Arrow read-only columns to mutable arrays
    df.latitude  = copy(df.latitude)
    df.longitude = copy(df.longitude)

    for row in eachrow(df)
        if row.latitude == 0 || row.longitude == 0
            if haskey(bus_latlong, row.id_bus)
                lat, lon = bus_latlong[row.id_bus]
                row.latitude  = row.latitude  == 0 ? lat : row.latitude
                row.longitude = row.longitude == 0 ? lon : row.longitude
            end
        end
    end
end

df_generator = data["generator"]
show(df_generator[:, [:id_gen, :name, :id_bus, :latitude, :longitude]], allrows=true)

fill_latlong_from_bus!(df_generator, df_bus)
show(df_generator[:, [:id_gen, :name, :id_bus, :latitude, :longitude]], allrows=true)
show(df_generator[:, [:id_bus, :latitude, :longitude]], allrows=true)
show(combine(groupby(df_generator[:, [:id_bus, :latitude, :longitude]], [:id_bus, :latitude, :longitude]), nrow => :count), allrows=true)

df_storage = data["storage"]
show(df_storage[:, [:id_ess, :name, :id_bus, :latitude, :longitude]], allrows=true)

fill_latlong_from_bus!(df_storage, df_bus)
show(df_storage[:, [:id_ess, :name, :id_bus, :latitude, :longitude]], allrows=true)
show(df_storage[:, [:id_bus, :latitude, :longitude]], allrows=true)
show(combine(groupby(df_storage[:, [:id_bus, :latitude, :longitude]], [:id_bus, :latitude, :longitude]), nrow => :count), allrows=true)

precision = 1
df_all_latlong = vcat(
    df_generator[:, [:id_bus, :latitude, :longitude]],
    df_storage[:, [:id_bus, :latitude, :longitude]],
    df_bus[:, [:id_bus, :latitude, :longitude]],
)
df_bus_latlong_range = combine(groupby(df_all_latlong, :id_bus),
    :latitude  => (x -> floor(minimum(x), digits=precision)) => :latitude_min,
    :latitude  => (x -> ceil(maximum(x),  digits=precision)) => :latitude_max,
    :longitude => (x -> floor(minimum(x), digits=precision)) => :longitude_min,
    :longitude => (x -> ceil(maximum(x),  digits=precision)) => :longitude_max,
)
show(df_bus_latlong_range, allrows=true)
# 12×5 DataFrame
#  Row │ id_bus  latitude_min  latitude_max  longitude_min  longitude_max 
#      │ Int64?  Float64       Float64       Float64        Float64       
# ─────┼──────────────────────────────────────────────────────────────────
#    1 │      1         -19.4         -16.8          144.1          146.9
#    2 │      2         -23.6         -22.8          145.3          149.5
#    3 │      3         -23.9         -23.8          151.2          151.3
#    4 │      4         -27.7         -26.2          149.7          153.1
#    5 │      5         -30.6         -30.5          151.6          151.7
#    6 │      6         -34.8         -33.4          150.1          150.5
#    7 │      7         -34.6         -32.7          150.8          151.5
#    8 │      8         -36.4         -35.1          147.0          148.5
#    9 │      9         -38.3         -35.7          143.7          148.2
#   10 │     10         -42.9         -41.1          145.1          147.4
#   11 │     11         -35.8         -33.0          135.8          143.8
#   12 │     12         -37.7         -37.6          140.4          140.9

leftjoin!(df_bus, df_bus_latlong_range, on=:id_bus)
show(df_bus[:, [:id_bus, :name, :latitude, :longitude, :latitude_min, :latitude_max, :longitude_min, :longitude_max]], allrows=true)
cols = [:id_bus, :name, :latitude, :longitude, :latitude_min, :latitude_max, :longitude_min, :longitude_max]
CSV.write("examples/result/eda/df_bus_latlong.csv", df_bus[:, cols])
