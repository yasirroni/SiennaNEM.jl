using DataFrames
using Arrow
using CSV

system_data_dir = "data/nem12/arrow"
# system_data_dir = "data/nem12/csv"
data_dir = system_data_dir

files = Dict(
    "bus" => "Bus",
    "demand" => "Demand",
    "der" => "DER",
    "storage" => "ESS",
    "generator" => "Generator",
    "line" => "Line",
)

data = Dict{String,Any}()
for (k, fname) in files
    path = joinpath(data_dir, fname)
    arrow_path = path * ".arrow"
    csv_path = path * ".csv"

    println("Checking: $k")
    println("  Arrow path: $arrow_path, exists: $(isfile(arrow_path))")
    println("  CSV path: $csv_path, exists: $(isfile(csv_path))")

    if isfile(arrow_path)
        df = DataFrame(Arrow.Table(arrow_path))
        println("  Loaded from Arrow, rows: $(nrow(df))")
    elseif isfile(csv_path)
        df = DataFrame(CSV.File(csv_path))
        println("  Loaded from CSV, rows: $(nrow(df))")
    else
        error("File not found for $k at $path")
    end
    data[k] = df
end

data["generator"]
