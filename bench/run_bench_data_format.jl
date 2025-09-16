using SiennaNEM
using DataFrames


include("bench_data_format.jl")

# Data directories
const DATA_DIRS = Dict(
    "csv" => "data/nem12",
    "arrow" => "data/nem12-arrow"
)

println("Running data format benchmarks...")
results = bench_all(DATA_DIRS; samples=10, seconds=5)

# Prepare a summary table
summary = DataFrame(
    Format=String[],
    operation=String[],
    median_ms=Float64[]
)

for (fmt, group) in results
    for op in ["read", "create"]
        time_ms = median(group[op]).time / 1_000_000
        push!(summary, (fmt, op, round(time_ms, digits=2)))
    end
    # Add total
    total_ms = (median(group["read"]).time + median(group["create"]).time) / 1_000_000
    push!(summary, (fmt, "total", round(total_ms, digits=2)))
end

println(summary)
