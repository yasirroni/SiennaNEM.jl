using BenchmarkTools
using SiennaNEM
using DataFrames


"""
    bench_read(data_dir; samples=10, seconds=5)

Benchmark the read operation for a given data directory.
Returns a BenchmarkTools.BenchmarkGroup result.
"""
function bench_read(data_dir::AbstractString; samples=10, seconds=5)
    bm = @benchmarkable read_system_data($data_dir) samples=samples seconds=seconds
    return run(bm)
end

"""
    bench_create(data_dir; samples=10, seconds=5)

Benchmark the create_system! operation.
Automatically reads data once for setup.
"""
function bench_create(data_dir::AbstractString; samples=10, seconds=5)
    data = read_system_data(data_dir)  # read once
    bm = @benchmarkable create_system!($data) samples=samples seconds=seconds
    return run(bm)
end

"""
    bench_all(data_dirs::Dict)

Benchmark both read and create for all data directories.
Returns a Dict of Dicts with results.
"""
function bench_all(data_dirs::Dict; samples=10, seconds=5)
    results = Dict()
    for (fmt, dir) in data_dirs
        results[fmt] = Dict(
            "read" => bench_read(dir, samples=samples, seconds=seconds),
            "create" => bench_create(dir, samples=samples, seconds=seconds)
        )
    end
    return results
end
