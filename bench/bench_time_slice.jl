using BenchmarkTools
using DataFrames
using Dates
using OrderedCollections

function create_time_slices(
    df::DataFrame;
    initial_time::DateTime,
    horizon::Period,
    window_shift::Period,
)
    max_date = maximum(df.date)
    
    slices = OrderedDict{DateTime, DataFrame}()
    current_time = initial_time
    
    while current_time < max_date
        slice_end = current_time + horizon
        slices[current_time] = filter(row -> current_time <= row.date < slice_end, df)
        current_time += window_shift
    end
    
    return slices
end

function get_time_slices_iterator(
    df::DataFrame;
    initial_time::DateTime,
    horizon::Period,
    window_shift::Period,
)
    max_date = maximum(df.date)
    
    return Channel() do ch
        current_time = initial_time
        
        while current_time < max_date
            slice_end = current_time + horizon
            put!(ch, (current_time, filter(row -> current_time <= row.date < slice_end, df)))
            
            current_time += window_shift
        end
    end
end

# Setup benchmark parameters
horizon = Hour(24)
window_shift = Hour(24)
initial_time = minimum(data["demand_l_ts"][!, "date"])

println("=" ^ 80)
println("Benchmarking Time Slicing Methods")
println("=" ^ 80)

# Benchmark 1: Dict-based approach (full allocation)
println("\n1. Dict-based (full allocation):")
b1 = @benchmark begin
    demand_time_slices = create_time_slices(
        $data["demand_l_ts"],
        initial_time = $initial_time,
        horizon = $horizon,
        window_shift = $window_shift,
    )
    generator_time_slices = create_time_slices(
        $data["generator_pmax_ts"],
        initial_time = $initial_time,
        horizon = $horizon,
        window_shift = $window_shift,
    )
    for time_slice in keys(demand_time_slices)
        df_demand_ts = demand_time_slices[time_slice]
        df_generator_ts = generator_time_slices[time_slice]
    end
end samples=10 evals=1

display(b1)
println("\nMemory allocated: ", BenchmarkTools.prettymemory(b1.memory))
println("Median time: ", BenchmarkTools.prettytime(median(b1).time))

println("\n2. Iterator-based (lazy evaluation):")
b2 = @benchmark begin
    demand_gen = get_time_slices_iterator(
        $data["demand_l_ts"],
        initial_time = $initial_time,
        horizon = $horizon,
        window_shift = $window_shift,
    )
    generator_gen = get_time_slices_iterator(
        $data["generator_pmax_ts"],
        initial_time = $initial_time,
        horizon = $horizon,
        window_shift = $window_shift,
    )
    for pair in zip(demand_gen, generator_gen)
        time_slice, df_demand_ts = pair[1]
        _, df_generator_ts = pair[2]
    end
end samples=10 evals=1

display(b2)
println("\nMemory allocated: ", BenchmarkTools.prettymemory(b2.memory))
println("Median time: ", BenchmarkTools.prettytime(median(b2).time))

# Comparison summary
println("\n" * "=" ^ 80)
println("COMPARISON SUMMARY")
println("=" ^ 80)
println("Memory savings: ", round((1 - b2.memory / b1.memory) * 100, digits=2), "%")
println("Time difference: ", round((median(b2).time / median(b1).time), digits=2), "x")
if median(b2).time < median(b1).time
    println("Winner: Iterator-based (faster)")
else
    println("Winner: Dict-based (faster)")
end
println("=" ^ 80)

# 1. Dict-based (full allocation):
# BenchmarkTools.Trial: 1 sample with 1 evaluation per sample.
#  Single result which took 5.361 s (4.61% GC) to evaluate,
#  with a memory estimate of 5.27 GiB, over 351958942 allocations.

# Memory allocated: 5.27 GiB
# Median time: 5.361 s

# 2. Iterator-based (lazy evaluation):
# BenchmarkTools.Trial: 10 samples with 1 evaluation per sample.
#  Range (min … max):  9.108 ms …   9.902 ms  ┊ GC (min … max): 0.00% … 0.00%
#  Time  (median):     9.522 ms               ┊ GC (median):    0.00%
#  Time  (mean ± σ):   9.492 ms ± 262.436 μs  ┊ GC (mean ± σ):  0.00% ± 0.00%

#   █ █          █      █     █       █  █  █         █       █  
#   █▁█▁▁▁▁▁▁▁▁▁▁█▁▁▁▁▁▁█▁▁▁▁▁█▁▁▁▁▁▁▁█▁▁█▁▁█▁▁▁▁▁▁▁▁▁█▁▁▁▁▁▁▁█ ▁
#   9.11 ms         Histogram: frequency by time         9.9 ms <

#  Memory estimate: 10.65 MiB, allocs estimate: 610350.

# Memory allocated: 10.65 MiB
# Median time: 9.522 ms
