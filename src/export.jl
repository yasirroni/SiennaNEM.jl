"""
    export_optimization_results_to_csv(
        dfs_res::Dict{String, Dict{String, DataFrame}},
        output_dir::String;
        prefix::String=""
    )

Export accumulated optimization results to CSV files.

# Arguments
- `dfs_res`: Dictionary of results from `accumulate_optimization_results`
- `output_dir`: Directory path for CSV files (will be created if it doesn't exist)
- `prefix`: Optional prefix for filenames (default: "")

# Returns
- `Int`: Number of files exported

# Example
```julia
dfs_res = accumulate_optimization_results(results)
export_optimization_results_to_csv(
    dfs_res, 
    "examples/result/nem12/csv"; 
    prefix="uc_24h"
)
```
"""
function export_optimization_results_to_csv(
    dfs_res::Dict{String, Dict{String, DataFrame}},
    output_dir::String;
    prefix::String=""
)
    mkpath(output_dir)

    for (category, dfs) in dfs_res
        for (name, df) in dfs
            filename = isempty(prefix) ? 
                "$(category)_$(name).csv" : 
                "$(prefix)_$(category)_$(name).csv"
            filepath = joinpath(output_dir, filename)
            CSV.write(filepath, df)
        end
    end
    println("Successfully exported files to: $output_dir/*csv")
end
