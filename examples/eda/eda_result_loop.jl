"""
    accumulate_optimization_results(res_dict::Dict{DateTime, OptimizationProblemResults})

Accumulate optimization results from multiple time slices into organized DataFrames.

Uses `optimization_result_handlers` constant to automatically extract all result categories
(expressions, variables, parameters, aux_variables, duals) from each time slice.

# Arguments
- `res_dict::Dict{DateTime, OptimizationProblemResults}`: Dictionary mapping time slices
  to their optimization results.

# Returns
- `Dict{String, Dict{String, DataFrame}}`: Nested dictionary where:
  - First level keys are result categories (expression, variable, parameter, aux_variable, dual)
  - Second level keys are specific result names
  - Values are concatenated DataFrames from all time slices
"""
function accumulate_optimization_results(res_dict::Dict{DateTime, OptimizationProblemResults})
    # Initialize results dictionary from handlers
    dfs_res = Dict(
        category => Dict{String, DataFrame}() 
        for (category, _, _) in optimization_result_handlers
    )
    
    # Accumulate all results
    for (time_slice, res) in res_dict
        for (category, list_fn, read_fn) in optimization_result_handlers
            for item in list_fn(res)
                df = sort_res_cols(read_fn(res, item))
                
                # Append or initialize DataFrame
                if haskey(dfs_res[category], item)
                    dfs_res[category][item] = vcat(dfs_res[category][item], df)
                else
                    dfs_res[category][item] = df
                end
            end
        end
    end
    
    return dfs_res
end

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

using CSV

# Accumulate all results from the loop
dfs_res = accumulate_optimization_results(res_dict)

# Export to CSV
export_optimization_results_to_csv(
    dfs_res, 
    "examples/result/nem12/csv"; 
    prefix=schedule_name
)

