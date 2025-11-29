"""
    get_problem_results(res::OptimizationProblemResults)

Extract optimization results from a single OptimizationProblemResults into organized DataFrames.

Uses `optimization_result_handlers` constant to automatically extract all result categories
(expressions, variables, parameters, aux_variables, duals).

# Arguments
- `res::OptimizationProblemResults`: Single optimization problem results.

# Returns
- `Dict{String, Dict{String, DataFrame}}`: Nested dictionary where:
  - First level keys are result categories (expression, variable, parameter, aux_variable, dual)
  - Second level keys are specific result names
  - Values are DataFrames for each result
"""
function get_problem_results(res::OptimizationProblemResults)
    # Initialize results dictionary from handlers
    dfs_res = Dict(
        category => Dict{String, DataFrame}() 
        for (category, _, _) in optimization_result_handlers
    )
    
    # Extract all results
    for (category, list_fn, read_fn) in optimization_result_handlers
        for item in list_fn(res)
            df = sort_res_cols(read_fn(res, item))
            dfs_res[category][item] = df
        end
    end
    
    return dfs_res
end

"""
    get_problem_results(res_dict::Dict{DateTime, OptimizationProblemResults})

Extract and accumulate optimization results from multiple time slices into organized DataFrames.

Uses `optimization_result_handlers` constant to automatically extract all result categories
(expressions, variables, parameters, aux_variables, duals) from each time slice and concatenate them.

# Arguments
- `res_dict::Dict{DateTime, OptimizationProblemResults}`: Dictionary mapping time slices
  to their optimization results.

# Returns
- `Dict{String, Dict{String, DataFrame}}`: Nested dictionary where:
  - First level keys are result categories (expression, variable, parameter, aux_variable, dual)
  - Second level keys are specific result names
  - Values are concatenated DataFrames from all time slices
"""
function get_problem_results(res_dict::Dict{DateTime, OptimizationProblemResults})
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

# Accumulate all results from the loop
dfs_res = get_problem_results(res_dict)

# Export to CSV
export_optimization_results_to_csv(
    dfs_res, 
    "examples/result/nem12/csv"; 
    prefix=schedule_name
)

