# Accumulate all results from the loop
dfs_res = SiennaNEM.get_problem_results(res)

# Export to CSV
SiennaNEM.export_optimization_results_to_csv(
    dfs_res, 
    "examples/result/nem12/csv/$(schedule_name)/scenario-$(scenario)"; 
    prefix="$(schedule_name)_scenario-$(scenario)"
)
