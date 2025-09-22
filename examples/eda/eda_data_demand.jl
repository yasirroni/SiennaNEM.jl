using DataFrames
using Plots
using Statistics

target_scenario = 1
df_demand_l_ts = data["demand_l_ts"]

df_demand_l_ts_s = filter(
    row -> row.scenario == target_scenario,
    df_demand_l_ts
)

# Prepare data
demand_dict_to_plot = Dict()
for group in groupby(df_demand_l_ts_s, :id_dem)
    id_dem = first(group.id_dem)

    min_val = minimum(group.value)
    max_val = maximum(group.value)

    if max_val != min_val
        normalized_values = (group.value .- min_val) ./ (max_val - min_val)
    else
        normalized_values = fill(1.0, length(group.value))
    end

    demand_dict_to_plot[id_dem] = DataFrame(
        date = group.date,
        value = group.value,
        normalized_value = normalized_values,
    )

    println("ID_DEM $id_dem: $(nrow(group)) data points")
end

# Plot 1: Original values for all id_dem groups
p1 = plot(title="Original Demand Values by ID_DEM (Scenario $target_scenario)", 
          xlabel="Date/Time", ylabel="Demand Value", 
          legend=:topright, size=(800, 600))
for (id_dem, data) in demand_dict_to_plot
    plot!(p1, data.date, data.value, label="ID_DEM $id_dem", linewidth=2)
end
display(p1)

# Plot 2: Normalized values for all id_dem groups
p2 = plot(title="Normalized Demand Values by ID_DEM (Scenario $target_scenario)", 
          xlabel="Date/Time", ylabel="Normalized Value (0-1)", 
          legend=:topright, size=(800, 600))

for (id_dem, data) in demand_dict_to_plot
    plot!(p2, data.date, data.normalized_value, label="ID_DEM $id_dem", linewidth=2)
end

display(p2)

# println("\nSummary Statistics:")
# println("="^50)
# for (id_dem, data) in sort(demand_dict_to_plot)
#     println("ID_DEM $id_dem:")
#     println("  Original - Mean: $(round(mean(data.value), digits=2)), Std: $(round(std(data.value), digits=2))")
#     println("  Original - Min: $(round(minimum(data.value), digits=2)), Max: $(round(maximum(data.value), digits=2))")
#     println("  Normalized - Mean: $(round(mean(data.normalized_value), digits=3)), Std: $(round(std(data.normalized_value), digits=3))")
#     println()
# end

# savefig(p1, "demand_original_scenario_$target_scenario.png")
# savefig(p2, "demand_normalized_scenario_$target_scenario.png")
