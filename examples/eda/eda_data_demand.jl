using DataFrames
using PlotlyJS
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
traces1 = GenericTrace[]
for (id_dem, data) in sort(demand_dict_to_plot)
    trace = scatter(
        x=data.date,
        y=data.value,
        name="ID_DEM $id_dem",
        mode="lines",
        line=attr(width=2)
    )
    push!(traces1, trace)
end

layout1 = Layout(
    title="Original Demand Values by ID_DEM (Scenario $target_scenario)",
    xaxis_title="Date/Time",
    yaxis_title="Demand Value (MW)",
    hovermode="x unified",
    legend=attr(orientation="v", yanchor="top", y=1, xanchor="right", x=1)
)

p1 = plot(traces1, layout1)
display(p1)

# Plot 2: Normalized values for all id_dem groups
traces2 = GenericTrace[]
for (id_dem, data) in sort(demand_dict_to_plot)
    trace = scatter(
        x=data.date,
        y=data.normalized_value,
        name="ID_DEM $id_dem",
        mode="lines",
        line=attr(width=2)
    )
    push!(traces2, trace)
end

layout2 = Layout(
    title="Normalized Demand Values by ID_DEM (Scenario $target_scenario)",
    xaxis_title="Date/Time",
    yaxis_title="Normalized Value (0-1)",
    hovermode="x unified",
    legend=attr(orientation="v", yanchor="top", y=1, xanchor="right", x=1)
)

p2 = plot(traces2, layout2)
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

# # Save plots
# plots_dir = "examples/result/nem12/plots"
# mkpath(plots_dir)
# savefig(p1, joinpath(plots_dir, "demand_original_scenario_$target_scenario.png"))
# savefig(p2, joinpath(plots_dir, "demand_normalized_scenario_$target_scenario.png"))
