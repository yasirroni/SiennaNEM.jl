using PlotlyJS  # used for savefig


# Create bus ID to name mapping
bus_to_name = get_map_from_df(data["bus"], :id_bus, :name)
# area_to_name = get_map_from_df(data["area"], :id_area, :name)  # already defined in SiennaNEM/const.jl

# Create output directory for plots
plots_dir = "examples/result/nem12/plots"
mkpath(plots_dir)

# Plot generation by bus
p_pg = plot_stacked(
    dfs_res["post"]["bus_gen_pg"],
    bus_to_name;
    timecol=:DateTime,
    title="Generation by Bus",
    yaxis_title="Power (MW)",
)
savefig(p_pg, joinpath(plots_dir, "bus_gen_pg.png"))

# Plot PFR allocation by bus
p_pfr = plot_stacked(
    dfs_res["post"]["bus_gen_pfr"],
    bus_to_name;
    timecol=:DateTime,
    title="Primary Frequency Response Reserve by Bus",
    yaxis_title="Reserve Capacity (MW)",
)
savefig(p_pfr, joinpath(plots_dir, "bus_gen_pfr.png"))

# Plot generation by area
p_area_gen_pg = plot_stacked(
    dfs_res["post"]["area_gen_pg"],
    area_to_name;
    timecol=:DateTime,
    title="Generation by Area",
    yaxis_title="Power (MW)",
)
savefig(p_area_gen_pg, joinpath(plots_dir, "area_gen_pg.png"))

# Plot PFR allocation by area
p_area_gen_pfr = plot_stacked(
    dfs_res["post"]["area_gen_pfr"],
    area_to_name;
    timecol=:DateTime,
    title="Primary Frequency Response Reserve by Area", 
    yaxis_title="Reserve Capacity (MW)",
)
savefig(p_area_gen_pfr, joinpath(plots_dir, "area_gen_pfr.png"))

# Plot battery energy by bus
p_bus_ess_e = plot_stacked(
    dfs_res["post"]["bus_ess_e"],
    bus_to_name;
    timecol=:DateTime,
    title="Battery Energy by Bus",
    yaxis_title="Energy (MWh)",
)
savefig(p_bus_ess_e, joinpath(plots_dir, "bus_ess_e.png"))

# Plot battery charging by bus
p_bus_ess_ch = plot_stacked(
    dfs_res["post"]["bus_ess_ch"],
    bus_to_name;
    timecol=:DateTime,
    title="Battery Charging by Bus",
    yaxis_title="Power (MW)",
)
savefig(p_bus_ess_ch, joinpath(plots_dir, "bus_ess_ch.png"))

# Plot battery discharging by bus
p_bus_ess_dch = plot_stacked(
    dfs_res["post"]["bus_ess_dch"],
    bus_to_name;
    timecol=:DateTime,
    title="Battery Discharging by Bus",
    yaxis_title="Power (MW)",
)
savefig(p_bus_ess_dch, joinpath(plots_dir, "bus_ess_dch.png"))

# Plot battery charging and discharging by bus
p_bus_ess_chdch = plot_stacked(
    dfs_res["post"]["bus_ess_chdch"],
    bus_to_name;
    timecol=:DateTime,
    title="Battery Charging and Discharging by Bus",
    yaxis_title="Power (MW)",
)
savefig(p_bus_ess_chdch, joinpath(plots_dir, "bus_ess_chdch.png"))

# Plot battery energy by area
p_area_ess_e = plot_stacked(
    dfs_res["post"]["area_ess_e"],
    area_to_name;
    timecol=:DateTime,
    title="Battery Energy by Area",
    yaxis_title="Energy (MWh)",
)
savefig(p_area_ess_e, joinpath(plots_dir, "area_ess_e.png"))

# Plot battery charging by area
p_area_ess_ch = plot_stacked(
    dfs_res["post"]["area_ess_ch"],
    area_to_name;
    timecol=:DateTime,
    title="Battery Charging by Area",
    yaxis_title="Power (MW)",
)
savefig(p_area_ess_ch, joinpath(plots_dir, "area_ess_ch.png"))

# Plot battery discharging by area
p_area_ess_dch = plot_stacked(
    dfs_res["post"]["area_ess_dch"],
    area_to_name;
    timecol=:DateTime,
    title="Battery Discharging by Area",
    yaxis_title="Power (MW)",
)
savefig(p_area_ess_dch, joinpath(plots_dir, "area_ess_dch.png"))

# Plot battery charging and discharging by area
p_area_ess_chdch = plot_stacked(
    dfs_res["post"]["area_ess_chdch"],
    area_to_name;
    timecol=:DateTime,
    title="Battery Charging and Discharging by Area",
    yaxis_title="Power (MW)",
)
savefig(p_area_ess_chdch, joinpath(plots_dir, "area_ess_chdch.png"))
