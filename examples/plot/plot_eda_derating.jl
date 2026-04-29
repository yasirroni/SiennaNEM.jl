using DataFrames
using Plots
using Dates

tas = 0.0:0.5:50.0

line_df =
    filter(:investment => ==(false),
        filter(:active => ==(true), data["line"])
    )

sort!(line_df, :id_lin)
labels = String.(line_df.alias)

function normalise_series(series)
    out = similar(series)
    for (i, v) in enumerate(series)
        finite = filter(isfinite, v)
        mx = isempty(finite) ? NaN : maximum(finite)
        out[i] = (isnan(mx) || mx == 0.0) ? fill(NaN, length(v)) : v ./ mx
    end
    out
end

function make_ta_grid(line_df, tas)
    n_lines = nrow(line_df)
    n_tas = length(tas)

    DataFrame(
        id = 1:(n_lines * n_tas),
        id_lin = repeat(line_df.id_lin, inner=n_tas),
        scenario = fill(1, n_lines * n_tas),
        date = fill(DateTime(2000, 1, 1), n_lines * n_tas),
        value = repeat(collect(Float64.(tas)), outer=n_lines),
    )
end

function df_to_series(cap_df, line_df, tas)
    n_tas = length(tas)
    cap_df = sort(cap_df, [:id_lin, :id])

    g = groupby(cap_df, :id_lin)
    by_id = Dict(key.id_lin => sdf for (key, sdf) in pairs(g))

    series = Vector{Vector{Float64}}(undef, nrow(line_df))
    for (i, id_lin) in enumerate(line_df.id_lin)
        sdf = by_id[id_lin]
        series[i] = Vector{Float64}(sdf.value[1:n_tas])
    end
    series
end

ta_df = make_ta_grid(line_df, tas)

cap_fwcap_df = get_branch_thermal_capacity(
    ta_df, line_df,
    (:tref_winter, :tref_summer, :tref_peak_demand,
     :fwcap, :fwcap_summer, :fwcap_peak_demand,
     :tm1_fwcap, :tm2_fwcap, :tm3_fwcap)
)

cap_rvcap_df = get_branch_thermal_capacity(
    ta_df, line_df,
    (:tref_winter, :tref_summer, :tref_peak_demand,
     :rvcap, :rvcap_summer, :rvcap_peak_demand,
     :tm1_rvcap, :tm2_rvcap, :tm3_rvcap)
)

cap_fwd = df_to_series(cap_fwcap_df, line_df, tas)
cap_rev = df_to_series(cap_rvcap_df, line_df, tas)

norm_fwd = normalise_series(cap_fwd)
norm_rev = normalise_series(cap_rev)

colors = [
    colorant"#2563eb", colorant"#16a34a", colorant"#dc2626", colorant"#d97706",
    colorant"#7c3aed", colorant"#0891b2", colorant"#db2777", colorant"#65a30d",
    colorant"#ea580c", colorant"#6d28d9", colorant"#0d9488", colorant"#b45309",
    colorant"#1d4ed8", colorant"#be123c", colorant"#15803d",
]
linestyles = [
    :solid, :dash, :dot, :dashdot, :dashdotdot,
    :solid, :dash, :dot, :dashdot, :dashdotdot,
    :solid, :dash, :dot, :dashdot, :dashdotdot,
]

function style_at(i)
    colors[mod1(i, length(colors))], linestyles[mod1(i, length(linestyles))]
end

function make_plot(series_fwd, series_rev, labels, tas, ylabel, title_suffix; kwargs...)
    base = (
        xlabel = "Ambient temperature (°C)",
        ylabel = ylabel,
        legend = :outertopright,
        legendfontsize = 7,
        titlefontsize  = 10,
        xlims  = (first(tas), last(tas)),
        xticks = 0:5:50,
        size   = (900, 420),
        grid   = true,
        gridalpha = 0.25,
        framestyle = :box,
    )

    x = collect(Float64.(tas))

    p_fwd = plot(; base..., title = "Forward flow (fwcap) — $title_suffix", kwargs...)
    for (i, v) in enumerate(series_fwd)
        c, ls = style_at(i)
        plot!(p_fwd, x, v; label=labels[i], color=c, linestyle=ls, linewidth=1.5)
    end

    p_rev = plot(; base..., title = "Reverse flow (rvcap) — $title_suffix", kwargs...)
    for (i, v) in enumerate(series_rev)
        c, ls = style_at(i)
        plot!(p_rev, x, v; label=labels[i], color=c, linestyle=ls, linewidth=1.5)
    end

    p_fwd, p_rev
end

imgs_dir = "examples/result/eda"
mkpath(imgs_dir)

p1_fwd, p1_rev = make_plot(cap_fwd, cap_rev, labels, tas, "Capacity (MW)", "capacity (MW)"; ylims=(0, Inf))
p_capacity = plot(p1_fwd, p1_rev; layout=(2, 1), size=(950, 860), dpi=150)
savefig(p_capacity, joinpath(imgs_dir, "plot_branch_capacity_derating_thermal_functions.png"))

p2_fwd, p2_rev = make_plot(norm_fwd, norm_rev, labels, tas, "Normalised capacity (0–1)", "normalised capacity"; ylims=(0, 1.05))
p_norm = plot(p2_fwd, p2_rev; layout=(2, 1), size=(950, 860), dpi=150)
savefig(p_norm, joinpath(imgs_dir, "plot_branch_normalised_capacity_derating_thermal_functions.png"))

using Plots
using Plots.PlotMeasures   # needed for `px` and `mm` units

# Parse dates once
windcf_sched[!, :datetime] = DateTime.(windcf_sched[!, :date], "yyyy-mm-dd HH:MM:SS")

id_gen_to_name = get_map_from_df(data["generator"], :id_gen, :name)
gen_ids = unique(windcf_sched[!, :id_gen])

# Filter by date range
dt_start = DateTime("2038-01-23 00:00:00", "yyyy-mm-dd HH:MM:SS")
dt_end   = DateTime("2038-01-25 00:00:00", "yyyy-mm-dd HH:MM:SS")
windcf_filtered = filter(:datetime => d -> dt_start <= d <= dt_end, windcf_sched)

plt = plot(
    xlabel = "Date",
    ylabel = "Correction Factor (p.u.)",
    title  = "Wind Thermal Correction Factor by Location",
    legend = :outertopright,
    size   = (1000, 500),
    left_margin = 5mm,
)

for gid in gen_ids
    sub = filter(:id_gen => ==(gid), windcf_filtered)
    sort!(sub, :datetime)
    plot!(plt, sub[!, :datetime], sub[!, :value]; label = id_gen_to_name[gid])
end

plt

# Parse dates
windcf_sched[!, :datetime] = DateTime.(windcf_sched[!, :date], "yyyy-mm-dd HH:MM:SS")
ta_df[!, :datetime]        = DateTime.(ta_df[!, :date], "yyyy-mm-dd HH:MM:SS")

# Filter by date range
dt_start = DateTime("2038-01-20 00:00:00", "yyyy-mm-dd HH:MM:SS")
dt_end   = DateTime("2038-01-26 23:00:00", "yyyy-mm-dd HH:MM:SS")

# Filter WIND_SNSW (id_gen = 120) within date range
id_gen = 120  # WIND_SNSW
cf_snsw = filter(
    r -> r.id_gen == id_gen && dt_start <= r.datetime <= dt_end,
    windcf_sched,
)
sort!(cf_snsw, :datetime)

ta_snsw = filter(
    r -> r.id_gen == id_gen && dt_start <= r.datetime <= dt_end,
    ta_df,
)
sort!(ta_snsw, :datetime)

# Two-panel plot: CF on top, temperature on bottom
plt = plot(
    layout = (2, 1),
    size   = (1000, 600),
    xlabel = "Date",
)

plot!(plt[1],
    cf_snsw[!, :datetime], cf_snsw[!, :value];
    ylabel      = "Correction Factor (p.u.)",
    title       = "WIND_SNSW — Thermal Correction Factor",
    label       = "CF",
    color       = :blue,
    left_margin = 5mm,
)

plot!(plt[2],
    ta_snsw[!, :datetime], ta_snsw[!, :value];
    ylabel      = "Temperature (°C)",
    title       = "WIND_SNSW — Ambient Temperature",
    label       = "t2m",
    color       = :red,
    left_margin = 5mm,
)

plt

using PlotlyJS
import PlotlyJS: scatter, Layout, Plot, attr

# --- Data prep ---
scenario_id = 1
windpmax_filtered = filter(:id_gen => idg -> idg in wind_id_gens, data["generator_pmax_ts"])
dt_start = DateTime("2038-01-23 00:00:00", "yyyy-mm-dd HH:MM:SS")
dt_end   = DateTime("2038-01-25 00:00:00", "yyyy-mm-dd HH:MM:SS")
windpmax_filtered = filter(:date => d -> dt_start <= d <= dt_end, windpmax_filtered)

windcf_sched[!, :datetime] = DateTime.(windcf_sched[!, :date], "yyyy-mm-dd HH:MM:SS")
windcf_filtered = filter(:id_gen => idg -> idg in wind_id_gens, windcf_sched)
windcf_filtered = filter(:datetime => d -> dt_start <= d <= dt_end, windcf_filtered)

pm_scen = filter(:scenario => ==(scenario_id), windpmax_filtered)
cf_scen = filter(:scenario => ==(scenario_id), windcf_filtered)

id_gen_to_name = get_map_from_df(data["generator"], :id_gen, :name)
gen_ids = unique(pm_scen[!, :id_gen])
labels  = [id_gen_to_name[gid] for gid in gen_ids]

# --- Build merged DataFrame ---
merged_all = DataFrame()
for gid in gen_ids
    pm = filter(:id_gen => ==(gid), pm_scen)
    cf = filter(:id_gen => ==(gid), cf_scen)
    merged = innerjoin(pm, cf, on = :date => :datetime, makeunique = true)
    sort!(merged, :date)
    merged[!, :pmax_corrected] = merged[!, :value] .* merged[!, :value_1]
    append!(merged_all, select(merged, :date, :id_gen, :value, :value_1, :pmax_corrected))
end

dates = sort(unique(merged_all[!, :date]))

# --- Build matrices ---
function build_matrix_from(df, val_col)
    hcat([
        begin
            sub = filter(:id_gen => ==(gid), df)
            sort!(sub, :date)
            sub[!, val_col]
        end
        for gid in gen_ids
    ]...)
end

before_gw = build_matrix_from(merged_all, :value)        ./ 1000
after_gw  = build_matrix_from(merged_all, :pmax_corrected) ./ 1000

# --- PlotlyJS stacked area, two subplots ---
colors = [
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
    "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac",
    "#d37295"  # 11th
]
traces_before = [
    scatter(
        x          = dates,
        y          = before_gw[:, i],
        name       = labels[i],
        stackgroup = "before",
        fill       = "tonexty",
        mode       = "lines",
        legendgroup = labels[i],
        line = attr(color = colors[i])
    )
    for i in 1:length(labels)
]

traces_after = [
    scatter(
        x          = dates,
        y          = after_gw[:, i],
        name       = labels[i],
        stackgroup = "after",
        fill       = "tonexty",
        mode       = "lines",
        legendgroup = labels[i],
        showlegend = false,   # avoid duplicate legend entries
        xaxis      = "x2",
        yaxis      = "y2",
        line = attr(color = colors[i])
    )
    for i in 1:length(labels)
]

layout = Layout(
    title  = "Wind Power — Before vs After CF Correction",
    xaxis  = attr(title = "", domain = [0, 1], anchor = "y"),
    yaxis  = attr(title = "Power Output (GW)", domain = [0.55, 1.0], anchor = "x"),
    xaxis2 = attr(title = "Date", domain = [0, 1], anchor = "y2"),
    yaxis2 = attr(title = "Power Output (GW)", domain = [0.0, 0.45], anchor = "x2"),
    legend = attr(x = 1.02, y = 1.0),
    annotations = [
        attr(text="Before CF Correction", x=0.5, y=1.02, xref="paper", yref="paper", showarrow=false, font=attr(size=13)),
        attr(text="After CF Correction",  x=0.5, y=0.47, xref="paper", yref="paper", showarrow=false, font=attr(size=13)),
    ],
)

Plot([traces_before; traces_after], layout)

id_gen = 120

pm_snsw = filter(:id_gen => ==(id_gen), pm_scen)
cf_snsw = filter(:id_gen => ==(id_gen), cf_scen)

merged = innerjoin(pm_snsw, cf_snsw, on = :date => :datetime, makeunique = true)
sort!(merged, :date)

println(names(merged))  # confirm column names
println(merged[1:5, :]) # sanity check

plt = Plots.plot(
    layout      = (2, 1),
    size        = (1000, 600),
    left_margin = 10mm,
    link        = :x,
)

Plots.plot!(merged[!, :date], merged[!, :value];
    label   = "Before (pmax)",
    ylabel  = "Power (MW)",
    title   = "WIND_SNSW — Before vs After CF",
    subplot = 1,
)

Plots.plot!(merged[!, :date], merged[!, :value] .* merged[!, :value_1];
    label   = "After (pmax × CF)",
    subplot = 1,
)

Plots.plot!(merged[!, :date], merged[!, :value_1];
    label   = "CF",
    ylabel  = "CF (p.u.)",
    title   = "WIND_SNSW — Correction Factor",
    xlabel  = "Date",
    subplot = 2,
    color   = :green,
)

plt