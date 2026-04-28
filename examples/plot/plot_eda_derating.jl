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
    title  = "Wind Thermal Correction Factor by Generator",
    legend = :outertopright,
    size   = (1000, 500),
)

for gid in gen_ids
    sub = filter(:id_gen => ==(gid), windcf_filtered)
    sort!(sub, :datetime)
    plot!(plt, sub[!, :datetime], sub[!, :value]; label = id_gen_to_name[gid])
end

plt

using Plots

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
    ylabel = "Correction Factor (p.u.)",
    title  = "WIND_SNSW — Thermal Correction Factor",
    label  = "CF",
    color  = :blue,
)

plot!(plt[2],
    ta_snsw[!, :datetime], ta_snsw[!, :value];
    ylabel = "Temperature (°C)",
    title  = "WIND_SNSW — Ambient Temperature",
    label  = "t2m",
    color  = :red,
)

plt
