## Shared function for wind and solar REZ mesh plots

using Dates
using DataFrames
using Statistics
using PlotlyJS
import PlotlyJS: scatter, Layout, Plot, attr

function _filter_format_bus_level_for_csv(
    bus_level::DataFrame,
    bus_to_idgen::Dict{Int,Int};
    drop_missing_idgen::Bool=true,
    date_shift_days::Int=0,
)
    out = copy(bus_level)

    # Keep original CSV-esque date string format, but allow shifting
    if eltype(out.date) <: AbstractString
        fmt = dateformat"yyyy-mm-dd HH:MM:SS"
        dt = DateTime.(String.(out.date), fmt)

        if date_shift_days != 0
            dt = dt .+ Day(date_shift_days)
        end

        # convert back to the same string format for CSV output
        out.date = Dates.format.(dt, fmt)
    else
        # non-string dates: just shift, keeping DateTime/Date type
        if date_shift_days != 0
            out.date = out.date .+ Day(date_shift_days)
        end
    end

    # map bus -> id_gen
    out.id_gen = get.(Ref(bus_to_idgen), Int.(out.id_bus), missing)

    if drop_missing_idgen
        filter!(row -> !ismissing(row.id_gen), out)
    end

    # final formatting
    out.id_gen = Int.(out.id_gen)
    out.id = out.id_gen

    out = select(
        out,
        :id,
        :id_rez_mesh,
        :scenario,
        :date,
        :value,
        :id_bus,
        :bus_name,
        :id_rez,
        :rez_name,
        :id_gen,
    )
    sort!(out, [:scenario, :id_gen, :date])

    return out
end

function _as_datetime_for_plot(x, fmt::DateFormat)
    if x isa DateTime
        return x
    elseif x isa Date
        return DateTime(x)
    else
        return DateTime(String(x), fmt)
    end
end

function _rgba(rgb::NTuple{3,Int}, a::Real)
    return "rgba($(rgb[1]),$(rgb[2]),$(rgb[3]),$(Float64(a)))"
end

function plot_rez_mesh_cf_by_bus(
    df_mesh::DataFrame;
    id_bus_sel::Int,
    scenario_sel::Int=1,
    dt_start::DateTime,
    dt_end::DateTime,
    title_prefix::AbstractString="REZ CF",
    yaxis_title::AbstractString="CF (p.u.)",

    # OPTIONAL 1: mean line per REZ
    show_rez_mean::Bool=false,
    df_mean_rez::Union{Nothing,DataFrame}=nothing,

    # OPTIONAL 2: overall bus mean
    show_bus_mean::Bool=false,

    # OPTIONAL 3: low-spatial-granularity line, e.g. one profile per bus
    show_low_spatial::Bool=false,
    df_low_spatial::Union{Nothing,DataFrame}=nothing,
    low_spatial_generator::Union{Nothing,DataFrame}=nothing,
    low_spatial_name::AbstractString="low-granularity CF (bus profile)",

    # mesh / mean dataframe columns
    date_col::Symbol=:date,
    value_col::Symbol=:value,
    mean_value_col::Symbol=:cf_mean,
    scenario_col::Symbol=:scenario,
    id_bus_col::Symbol=:id_bus,
    bus_name_col::Symbol=:bus_name,
    id_rez_col::Symbol=:id_rez,
    rez_name_col::Symbol=:rez_name,
    id_rez_mesh_col::Symbol=:id_rez_mesh,

    # low-spatial dataframe columns
    low_date_col::Symbol=:date,
    low_value_col::Symbol=:value,
    low_scenario_col::Symbol=:scenario,
    low_id_bus_col::Symbol=:id_bus,
    low_id_gen_col::Symbol=:id_gen,

    # generator lookup columns, used only if df_low_spatial does not already have id_bus
    generator_id_gen_col::Symbol=:id_gen,
    generator_id_bus_col::Symbol=:id_bus, date_fmt::DateFormat=dateformat"yyyy-mm-dd HH:MM:SS",
    mesh_alpha::Real=0.10,
    mean_alpha::Real=1.00,
    legend_alpha::Real=1.00,
    mesh_line_width::Real=1,
    mean_line_width::Real=3,
    legend_line_width::Real=3,
    bus_mean_line_width::Real=4,
    low_spatial_line_width::Real=3,
    width::Int=1100,
    height::Int=500,
)
    # --- prep: filter + parse datetimes ---
    df_bus_vre = filter(scenario_col => ==(scenario_sel), df_mesh)
    df_bus_vre = filter(id_bus_col => ==(id_bus_sel), df_bus_vre)

    df_bus_vre = copy(df_bus_vre)
    df_bus_vre[!, :datetime] = _as_datetime_for_plot.(df_bus_vre[!, date_col], Ref(date_fmt))
    df_bus_vre = filter(:datetime => d -> dt_start <= d <= dt_end, df_bus_vre)
    sort!(df_bus_vre, [id_rez_col, id_rez_mesh_col, :datetime])

    # --- bus label (handles empty + missing) ---
    bus_name = let
        if nrow(df_bus_vre) == 0 || !(bus_name_col in propertynames(df_bus_vre))
            "bus $(id_bus_sel)"
        else
            nm = collect(skipmissing(df_bus_vre[!, bus_name_col]))
            isempty(nm) ? "bus $(id_bus_sel)" : first(nm)
        end
    end

    # --- colors: one base RGB per REZ, alpha for mesh vs mean/legend ---
    palette_rgb = [
        (31, 119, 180),   # blue
        (255, 127, 14),   # orange
        (44, 160, 44),    # green
        (214, 39, 40),    # red
        (148, 103, 189),  # purple
        (140, 86, 75),    # brown
        (227, 119, 194),  # pink
        (127, 127, 127),  # gray
        (188, 189, 34),   # olive
        (23, 190, 207),   # cyan
    ]

    rez_keys = unique(select(df_bus_vre, [id_rez_col, rez_name_col]))
    sort!(rez_keys, id_rez_col)

    rez_to_rgb = Dict{Int,NTuple{3,Int}}()
    for (i, r) in enumerate(eachrow(rez_keys))
        rez_to_rgb[Int(r[id_rez_col])] = palette_rgb[mod1(i, length(palette_rgb))]
    end

    # --- OPTIONAL 1: mean per REZ ---
    df_mean_rez_plot = nothing

    if show_rez_mean
        if df_mean_rez === nothing
            # Compute REZ mean from the mesh-level dataframe
            df_mean_rez_plot = combine(
                groupby(df_bus_vre, [scenario_col, :datetime, id_rez_col, rez_name_col]),
                value_col => mean => mean_value_col,
            )
        else
            # Use precomputed REZ mean dataframe
            df_mean_rez_plot = filter(scenario_col => ==(scenario_sel), df_mean_rez)
            df_mean_rez_plot = filter(id_bus_col => ==(id_bus_sel), df_mean_rez_plot)
            df_mean_rez_plot = copy(df_mean_rez_plot)
            df_mean_rez_plot[!, :datetime] = _as_datetime_for_plot.(df_mean_rez_plot[!, date_col], Ref(date_fmt))
            df_mean_rez_plot = filter(:datetime => d -> dt_start <= d <= dt_end, df_mean_rez_plot)
        end

        sort!(df_mean_rez_plot, [id_rez_col, :datetime])
    end

    # --- OPTIONAL 2: overall bus mean across all meshes ---
    df_mean_bus = nothing

    if show_bus_mean
        df_mean_bus = combine(
            groupby(df_bus_vre, [scenario_col, :datetime]),
            value_col => mean => :cf_mean_bus,
        )
        sort!(df_mean_bus, :datetime)
    end

    # --- OPTIONAL 3: low-spatial-granularity line ---
    df_low_spatial_plot = nothing

    if show_low_spatial
        if df_low_spatial === nothing
            error("show_low_spatial=true requires df_low_spatial to be supplied.")
        end

        df_low_spatial_plot = copy(df_low_spatial)

        # If the low-spatial dataframe has id_gen but not id_bus, add id_bus from generator metadata.
        if !(low_id_bus_col in propertynames(df_low_spatial_plot))
            if low_spatial_generator === nothing
                error("df_low_spatial does not have $(low_id_bus_col). Supply low_spatial_generator to join bus metadata.")
            end

            gen_lookup = select(
                low_spatial_generator,
                generator_id_gen_col => low_id_gen_col,
                generator_id_bus_col => low_id_bus_col,
            )

            df_low_spatial_plot = leftjoin(
                df_low_spatial_plot,
                gen_lookup;
                on=low_id_gen_col,
            )
        end

        df_low_spatial_plot[!, :datetime] = _as_datetime_for_plot.(df_low_spatial_plot[!, low_date_col], Ref(date_fmt))
        df_low_spatial_plot = filter(low_scenario_col => ==(scenario_sel), df_low_spatial_plot)
        df_low_spatial_plot = filter(low_id_bus_col => ==(id_bus_sel), df_low_spatial_plot)
        df_low_spatial_plot = filter(:datetime => d -> dt_start <= d <= dt_end, df_low_spatial_plot)
        sort!(df_low_spatial_plot, :datetime)
    end

    # --- build traces ---
    mesh_traces = PlotlyJS.GenericTrace[]
    rez_display_traces = PlotlyJS.GenericTrace[]

    # Mesh traces, grouped by REZ (same shade within REZ)
    for r in eachrow(rez_keys)
        id_rez = Int(r[id_rez_col])
        rez_name = r[rez_name_col]
        base_rgb = rez_to_rgb[id_rez]

        mesh_color = _rgba(base_rgb, mesh_alpha)
        mean_color = _rgba(base_rgb, mean_alpha)
        legend_color = _rgba(base_rgb, legend_alpha)
        group_name = "rez_$(id_rez)"

        mesh_ids_rez = unique(df_bus_vre[!, id_rez_mesh_col][df_bus_vre[!, id_rez_col].==id_rez])

        for mid in mesh_ids_rez
            sub = view(
                df_bus_vre,
                (df_bus_vre[!, id_rez_col] .== id_rez) .&
                (df_bus_vre[!, id_rez_mesh_col] .== mid),
                :,
            )

            push!(
                mesh_traces,
                scatter(
                    x=sub.datetime,
                    y=sub[!, value_col],
                    mode="lines",
                    name="REZ $(id_rez): $(rez_name)",
                    showlegend=false,
                    legendgroup=group_name,
                    line=attr(color=mesh_color, width=mesh_line_width),
                    hovertemplate="REZ: %{customdata[1]}<br>mesh %{customdata[2]}<br>%{x}<br>cf=%{y:.3f}<extra></extra>",
                    customdata=hcat(
                        fill(string(rez_name), nrow(sub)),
                        fill(string(mid), nrow(sub)),
                    ),
                ),
            )
        end

        if show_rez_mean
            # OPTIONAL 1: mean line per REZ (opaque, shown in legend)
            subm = view(df_mean_rez_plot, df_mean_rez_plot[!, id_rez_col] .== id_rez, :)

            push!(
                rez_display_traces,
                scatter(
                    x=subm.datetime,
                    y=subm[!, mean_value_col],
                    mode="lines",
                    name="REZ $(id_rez): $(rez_name) mean",
                    showlegend=true,
                    legendgroup=group_name,
                    line=attr(color=mean_color, width=mean_line_width),
                    hovertemplate="REZ mean<br>REZ: $(rez_name)<br>%{x}<br>cf=%{y:.3f}<extra></extra>",
                ),
            )
        else
            # Legend-only dummy trace: shown in legend, not visible on plot.
            # y = missing means no actual line is drawn, but the legend swatch stays opaque.
            push!(
                rez_display_traces,
                scatter(
                    x=[dt_start, dt_end],
                    y=[missing, missing],
                    mode="lines",
                    name="REZ $(id_rez): $(rez_name)",
                    showlegend=true,
                    legendgroup=group_name,
                    line=attr(color=legend_color, width=legend_line_width),
                    hoverinfo="skip",
                ),
            )
        end
    end

    plot_traces = PlotlyJS.GenericTrace[]
    append!(plot_traces, mesh_traces)
    append!(plot_traces, rez_display_traces)

    if show_bus_mean
        # OPTIONAL 2: overall bus mean (black)
        overall_mean_trace = scatter(
            x=df_mean_bus.datetime,
            y=df_mean_bus.cf_mean_bus,
            mode="lines",
            name="bus mean",
            line=attr(color="rgba(0,0,0,1.0)", width=bus_mean_line_width, dash="dash"),
            hovertemplate="bus mean<br>%{x}<br>cf=%{y:.3f}<extra></extra>",
        )
        push!(plot_traces, overall_mean_trace)
    end

    if show_low_spatial
        # OPTIONAL 3: low-spatial-granularity (1 profile per bus)
        low_spatial_trace = scatter(
            x=df_low_spatial_plot.datetime,
            y=df_low_spatial_plot[!, low_value_col],
            mode="lines",
            name=low_spatial_name,
            line=attr(color="rgba(90,90,90,0.95)", width=low_spatial_line_width, dash="dash"),
            hovertemplate="low-gran CF<br>%{x}<br>cf=%{y:.3f}<extra></extra>",
        )
        push!(plot_traces, low_spatial_trace)
    end

    title_bits = String[]
    push!(title_bits, "meshes")
    show_rez_mean && push!(title_bits, "REZ means")
    show_bus_mean && push!(title_bits, "bus mean")
    show_low_spatial && push!(title_bits, "low-granularity profile")
    title_suffix = join(title_bits, " + ")

    layout = Layout(
        title="$(title_prefix) — $(title_suffix) (bus $(id_bus_sel): $(bus_name))",
        xaxis=attr(title="Date"),
        yaxis=attr(title=yaxis_title),
        width=width,
        height=height,
        legend=attr(
            x=1.02,
            y=1.0,
            groupclick="togglegroup",
        ),
        margin=attr(l=70, r=40, t=70, b=60),
    )
    plt = Plot(plot_traces, layout)
    return plt
end

# id_bus_sel = 8  # SNSW
id_bus_sel = 1  # NQ
scenario_sel = 1  # TODO: do scenario 2

dt_start = DateTime("2040-02-07 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")
dt_end = DateTime("2040-02-13 00:00:00", dateformat"yyyy-mm-dd HH:MM:SS")


## Wind example
rez_windcf_bus_filtered = _filter_format_bus_level_for_csv(
    rez_windcf_bus,
    wind_bus_to_idgen;
    drop_missing_idgen=true,
    date_shift_days=0,
)

plt_wind = plot_rez_mesh_cf_by_bus(
    rez_windcf_bus_filtered;
    id_bus_sel=id_bus_sel,
    scenario_sel=scenario_sel,
    dt_start=dt_start,
    dt_end=dt_end,
    title_prefix="REZ Wind CF",
    yaxis_title="CF (p.u.)",

    # OPTIONAL switches
    show_rez_mean=false,
    show_bus_mean=false,
    show_low_spatial=false,

    # Needed only when show_low_spatial=true
    df_low_spatial=windcf_sched,
    low_spatial_generator=data["generator"],
    low_spatial_name="low-granularity CF (bus profile)",
    mesh_alpha=0.10,
    legend_alpha=1.00,
)
plt_wind

# Example with all wind optional traces enabled:
# plt_wind_all = plot_rez_mesh_cf_by_bus(
#     rez_windcf_bus_filtered;
#     id_bus_sel=id_bus_sel,
#     scenario_sel=scenario_sel,
#     dt_start=dt_start,
#     dt_end=dt_end,
#     title_prefix="REZ Wind CF",
#     yaxis_title="CF (p.u.)",
#     show_rez_mean=true,
#     show_bus_mean=true,
#     show_low_spatial=true,
#     df_low_spatial=windcf_sched,
#     low_spatial_generator=data["generator"],
#     low_spatial_name="low-granularity CF (bus profile)",
#     mean_value_col=:cf_mean_rez,
#     mesh_alpha=0.10,
#     mean_alpha=1.00,
# )
# plt_wind_all


## LargePV example
plt_largepv = plot_rez_mesh_cf_by_bus(
    rez_pvmodcf_largepv_bus;
    id_bus_sel=id_bus_sel,
    scenario_sel=scenario_sel,
    dt_start=dt_start,
    dt_end=dt_end,
    title_prefix="REZ PV module CF",
    yaxis_title="CF (p.u.)",

    # OPTIONAL switches
    show_rez_mean=false,
    show_bus_mean=false,
    show_low_spatial=false,

    # Needed only when show_rez_mean=true and using precomputed REZ mean
    df_mean_rez=rez_pvmodcf_largepv_bus_mean,
    mean_value_col=:cf_mean,
    mesh_alpha=0.10,
    mean_alpha=1.00,
)
plt_largepv
