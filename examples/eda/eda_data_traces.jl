using Plots, Dates, DataFrames, Arrow

data_files = Dict(
    "bus" => "Bus",
    "demand" => "Demand",
    "der" => "DER",
    "storage" => "ESS",
    "generator" => "Generator",
    "line" => "Line",
)
trace_files = Dict(
    "demand_l_ts" => "Demand_load_sched",
    "generator_pmax_ts" => "Generator_pmax_sched",
    "generator_n_ts" => "Generator_n_sched",
    "der_p_ts" => "DER_pred_sched",
    "storage_emax_ts" => "ESS_emax_sched",
    "storage_lmax_ts" => "ESS_lmax_sched",
    "storage_n_ts" => "ESS_n_sched",
    "storage_pmax_ts" => "ESS_pmax_sched",
    "line_tmax_ts" => "Line_tmax_sched",
    "line_tmin_ts" => "Line_tmin_sched",
)

nem_reliability_data_dir = joinpath(@__DIR__, "../../..", "NEM-reliability-suite")
pisp_data_dir = joinpath(nem_reliability_data_dir, "data/pisp-datasets/out-ref4006-poe10")
arrow_dir = joinpath(pisp_data_dir, "arrow")
system_data_dir = arrow_dir
schedule_names = filter(
    name -> startswith(name, "schedule-"),
    readdir(arrow_dir)
)
schedule_name = schedule_names[1]

ts_data_dir = joinpath(system_data_dir, schedule_name)
scenario = 1
interval = Hour(24)

data = Dict{String,Any}()
for (k, fname) in data_files
    path = joinpath(system_data_dir, fname * ".arrow")
    data[k] = DataFrame(Arrow.Table(path))
end

for (k, fname) in trace_files
    path = joinpath(ts_data_dir, fname * ".arrow")
    data[k] = DataFrame(Arrow.Table(path))
end

# unique(data["generator"][:, :fuel])
# 7-element Vector{String}:
#  "Coal"
#  "Diesel"
#  "Hydro"
#  "Hydrogen"
#  "Natural Gas"
#  "Solar"
#  "Wind"

df_gen_re = filter(:fuel => in(["Solar", "Wind"]), data["generator"])
df_gen_re_pmax_ts = filter(:id_gen => in(df_gen_re[:, :id_gen]), data["generator_pmax_ts"])

gen_id = 92
pmax = only(filter(:id_gen => ==(gen_id), df_gen_re)[:, :pmax])  # 100
vmax = maximum(filter(:id_gen => ==(gen_id), df_gen_re_pmax_ts)[:, :value])  # 989.8632
println("gen_id=$(gen_id): pmax=$(pmax), vmax=$(vmax), vmax < pmax => ", isless(vmax, pmax))
# NOTE: we can see that the trace maximum value (vmax) is much higher than the pmax in
# the system data, because the system data `pmax` is a dummy value for RE components.
# This will be "fixed" by SiennaNEM.read_data.update_system_data_bound!


# plot demands
re_ids = Set(df_gen_re[:, :id_gen])
vre_ts = filter(:id_gen => in(re_ids), data["generator_pmax_ts"])

plts = map(1:3) do scen
    d = filter(:scenario => ==(scen), data["demand_l_ts"])
    v = filter(:scenario => ==(scen), vre_ts)

    d_sum = combine(groupby(d, :date), :value => sum => :total)
    v_sum = combine(groupby(v, :date), :value => sum => :total)

    p = plot(title = "Scenario $scen", xlabel = "Time", ylabel = "Value")
    plot!(p, d_sum[:, :date], d_sum[:, :total], label = "Demand", color = :steelblue)
    plot!(p, v_sum[:, :date], v_sum[:, :total], label = "VRE", color = :orange)
    p
end

plot(plts..., layout = (3, 1), size = (900, 900))
