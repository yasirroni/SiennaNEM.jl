using PowerGraphics
using PowerAnalytics
using PowerSystems

const PSY = PowerSystems

plotlyjs()

# embed system data into result
set_system!(res, sys)

# extract timestamps and generation data
timestamps = PowerSimulations.get_realized_timestamps(res)
gen = get_generation_data(res)

# line plot of various available variables
p = plot_powerdata(gen)

# line plot example for specific DataFrame
p = plot_dataframe(
    dfs_res["variable"]["ActivePowerVariable__RenewableDispatch"], timestamps
)

# stacked plot example for specific DataFrame
p = plot_dataframe(
    dfs_res["variable"]["ActivePowerVariable__RenewableDispatch"], timestamps;
    stack=true
)

# aggregated stacked plot for fuel with mapping file
plot_fuel(res; generator_mapping_file="deps/generator_mapping.yml")

# plot demand profile
plot_demand(res)  # based on result, horizon follow result
plot_demand(sys, aggregation=PSY.System)  # based on system, full horizon unless specified
plot_demand(sys, aggregation=PSY.PowerLoad)  # based on system, full horizon unless specified
plot_demand(sys, aggregation=PSY.PowerLoad, stack=true)  # based on system, full horizon unless specified
plot_demand(sys, aggregation=PSY.Area)  # based on system, full horizon unless specified
plot_demand(sys, aggregation=PSY.Area, stack=true)  # based on system, full horizon unless specified
plot_demand(sys, aggregation=PSY.ACBus)  # based on system, full horizon unless specified
plot_demand(sys, aggregation=PSY.ACBus, stack=true)  # based on system, full horizon unless specified

# TODO: plot services
