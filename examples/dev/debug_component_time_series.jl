using PowerSystems
using Dates
using TimeSeries
using InfrastructureSystems

initial_time = DateTime("2025-01-07T00:00:00")  # start of the first slice
horizon = Hour(24)  # size of each slice
interval = Hour(1)  # timestep within each slice
window_shift = Hour(24)  # shift between slices

# RenewableNonDispatch
renewable_non_dispatch = first(collect(get_components(RenewableNonDispatch, sys)))
ta = get_time_series_array(
    Deterministic, renewable_non_dispatch, "max_active_power";
    start_time=initial_time
)
ta_times = timestamp(ta)
ta_start = ta_times[1]
ta_end   = ta_times[end]

power_load = first(collect(get_components(PowerLoad, sys)))
show_time_series(power_load)

renewable_non_dispatch = first(collect(get_components(RenewableNonDispatch, sys)))
show_time_series(renewable_non_dispatch)
get_time_series_array(
    Deterministic, renewable_non_dispatch, "max_active_power";
    start_time=initial_time
)

# PowerLoad
power_load = first(collect(get_components(PowerLoad, sys)))
initial_times = collect(InfrastructureSystems.get_forecast_initial_times(sys.data))
minimum_initial_time = first(initial_times)
maximum_initial_time = last(initial_times)
show_time_series(power_load)
ta = get_time_series_array(
    DeterministicSingleTimeSeries, power_load, "max_active_power";
    start_time=minimum_initial_time
)
ta = get_time_series_array(
    DeterministicSingleTimeSeries, power_load, "max_active_power";
    start_time=maximum_initial_time
)

# to get the full forecasts, not just single array
forecast = get_time_series(DeterministicSingleTimeSeries, power_load, "max_active_power")
initial_timestamp = get_initial_timestamp(forecast)
step = InfrastructureSystems.get_resolution(forecast)
horizon_count = InfrastructureSystems.get_horizon_count(forecast)
horizon = InfrastructureSystems.get_horizon(forecast)

# to get time series from system data directly
initial_times = collect(InfrastructureSystems.get_forecast_initial_times(sys.data))
horizon_count = get_forecast_window_count(sys)
horizon = Hour(get_forecast_horizon(sys))
initial_timestamp = get_forecast_initial_timestamp(sys)
interval = Hour(get_forecast_interval(sys))
