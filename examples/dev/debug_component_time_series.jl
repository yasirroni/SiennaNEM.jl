using PowerSystems
using Dates
using TimeSeries

initial_time = DateTime("2025-01-07T00:00:00")  # start of the first slice
horizon = Hour(24)  # size of each slice
interval = Hour(1)  # timestep within each slice
window_shift = Hour(24)  # shift between slices

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
show_time_series(power_load)
ta = get_time_series_array(
    DeterministicSingleTimeSeries, power_load, "max_active_power";
    start_time=initial_time
)
ta_times = timestamp(ta)
