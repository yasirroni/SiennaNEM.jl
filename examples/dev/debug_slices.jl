time_slices = create_time_slices(
    data["demand_l_ts"],
    initial_time = DateTime("2025-01-07T00:00:00"),
    horizon = Hour(24),
    window_shift = Hour(24),
)
keys_array = collect(keys(time_slices))  # in ordered DateTime keys

components_with_time_series = vcat(
    collect(values(get_flat_generators(data["components"]["renewable_dispatch_generators"]))),
    collect(values(get_flat_generators(data["components"]["renewable_nondispatch_generators"]))),
    collect(values(data["components"]["demands"]))
)
