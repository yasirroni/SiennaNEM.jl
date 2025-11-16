data["components"]
data["components"]["generators"]
data["components"]["thermal_generators"]
data["components"]["renewable_dispatch_generators"]
data["components"]["renewable_nondispatch_generators"]
data["components"]["hydro_dispatch_generators"]
data["components"]["hydro_energyreservoir_generators"]
length(data["components"]["generators"]) == (
    length(data["components"]["thermal_generators"]) +
    length(data["components"]["renewable_dispatch_generators"]) +
    length(data["components"]["renewable_nondispatch_generators"]) +
    length(data["components"]["hydro_dispatch_generators"]) +
    length(data["components"]["hydro_energyreservoir_generators"])
)

generator_keys_1 = collect(keys(data["components"]["generators"]))
generator_keys_2 = vcat(
    collect(keys(data["components"]["thermal_generators"])),
    collect(keys(data["components"]["renewable_dispatch_generators"])),
    collect(keys(data["components"]["renewable_nondispatch_generators"])),
    collect(keys(data["components"]["hydro_dispatch_generators"])),
    collect(keys(data["components"]["hydro_energyreservoir_generators"])),
)

missing_in_concat = setdiff(generator_keys_2, generator_keys_1)
extra_in_concat = setdiff(generator_keys_1, generator_keys_2)

println("Keys in concatenated list but missing from generators: ", missing_in_concat)
println("Keys in generators but missing from concatenated list: ", extra_in_concat)
