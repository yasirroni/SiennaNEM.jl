data["components"]
data["components"]["generators"]
data["components"]["thermal_generators"]
data["components"]["renewable_dispatch_generators"]
data["components"]["renewable_nondispatch_generators"]
data["components"]["hydro_dispatch_generators"]
length(data["components"]["generators"]) == (
    length(data["components"]["thermal_generators"]) +
    length(data["components"]["renewable_dispatch_generators"]) +
    length(data["components"]["renewable_nondispatch_generators"]) +
    length(data["components"]["hydro_dispatch_generators"])
)

vcat(
    collect(values(data["components"]["thermal_generators"])),
    collect(values(data["components"]["renewable_dispatch_generators"])),
    collect(values(data["components"]["renewable_nondispatch_generators"])),
    collect(values(data["components"]["hydro_dispatch_generators"])),
)