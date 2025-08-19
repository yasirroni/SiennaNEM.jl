# TODO:
#   Check and support all types below
# Row │ tech                         type
#     │ String31                     String31
# ────┼──────────────────────────────────────────────────────────
#   1 │ Coal                         Coal
#   2 │ Black Coal QLD               Steam Sub Critical
#   3 │ Diesel                       Diesel
#   4 │ Diesel                       Reciprocating Engine
#   5 │ Hydro                        Hydro
#   6 │ Hydrogen-based gas turbines  Hydrogen-based gas turbines
#   7 │ OCGT                         OCGT
#   8 │ CCGT                         CCGT
#   9 │ OCGT                         Gas-powered steam turbine
#  10 │ RoofPV                       RoofPV
#  11 │ LargePV                      LargePV
#  12 │ Wind                         Wind

# NOTE:
#   Sienna also has WS for off shore wind turbine, but no off shore wind in
# Australia for now
#   Various other coal and oil fuel availables.
# TODO:
#   Support black coal for QLD
#   The Hydro is decided as Dam

if !isdefined(Main, :type_to_primemover)
    const type_to_primemover = Dict(
        "Coal" => PrimeMovers.ST,
        "Steam Sub Critical" => PrimeMovers.ST,
        "Diesel" => PrimeMovers.IC,
        "Reciprocating Engine" => PrimeMovers.IC,
        "Hydro" => PrimeMovers.HY,
        "Hydrogen-based gas turbines" => PrimeMovers.GT,
        "OCGT" => PrimeMovers.GT,
        "CCGT" => PrimeMovers.CC,
        "Gas-powered steam turbine" => PrimeMovers.ST,
        "RoofPV" => PrimeMovers.PVe,
        "LargePV" => PrimeMovers.PVe,
        "Wind" => PrimeMovers.WT,
        "BESS" => PrimeMovers.BA,
        "PS" => PrimeMovers.PS,
    )
end
if !isdefined(Main, :type_to_datatype)
    const type_to_datatype = Dict(
        "Coal" => ThermalStandard,
        "Steam Sub Critical" => ThermalStandard,
        "Diesel" => ThermalStandard,
        "Reciprocating Engine" => ThermalStandard,
        "Hydro" => HydroDispatch,
        "Hydrogen-based gas turbines" => ThermalStandard,
        "OCGT" => ThermalStandard,
        "CCGT" => ThermalStandard,
        "Gas-powered steam turbine" => ThermalStandard,
        "RoofPV" => RenewableNonDispatch,
        "LargePV" => RenewableDispatch,
        "Wind" => RenewableDispatch,
        "BESS" => EnergyReservoirStorage,
        "PS" => HydroPumpedStorage,
    )
end
if !isdefined(Main, :type_to_fuel)
    const type_to_fuel = Dict(
        "Coal" => ThermalFuels.COAL,
        "Steam Sub Critical" => ThermalFuels.COAL,
        "Diesel" => ThermalFuels.DISTILLATE_FUEL_OIL,
        "Reciprocating Engine" => ThermalFuels.DISTILLATE_FUEL_OIL,
        "Hydrogen-based gas turbines" => ThermalFuels.OTHER,
        "OCGT" => ThermalFuels.NATURAL_GAS,
        "CCGT" => ThermalFuels.NATURAL_GAS,
        "Gas-powered steam turbine" => ThermalFuels.NATURAL_GAS,
    )
end
