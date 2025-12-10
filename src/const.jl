# TODO:
#   Check and support all types below
# 
# unique(data["generator"][:, [:tech, :type]])
# combine(groupby(data["generator"], [:tech, :type]), nrow => :count)
# 
# Row │ tech                         type                         count 
#     │ String31                     String31                     Int64 
# ───-┼─────────────────────────────────────────────────────────────────
#   1 │ Black Coal NSW               Steam Sub Critical               4
#   2 │ Black Coal QLD               Steam Sub Critical               4
#   3 │ Black Coal QLD               Steam Super Critical             4
#   4 │ Brown Coal VIC               Steam Sub Critical               2
#   5 │ Brown Coal                   Steam Sub Critical               1
#   6 │ Diesel                       Reciprocating Engine             7
#   7 │ Run-of-River                 Vertical Francis                 1
#   8 │ Run-of-River                 Francis / Pelton                 1
#   9 │ Run-of-River                 Pelton                           1
#  10 │ Reservoir                    Francis                         22
#  11 │ Reservoir                    Pelton                           2
#  12 │ Reservoir                    Kaplan                           2
#  13 │ Reservoir                    Nozzle-spear Pelton              1
#  14 │ Hydrogen-based gas turbines  Hydrogen-based gas turbines      2
#  15 │ OCGT                         OCGT                            24
#  16 │ OCGT                         GE Frame 5                       1
#  17 │ OCGT                         Gas-powered steam turbine        2
#  18 │ OCGT                         Frame 6B                         1
#  19 │ CCGT                         CCGT                             9
#  20 │ RoofPV                       RoofPV                          12
#  21 │ LargePV                      LargePV                         10
#  22 │ Wind                         Wind                            10

# NOTE:
#   Sienna also has WS for off shore wind turbine, but no off shore wind in
# Australia for now
#   Various other coal and oil fuel availables.

# https://nrel-sienna.github.io/PowerSystems.jl/stable/api/enumerated_types/

if !isdefined(Main, :tech_to_primemover)
    const tech_to_primemover = Dict(
        "Black Coal NSW" => PrimeMovers.ST,
        "Black Coal QLD" => PrimeMovers.ST,
        "Brown Coal VIC" => PrimeMovers.ST,
        "Brown Coal" => PrimeMovers.ST,
        "Diesel" => PrimeMovers.IC,
        "Run-of-River" => PrimeMovers.HY,
        "Reservoir" => PrimeMovers.HY,
        "Hydrogen-based gas turbines" => PrimeMovers.GT,
        "OCGT" => PrimeMovers.GT,
        "CCGT" => PrimeMovers.CC,
        "RoofPV" => PrimeMovers.PVe,
        "LargePV" => PrimeMovers.PVe,
        "Wind" => PrimeMovers.WT,
        "BESS" => PrimeMovers.BA,
        "PS" => PrimeMovers.HY,
    )
end

# TODO: use tech instead of type
if !isdefined(Main, :tech_to_datatype)
    const tech_to_datatype = Dict(
        "Black Coal NSW" => ThermalStandard,
        "Black Coal QLD" => ThermalStandard,
        "Brown Coal VIC" => ThermalStandard,
        "Brown Coal" => ThermalStandard,
        "Diesel" => ThermalStandard,
        "Run-of-River" => HydroDispatch,
        "Reservoir" => HydroTurbine,
        "Hydrogen-based gas turbines" => ThermalStandard,
        "OCGT" => ThermalStandard,
        "CCGT" => ThermalStandard,
        "RoofPV" => RenewableNonDispatch,
        "LargePV" => RenewableDispatch,
        "Wind" => RenewableDispatch,
        "BESS" => EnergyReservoirStorage,
        "PS" => HydroPumpTurbine,
    )
end
if !isdefined(Main, :tech_to_fuel)
    const tech_to_fuel = Dict(
        "Black Coal NSW" => ThermalFuels.COAL,
        "Black Coal QLD" => ThermalFuels.COAL,
        "Brown Coal VIC" => ThermalFuels.COAL,
        "Brown Coal" => ThermalFuels.COAL,
        "Diesel" => ThermalFuels.DISTILLATE_FUEL_OIL,
        "Hydrogen-based gas turbines" => ThermalFuels.OTHER,
        "OCGT" => ThermalFuels.NATURAL_GAS,
        "CCGT" => ThermalFuels.NATURAL_GAS,
    )
end

if !isdefined(Main, :area_to_name)
    const area_to_name = OrderedDict(
        1 => "QLD",
        2 => "NSW",
        3 => "VIC",
        4 => "TAS",
        5 => "SA",
    )
end

# TODO: use,
#   read_expressions
#   read_aux_variables
#   read_parameters
#   read_variables
#   read_duals
if !isdefined(Main, :optimization_result_handlers)
    const optimization_result_handlers = [
        ("expression", list_expression_names, read_expression),
        ("aux_variable", list_aux_variable_names, read_aux_variable),
        ("parameter", list_parameter_names, read_parameter),
        ("variable", list_variable_names, read_variable),
        ("dual", list_dual_names, read_dual),
    ]
end
