display(res)

# Start: 2025-01-07T00:00:00
# End: 2025-01-07T23:00:00
# Resolution: 60 minutes

# PowerSimulations Problem Auxiliary variables Results
# ┌─────────────────────────────────────────────┐
# │ StorageEnergyOutput__EnergyReservoirStorage │
# │ TimeDurationOn__ThermalStandard             │
# │ TimeDurationOff__ThermalStandard            │
# └─────────────────────────────────────────────┘

# PowerSimulations Problem Expressions Results
# ┌─────────────────────────────────────────────┐
# │ ProductionCostExpression__RenewableDispatch │
# │ ProductionCostExpression__ThermalStandard   │
# │ ActivePowerBalance__ACBus                   │
# └─────────────────────────────────────────────┘

# PowerSimulations Problem Parameters Results
# ┌──────────────────────────────────────────────────────┐
# │ ActivePowerTimeSeriesParameter__RenewableNonDispatch │
# │ ActivePowerTimeSeriesParameter__PowerLoad            │
# │ ActivePowerTimeSeriesParameter__RenewableDispatch    │
# └──────────────────────────────────────────────────────┘

# PowerSimulations Problem Variables Results
# ┌───────────────────────────────────────────────────────┐
# │ StorageEnergyShortageVariable__EnergyReservoirStorage │
# │ ActivePowerVariable__ThermalStandard                  │
# │ FlowActivePowerVariable__Line                         │
# │ EnergyVariable__EnergyReservoirStorage                │
# │ OnVariable__ThermalStandard                           │
# │ SystemBalanceSlackDown__ACBus                         │
# │ StorageEnergySurplusVariable__EnergyReservoirStorage  │
# │ StartVariable__ThermalStandard                        │
# │ ReservationVariable__EnergyReservoirStorage           │
# │ SystemBalanceSlackUp__ACBus                           │
# │ ActivePowerInVariable__EnergyReservoirStorage         │
# │ ActivePowerVariable__RenewableDispatch                │
# │ StopVariable__ThermalStandard                         │
# │ ActivePowerOutVariable__EnergyReservoirStorage        │
# └───────────────────────────────────────────────────────┘

# Use:
# read_aux_variable: PowerSimulations Problem Auxiliary variables Results
# read_expression: PowerSimulations Problem Expressions Results
# read_dual: PowerSimulations Problem Duals Results
# read_parameter: PowerSimulations Problem Parameters Results
# read_variable: PowerSimulations Problem Variables Results

# Example to fetch and sort results by columns for each results type:
# sort_res_cols(read_aux_variable(res, "TimeDurationOn__ThermalStandard"))
# sort_res_cols(read_aux_variable(res, "TimeDurationOff__ThermalStandard"))
# sort_res_cols(read_aux_variable(res, "StorageEnergyOutput__EnergyReservoirStorage"))

# sort_res_cols(read_expression(res, "ProductionCostExpression__RenewableDispatch"))
# sort_res_cols(read_expression(res, "ProductionCostExpression__ThermalStandard"))
# sort_res_cols(read_expression(res, "ActivePowerBalance__ACBus"))

# sort_res_cols(read_parameter(res, "ActivePowerTimeSeriesParameter__RenewableNonDispatch"))
# sort_res_cols(read_parameter(res, "ActivePowerTimeSeriesParameter__PowerLoad"))
# sort_res_cols(read_parameter(res, "ActivePowerTimeSeriesParameter__RenewableDispatch"))

# sort_res_cols(read_variable(res, "StorageEnergyShortageVariable__EnergyReservoirStorage"))
# sort_res_cols(read_variable(res, "ActivePowerVariable__ThermalStandard"))
# sort_res_cols(read_variable(res, "FlowActivePowerVariable__Line"))
# sort_res_cols(read_variable(res, "EnergyVariable__EnergyReservoirStorage"))
# sort_res_cols(read_variable(res, "OnVariable__ThermalStandard"))
# sort_res_cols(read_variable(res, "SystemBalanceSlackDown__ACBus"))
# sort_res_cols(read_variable(res, "StorageEnergySurplusVariable__EnergyReservoirStorage"))
# sort_res_cols(read_variable(res, "StartVariable__ThermalStandard"))
# sort_res_cols(read_variable(res, "ReservationVariable__EnergyReservoirStorage"))
# sort_res_cols(read_variable(res, "SystemBalanceSlackUp__ACBus"))
# sort_res_cols(read_variable(res, "ActivePowerInVariable__EnergyReservoirStorage"))
# sort_res_cols(read_variable(res, "ActivePowerVariable__RenewableDispatch"))
# sort_res_cols(read_variable(res, "StopVariable__ThermalStandard"))
# sort_res_cols(read_variable(res, "ActivePowerOutVariable__EnergyReservoirStorage"))

# Create structured dictionary to store all results
dfs_res = Dict(
    "aux_variable" => Dict{String, Any}(),
    "expression" => Dict{String, Any}(),
    "parameter" => Dict{String, Any}(),
    "variable" => Dict{String, Any}()
)

# Store auxiliary variables
aux_vars = [
    "TimeDurationOn__ThermalStandard",
    "TimeDurationOff__ThermalStandard",
    "StorageEnergyOutput__EnergyReservoirStorage"
]
for var in aux_vars
    dfs_res["aux_variable"][var] = sort_res_cols(read_aux_variable(res, var))
end

# Store expressions
expressions = [
    "ProductionCostExpression__RenewableDispatch",
    "ProductionCostExpression__ThermalStandard",
    "ActivePowerBalance__ACBus"
]
for expr in expressions
    dfs_res["expression"][expr] = sort_res_cols(read_expression(res, expr))
end

# Store parameters
parameters = [
    "ActivePowerTimeSeriesParameter__RenewableNonDispatch",
    "ActivePowerTimeSeriesParameter__PowerLoad",
    "ActivePowerTimeSeriesParameter__RenewableDispatch"
]
for param in parameters
    dfs_res["parameter"][param] = sort_res_cols(read_parameter(res, param))
end

# Store variables
variables = [
    "StorageEnergyShortageVariable__EnergyReservoirStorage",
    "ActivePowerVariable__ThermalStandard",
    "FlowActivePowerVariable__Line",
    "EnergyVariable__EnergyReservoirStorage",
    "OnVariable__ThermalStandard",
    "SystemBalanceSlackDown__ACBus",
    "StorageEnergySurplusVariable__EnergyReservoirStorage",
    "StartVariable__ThermalStandard",
    "ReservationVariable__EnergyReservoirStorage",
    "SystemBalanceSlackUp__ACBus",
    "ActivePowerInVariable__EnergyReservoirStorage",
    "ActivePowerVariable__RenewableDispatch",
    "StopVariable__ThermalStandard",
    "ActivePowerOutVariable__EnergyReservoirStorage"
]
for var in variables
    dfs_res["variable"][var] = sort_res_cols(read_variable(res, var))
end

# Access examples:
# dfs_res["aux_variable"]["TimeDurationOn__ThermalStandard"]
# dfs_res["expression"]["ProductionCostExpression__ThermalStandard"]
# dfs_res["parameter"]["ActivePowerTimeSeriesParameter__PowerLoad"]
# dfs_res["variable"]["ActivePowerVariable__ThermalStandard"]
