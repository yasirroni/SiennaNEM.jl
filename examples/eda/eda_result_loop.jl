aux_vars = [
    "TimeDurationOn__ThermalStandard",
    "TimeDurationOff__ThermalStandard",
    "StorageEnergyOutput__EnergyReservoirStorage"
]
expressions = [
    "ProductionCostExpression__RenewableDispatch",
    "ProductionCostExpression__ThermalStandard",
    "ActivePowerBalance__ACBus"
]
parameters = [
    "ActivePowerTimeSeriesParameter__RenewableNonDispatch",
    "ActivePowerTimeSeriesParameter__PowerLoad",
    "ActivePowerTimeSeriesParameter__RenewableDispatch"
]

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

dfs_res = Dict(
    "aux_variable" => Dict{String, Any}(),
    "expression" => Dict{String, Any}(),
    "parameter" => Dict{String, Any}(),
    "variable" => Dict{String, Any}()
)
for (time_slice, res) in res_dict
    for expr in expressions
        df = sort_res_cols(read_expression(res, expr))
        if haskey(dfs_res["expression"], expr)
            dfs_res["expression"][expr] = vcat(dfs_res["expression"][expr], df)
        else
            dfs_res["expression"][expr] = df
        end
    end
    
    for var in aux_vars
        df = sort_res_cols(read_aux_variable(res, var))
        if haskey(dfs_res["aux_variable"], var)
            dfs_res["aux_variable"][var] = vcat(dfs_res["aux_variable"][var], df)
        else
            dfs_res["aux_variable"][var] = df
        end
    end
    
    for param in parameters
        df = sort_res_cols(read_parameter(res, param))
        if haskey(dfs_res["parameter"], param)
            dfs_res["parameter"][param] = vcat(dfs_res["parameter"][param], df)
        else
            dfs_res["parameter"][param] = df
        end
    end
    
    for var in variables
        df = sort_res_cols(read_variable(res, var))
        if haskey(dfs_res["variable"], var)
            dfs_res["variable"][var] = vcat(dfs_res["variable"][var], df)
        else
            dfs_res["variable"][var] = df
        end
    end
end

# Export all dataframes to CSV
using CSV

output_dir = "examples/result/nem12/csv"
mkpath(output_dir)  # Create directory if it doesn't exist

for (category, dfs) in dfs_res
    for (name, df) in dfs
        filename = "$(schedule_name)_$(category)_$(name).csv"
        filepath = joinpath(output_dir, filename)
        CSV.write(filepath, df)
        println("Exported: $filepath")
    end
end
