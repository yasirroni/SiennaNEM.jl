using Revise
using SiennaNEM

using PowerSystems
using PowerSimulations
using HydroPowerSimulations
using StorageSystemsSimulations
using HiGHS
using Dates

using PowerModels

data_dir = "data/nem12"

data = read_data_csv(data_dir)
sys = create_system!(data)
add_ts!(sys, data, scenario_name=1)

template_uc = ProblemTemplate()
set_device_model!(template_uc, Line, StaticBranch)
set_device_model!(template_uc, ThermalStandard, ThermalStandardUnitCommitment)
set_device_model!(template_uc, RenewableDispatch, RenewableFullDispatch)
set_device_model!(template_uc, RenewableNonDispatch, FixedOutput)
set_device_model!(template_uc, PowerLoad, StaticPowerLoad)
storage_model = DeviceModel(
    EnergyReservoirStorage,
    StorageDispatchWithReserves;
    attributes=Dict(
        "reservation" => true,
        "energy_target" => true,
        "cycling_limits" => false,
        "regularization" => false,
    ),
    use_slacks=false,
)
set_device_model!(template_uc, storage_model)
set_network_model!(
    template_uc,
    NetworkModel(
        # CopperPlatePowerModel,
        # PTDFPowerModel,
        NFAPowerModel,
        use_slacks = true,
    ),
)

hours = Hour(24)
solver = optimizer_with_attributes(HiGHS.Optimizer, "mip_rel_gap" => 0.5)
problem = DecisionModel(template_uc, sys; optimizer=solver, horizon=hours)
build!(problem; output_dir=mktempdir())
solve!(problem)
res = OptimizationProblemResults(problem)
