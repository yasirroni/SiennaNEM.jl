using HydroPowerSimulations
using StorageSystemsSimulations
using PowerModels


function build_problem_base_uc()
    template_uc = ProblemTemplate()
    set_device_model!(template_uc, Line, StaticBranch)
    set_device_model!(template_uc, ThermalStandard, ThermalStandardUnitCommitment)
    set_device_model!(template_uc, RenewableDispatch, RenewableFullDispatch)
    set_device_model!(template_uc, RenewableNonDispatch, FixedOutput)
    set_device_model!(template_uc, PowerLoad, StaticPowerLoad)

    #   Warning: Currently, there is a bug in Sienna that prevents the number of
    # batteries to be higher than time horizon. Since there is about 50 active
    # batteries in the NEM system model, we use `horizon = Hour(72)` to make sure
    # the time horizon is bigger than the number of batteries.
    # See: https://github.com/NREL-Sienna/StorageSystemsSimulations.jl/issues/76

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
    return template_uc
end
