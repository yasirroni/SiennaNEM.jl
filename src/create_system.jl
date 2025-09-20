# Get all generators of a specific type as a flat dictionary
function get_flat_generators(nested_dict)
    flat_dict = Dict{String,Any}()
    for (id, units) in nested_dict
        for (unit_num, gen) in units
            flat_dict[get_name(gen)] = gen
        end
    end
    return flat_dict
end

# Get all units for a specific generator ID
function get_generator_units(nested_dict, id_gen)
    return get(nested_dict, id_gen, Dict{Int,Any}())
end

# Get total count of all generators
function count_all_generators(nested_dict)
    return sum(length(units) for units in values(nested_dict))
end

function create_system!(data)
    df_bus = data["bus"]
    df_generator = data["generator"]
    df_line = data["line"]
    df_demand = data["demand"]
    df_storage = data["storage"]

    df_demand_l_ts = data["demand_l_ts"]
    df_generator_pmax_ts = data["generator_pmax_ts"]
    df_generator_n_ts = data["generator_n_ts"]
    df_der_p_ts = data["der_p_ts"]
    df_storage_emax_ts = data["storage_emax_ts"]
    df_storage_lmax_ts = data["storage_lmax_ts"]
    df_storage_n_ts = data["storage_n_ts"]
    df_storage_pmax_ts = data["storage_pmax_ts"]
    df_line_tmax_ts = data["line_tmax_ts"]
    df_line_tmin_ts = data["line_tmin_ts"]

    df_generator_ts = data["generator_pmax_ts"]
    time_unit = (T=Hour, L=1)
    start_dt = DateTime("2044-07-01T00:00:00")
    end_dt = DateTime("2044-07-01T00:00:00")

    baseMVA = 100
    sys = PSY.System(baseMVA)
    set_units_base_system!(sys, "SYSTEM_BASE") # for p.u.

    # Make buses dict
    areas = Dict{Int,PSY.Area}()
    buses = Dict{Int,PSY.ACBus}()
    for row in eachrow(df_bus)
        id = row.id_bus
        name = string(id)
        area = PSY.Area(
            name=name,
            peak_active_power=0.0,
            peak_reactive_power=0.0,
            load_response=0.0,  # support load-frequency damping
        )
        bus = PSY.ACBus(;
            number=id,
            name=name,
            bustype=ACBusTypes.PV,
            angle=0.0,
            magnitude=1.0,
            voltage_limits=(min=0.9, max=1.05),
            base_voltage=220.0,
            area=area,
            ext=Dict(
                "latitude"=>row.latitude,
                "longitude"=>row.longitude,
            ),
        )
        areas[id] = area
        buses[id] = bus
        add_component!(sys, area)
        add_component!(sys, bus)
    end

    # Make lines and arcs
    # !WARNING: df_line.name is not unique, thus we use ID instead of name
    # !WARNING: multiple parallel lines have different capacity with same r and x
    lines = Dict{Int,PSY.Line}()
    for row in eachrow(df_line)
        # !WARNING line 38 CNSW-SNW Option 2d has capacity of 0, we skip it
        if row.capacity == 0
            continue
        end

        # skip investment line for now
        if row.investment == 1
            # TODO: dynamically change available based on investment?
            continue
        end

        # NOTE: innactive lines is properly excluded by PowerModels.

        id = row.id_lin
        line = PSY.Line(;
            name=string(id),
            available=row.active,
            active_power_flow=0.0,
            reactive_power_flow=0.0,
            arc=PSY.Arc(;
                from=buses[row["id_bus_from"]],
                to=buses[row["id_bus_to"]]
            ),
            r=row.r,
            x=row.x,
            b=(from=0.0, to=0.0),
            rating=row.capacity / baseMVA,
            angle_limits=(min=-pi/2, max=pi/2),
        )
        lines[id] = line
        add_component!(sys, line)
    end

    # Add generators
    #   See:
    #       PowerSystems.jl/src/models/generated for types of generator
    #       PowerSystems.jl/src/definitions.jl for prime_mover_type and ThermalFuels
    #       https://nrel-sienna.github.io/PowerSystems.jl/stable/ in model_library/
    # TODO:
    #   Support n that able to change using _orig_Generator_n_sched.csv
    #   Use FuelCurve to support fuel cost changes scenario
    #   Add time series
    #   No reactive capability data > same as capacity for now

    # create generators - nested structure to keep units grouped by original ID
    generators = Dict{Int,Dict{Int,PSY.Generator}}()
    thermal_generators = Dict{Int,Dict{Int,PSY.ThermalStandard}}()
    renewable_dispatch_generators = Dict{Int,Dict{Int,PSY.RenewableDispatch}}()
    renewable_nondispatch_generators = Dict{Int,Dict{Int,PSY.RenewableNonDispatch}}()

    if !ENV_HYDRORES_AS_THERMAL
        hydro_dispatch_generators = Dict{Int,Dict{Int,PSY.HydroDispatch}}()
        hydro_energyreservoir_generators = Dict{Int,Dict{Int,PSY.HydroEnergyReservoir}}()
    else
        hydro_dispatch_generators = Dict{Int,Dict{Int,PSY.ThermalStandard}}()
        hydro_energyreservoir_generators = Dict{Int,Dict{Int,PSY.ThermalStandard}}()
    end

    # NOTE:
    #   In initial year (not looking at future _orig_Generator_n_sched), only
    # ThermalStandard, HydroDispatch, and HydroEnergyReservoir has n > 1.
    # 
    #   unique(df_generator[df_generator.n .> 1, :DataType])
    #   3-element Vector{DataType}:
    #    ThermalStandard
    #    HydroDispatch
    #    HydroEnergyReservoir

    # NOTE:
    #   1. RenewableDispatch and RenewableNonDispatch is free (cvar == 0)
    #   2. ThermalStandard only single linear curve

    for row in eachrow(df_generator)
        if row.DataType === missing || row.capacity == 0
            # NOTE:
            # To print data that is skipped:
            #
            #   println(id, ": ", row.DataType, " ", row.capacity)
            continue
        end

        # Initialize nested dictionaries for this generator ID
        id = row.id_gen
        generators[id] = Dict{Int,PSY.Generator}()

        if row.DataType == ThermalStandard
            thermal_generators[id] = Dict{Int,PSY.ThermalStandard}()

            for i in 1:row.n
                name = string(id, "_", i)
                gen = PSY.ThermalStandard(;
                    name=name,
                    available=row.active,
                    status=true,
                    bus=buses[row.id_bus],
                    active_power=0,
                    reactive_power=0,
                    rating=1,
                    active_power_limits=(min=row.pmin / row.pmax, max=1),
                    reactive_power_limits=(min=-1, max=1),  # same as capacity for now
                    ramp_limits=(up=row.rdw / row.pmax, down=row.rup / row.pmax),
                    operation_cost=ThermalGenerationCost(
                        variable=CostCurve(;  # Sienna support FuelCurve with fuel_cost
                            value_curve=LinearCurve(row.cvar),
                        ),
                        fixed = 0.0,
                        start_up = 0.0,
                        shut_down = 0.0,
                    ),
                    base_power=row.pmax, # MVA
                    time_limits=nothing, # MUT MDT, if in Hours: (up = 8.0, down = 8.0)
                    must_run=false,
                    prime_mover_type=row.PrimeMovers,
                    fuel=row.ThermalFuels,
                )
                generators[id][i] = gen
                thermal_generators[id][i] = gen
                add_component!(sys, gen)
            end

        elseif row.DataType == HydroDispatch
            if !ENV_HYDRORES_AS_THERMAL
                hydro_dispatch_generators[id] = Dict{Int,PSY.HydroDispatch}()
                for i in 1:row.n
                    name = string(id, "_", i)
                    gen = HydroDispatch(;
                        name=name,
                        available=row.active,
                        bus=buses[row.id_bus],
                        active_power=0,
                        reactive_power=0,
                        rating=1,
                        active_power_limits=(min=row.pmin / row.pmax, max=1),
                        reactive_power_limits=(min=-1, max=1),  # same as capacity for now
                        ramp_limits=(up=row.rdw / row.pmax, down=row.rup / row.pmax),
                        operation_cost=HydroGenerationCost(nothing),
                        base_power=row.pmax, # MVA
                        time_limits=nothing, # MUT MDT, if in Hours: (up = 8.0, down = 8.0)
                        prime_mover_type=row.PrimeMovers,
                    )
                    generators[id][i] = gen
                    hydro_dispatch_generators[id][i] = gen
                    add_component!(sys, gen)
                end
            else
                hydro_dispatch_generators[id] = Dict{Int,PSY.ThermalStandard}()
                for i in 1:row.n
                    name = string(id, "_", i)
                    gen = PSY.ThermalStandard(;
                        name=name,
                        available=row.active,
                        status=true,
                        bus=buses[row.id_bus],
                        active_power=0,
                        reactive_power=0,
                        rating=1,
                        active_power_limits=(min=row.pmin / row.pmax, max=1),
                        reactive_power_limits=(min=-1, max=1),  # same as capacity for now
                        ramp_limits=(up=row.rdw / row.pmax, down=row.rup / row.pmax),
                        operation_cost=ThermalGenerationCost(
                            variable=CostCurve(;  # Sienna support FuelCurve with fuel_cost
                                value_curve=LinearCurve(row.cvar),
                            ),
                            fixed = 0.0,
                            start_up = 0.0,
                            shut_down = 0.0,
                        ),
                        base_power=row.pmax, # MVA
                        time_limits=nothing, # MUT MDT, if in Hours: (up = 8.0, down = 8.0)
                        must_run=false,
                        prime_mover_type=PrimeMovers.GT,  # Gas Turbine to show fast ramp
                        fuel=ThermalFuels.OTHER,  # other, using water
                    )
                    generators[id][i] = gen
                    hydro_dispatch_generators[id][i] = gen
                    add_component!(sys, gen)
                end
            end

        elseif row.DataType == HydroEnergyReservoir
            if !ENV_HYDRORES_AS_THERMAL
                # TODO: support HydroEnergyReservoir
                continue
            else
                hydro_energyreservoir_generators[id] = Dict{Int,PSY.ThermalStandard}()
                for i in 1:row.n
                    name = string(id, "_", i)
                    gen = PSY.ThermalStandard(;
                        name=name,
                        available=row.active,
                        status=true,
                        bus=buses[row.id_bus],
                        active_power=0,
                        reactive_power=0,
                        rating=1,
                        active_power_limits=(min=row.pmin / row.pmax, max=1),
                        reactive_power_limits=(min=-1, max=1),  # same as capacity for now
                        ramp_limits=(up=row.rdw / row.pmax, down=row.rup / row.pmax),
                        operation_cost=ThermalGenerationCost(
                            variable=CostCurve(;  # Sienna support FuelCurve with fuel_cost
                                value_curve=LinearCurve(row.cvar),
                            ),
                            fixed = 0.0,
                            start_up = 0.0,
                            shut_down = 0.0,
                        ),
                        base_power=row.pmax, # MVA
                        time_limits=nothing, # MUT MDT, if in Hours: (up = 8.0, down = 8.0)
                        must_run=false,
                        prime_mover_type=PrimeMovers.GT,  # Gas Turbine to show fast ramp
                        fuel=ThermalFuels.OTHER,  # other, using water
                    )
                    generators[id][i] = gen
                    hydro_energyreservoir_generators[id][i] = gen
                    add_component!(sys, gen)
                end
            end

        elseif row.DataType == RenewableDispatch
            renewable_dispatch_generators[id] = Dict{Int,PSY.RenewableDispatch}()

            for i in 1:row.n
                name = string(id, "_", i)
                gen = RenewableDispatch(;
                    name=name,
                    available=row.active,
                    bus=buses[row.id_bus],
                    active_power=0,
                    reactive_power=0,
                    rating=1,
                    prime_mover_type=row.PrimeMovers,
                    reactive_power_limits=(min=0, max=1),  # same as capacity for now
                    power_factor=1.0,
                    operation_cost=RenewableGenerationCost(nothing),
                    base_power=row.pmax,
                )
                generators[id][i] = gen
                renewable_dispatch_generators[id][i] = gen
                add_component!(sys, gen)
            end

        elseif row.DataType == RenewableNonDispatch
            renewable_nondispatch_generators[id] = Dict{Int,PSY.RenewableNonDispatch}()

            for i in 1:row.n
                name = string(id, "_", i)
                gen = RenewableNonDispatch(;
                    name=name,
                    available=row.active,
                    bus=buses[row.id_bus],
                    active_power=0,
                    reactive_power=0,
                    rating=1,
                    prime_mover_type=row.PrimeMovers,
                    power_factor=1.0,
                    base_power=row.pmax,
                )
                generators[id][i] = gen
                renewable_nondispatch_generators[id][i] = gen
                add_component!(sys, gen)
            end
        end
    end

    # TODO:
    #   Check the behavior of PowerLoad and StandardLoad as StandardLoad is used for
    # dynamic simulation.
    demands = Dict{Int,PowerLoad}()
    for row in eachrow(df_demand)
        if row.controllable == 1
            continue  # skip controllable load for now
        end
        id = row.id_dem
        demand = PowerLoad(;
            name=string(id),
            available=row.active,
            bus=buses[row.id_bus],
            active_power=0, # Per-unitized by device base_power
            reactive_power=0, # Per-unitized by device base_power
            base_power=baseMVA, # MVA
            max_active_power=100000.0, # per-unitized by device base_power
            max_reactive_power=100000.0,
        )
        demands[id] = demand
        add_component!(sys, demand)
    end

    # Add battery
    # I don't know what is the best for battery modelling, including dynamics.
    # 
    #   https://nrel-sienna.github.io/PowerSystems.jl/stable/model_library/generated_EnergyReservoirStorage/
    #   https://nrel-sienna.github.io/PowerSystems.jl/stable/model_library/generated_DCSource/#ZeroOrderBESS
    # 
    # Currently, use EnergyReservoirStorage.

    # TODO:
    #   In bench result, what is:
    # bpl: (ignore it)
    # bl: charge - battery load
    # bp: disharge - battery power generation
    # chdis: discharge status
    # se: SoE
    # bffrup: fast
    # bpfrup: primary
    # bres2up: secondary
    # bres3up: tertiary reserve - offline generator
    # bffrdw: 
    # bpfrdw: 
    # bres2dw: 
    # bres3dw: 
    # 
    #   In bench result, there is a data (id: 60) that bl and bp at the same time.
    # 
    #   In data:
    #   capacity: charge discharge capacity (base of the inverter)
    #   emax: storage energy capacity
    #   technology, PS: Pump Storage. TODO: PrimeMovers.PS
    #   type:
    #       SHALLOW
    #       MEDIUM
    #       DEEP
    #   lmax: load (charging) max.
    #   pmax: power (discharge) max.
    #   power_factor of ps:
    #   ffr: max ancilary services fast (5 s)
    #   pfr: max ancilary services fast (1 minute)
    #   res2 and res3: secondary (spinning reserve)
    # 
    #   Sienna support cycle_limits
    # 
    # Battery Types:
    #   https://nrel-sienna.github.io/PowerSystems.jl/stable/api/enumerated_types/#storagetech_list

    # TODO: in per unit systems, understand how they works
    # TODO: understand how storage_capacity=row.emax
    # TODO: add latitude and longitude
    # TODO: inertia
    # TODO: PS should use HydroPumpedStorage
    #   https://nrel-sienna.github.io/PowerSystems.jl/stable/model_library/generated_HydroPumpedStorage/#HydroPumpedStorage

    storages = Dict{Int,Dict{Int,Union{PSY.EnergyReservoirStorage,PSY.HydroPumpedStorage}}}()
    battery_storages = Dict{Int,Dict{Int,PSY.EnergyReservoirStorage}}()

    if !ENV_HYDROPUMP_AS_BATTERY
        hydro_storages = Dict{Int,Dict{Int,PSY.HydroPumpedStorage}}()
    else
        hydro_storages = Dict{Int,Dict{Int,PSY.EnergyReservoirStorage}}()
    end

    for row in eachrow(df_storage)
        if row.n == 0
            # TODO: support n = 0
            continue  # skip battery investment for now
        end

        storage_capacity = row.emax / row.capacity
        initial_storage_capacity_level = row.eini/100
        input_active_power_limits_max = row.lmax / row.capacity
        output_active_power_limits_max = row.pmax / row.capacity
        storage_target = row.eini/100

        id = row.id_ess
        if row.DataType == EnergyReservoirStorage
            storage_level_limits_min = row.emin/100
            storages[id] = Dict{Int,PSY.EnergyReservoirStorage}()
            battery_storages[id] = Dict{Int,PSY.EnergyReservoirStorage}()
            for i in 1:row.n
                name = string(id, "_", i)
                storage = EnergyReservoirStorage(;
                    name=name,
                    available=row.active,
                    bus=buses[row.id_bus],
                    prime_mover_type=row.PrimeMovers,
                    active_power=0, # Per-unitized by device base_power
                    reactive_power=0, # Per-unitized by device base_power
                    base_power=row.capacity, # MVA
                    storage_technology_type=StorageTech.LIB,
                    storage_capacity=storage_capacity,  # if not in MW, use conversion_factor
                    storage_level_limits=(min=storage_level_limits_min, max=1),
                    initial_storage_capacity_level=initial_storage_capacity_level,
                    rating=1,
                    input_active_power_limits=(min=0, max=input_active_power_limits_max),
                    output_active_power_limits=(min=0, max=output_active_power_limits_max),
                    efficiency=(in=row.ch_eff, out=row.dch_eff),
                    reactive_power_limits=(min=0, max=0),
                    operation_cost=StorageCost(nothing),  # only support based MW charge and discharge
                    storage_target=storage_target,
                )
                storages[id][i] = storage
                battery_storages[id][i] = storage
                add_component!(sys, storage)
            end
        elseif row.DataType == HydroPumpedStorage
            # NOTE:
            #   HydroPumpedStorage didn't support storage_level_limits
            #   HydroPumpedStorage support different data for lower and upper reservoir
            #   HydroPumpedStorage reactive_power_limits_pump, not fixed power factor
            #   HydroPumpedStorage status support three types (OFF, GEN, PUMP)

            # TODO:
            #   Is this correct?
            # 
            #       pump_efficiency=row.ch_eff,
            #       conversion_factor=row.dch_eff,

            if !ENV_HYDROPUMP_AS_BATTERY
                # NOTE: reactive_power_limits_max is used for reactive_power_limits_pump and reactive_power_limits
                reactive_power_limits_max = row.capacity * sqrt((1/row.powerfactor^2) - 1)
                storages[id] = Dict{Int,PSY.HydroPumpedStorage}()
                hydro_storages[id] = Dict{Int,PSY.HydroPumpedStorage}()
                for i in 1:row.n
                    name = string(id, "_", i)
                    storage = HydroPumpedStorage(;
                        name=name,
                        available=row.active,
                        bus=buses[row.id_bus],
                        active_power=0, # Per-unitized by device base_power
                        reactive_power=0, # Per-unitized by device base_power
                        rating=1,
                        base_power=row.capacity, # MVA
                        prime_mover_type=row.PrimeMovers,
                        active_power_limits=(min=0, max=output_active_power_limits_max),
                        reactive_power_limits=(min=0, max=reactive_power_limits_max),
                        ramp_limits=nothing,
                        time_limits=nothing,
                        rating_pump=1,
                        active_power_limits_pump=(min=0, max=input_active_power_limits_max),
                        reactive_power_limits_pump=(min=0, max=reactive_power_limits_max),
                        ramp_limits_pump=nothing,
                        time_limits_pump=nothing,
                        storage_capacity=(up=storage_capacity, down=storage_capacity),
                        inflow=0.0,
                        outflow=0.0,
                        initial_storage=(up=initial_storage_capacity_level, down=1-initial_storage_capacity_level),
                        storage_target=(up=initial_storage_capacity_level, down=1-initial_storage_capacity_level),
                        operation_cost=StorageCost(nothing),  # only support based MW charge and discharge
                        pump_efficiency=row.ch_eff,
                        conversion_factor=row.dch_eff,
                    )
                    storages[id][i] = storage
                    hydro_storages[id][i] = storage
                    add_component!(sys, storage)
                end
            else
                storage_level_limits_min = row.emin/100
                storages[id] = Dict{Int,PSY.EnergyReservoirStorage}()
                hydro_storages[id] = Dict{Int,PSY.EnergyReservoirStorage}()
                for i in 1:row.n
                    name = string(id, "_", i)
                    storage = EnergyReservoirStorage(;
                        name=name,
                        available=row.active,
                        bus=buses[row.id_bus],
                        prime_mover_type=row.PrimeMovers,
                        active_power=0, # Per-unitized by device base_power
                        reactive_power=0, # Per-unitized by device base_power
                        base_power=row.capacity, # MVA
                        storage_technology_type=StorageTech.LIB,
                        storage_capacity=storage_capacity,  # if not in MW, use conversion_factor
                        storage_level_limits=(min=storage_level_limits_min, max=1),
                        initial_storage_capacity_level=initial_storage_capacity_level,
                        rating=1,
                        input_active_power_limits=(min=0, max=input_active_power_limits_max),
                        output_active_power_limits=(min=0, max=output_active_power_limits_max),
                        efficiency=(in=row.ch_eff, out=row.dch_eff),
                        reactive_power_limits=(min=0, max=0),
                        operation_cost=StorageCost(nothing),  # only support based MW charge and discharge
                        storage_target=storage_target,
                    )
                    storages[id][i] = storage
                    hydro_storages[id][i] = storage
                    add_component!(sys, storage)
                end
            end
        end
    end

    # NOTE: BUG in PowerSystems.jl:
    #   Using `set_units_base_system!(sys, "NATURAL_UNITS")` before data creation
    # cause warning. This warning is fixed in latest PowerSystems.jl (that is
    # currently) not yet supported for PowerSimulations.jl
    # 
    # See: https://github.com/NREL-Sienna/PowerSystems.jl/issues/1418
    # set_units_base_system!(sys, "NATURAL_UNITS")  # for MW/MVA
    # set_units_base_system!(sys, "SYSTEM_BASE")  # for p.u.

    # get and set slack_bus
    df_thermalgenerators_active = filter(
        row -> row.DataType == ThermalStandard && row.active == 1, df_generator
    )
    slack_bus = df_thermalgenerators_active[
        argmax(df_thermalgenerators_active[:, :capacity]), :id_bus
    ]
    set_bustype!(buses[slack_bus], ACBusTypes.REF)

    data["baseMVA"] = baseMVA
    data["sys"] = sys
    data["components"] = Dict(
        "areas" => areas,
        "buses" => buses,
        "lines" => lines,
        "generators" => generators,
        "thermal_generators" => thermal_generators,
        "renewable_dispatch_generators" => renewable_dispatch_generators,
        "renewable_nondispatch_generators" => renewable_nondispatch_generators,
        "hydro_dispatch_generators" => hydro_dispatch_generators,
        "demands" => demands,
        "storages" => storages,
        "battery_storages" => battery_storages,
        "hydro_storages" => hydro_storages,
    )
    return sys
end
