# NOTE:
#   This function accept additional keyword arguments to pass to `DecisionModel`]
# constructor.
simulation_steps = 2
results = SiennaNEM.run_simulation(
    template_uc, sys;
    simulation_folder="examples/result/simulation_folder",
    simulation_name="$(schedule_name)_scenario-$(scenario)",
    simulation_steps=simulation_steps,
    decision_model_kwargs=(
        optimizer=solver,
    ),
)
