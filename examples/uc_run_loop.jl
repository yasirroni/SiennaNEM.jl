# NOTE:
#   This function accept additional keyword arguments to pass to `DecisionModel`]
# constructor.
results = SiennaNEM.run_decision_model_loop(
    template_uc, sys;
    simulation_folder="examples/result/simulation_folder",
    simulation_name="$(schedule_name)_scenario-$(scenario_name)",
    decision_model_kwargs=(
        optimizer=solver,
    ),
)
