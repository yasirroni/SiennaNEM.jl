# NOTE:
#   This function accept additional keyword arguments to pass to `DecisionModel`]
# constructor.
res = run_decision_model(
    template_uc, sys;
    schedule_horizon=Hour(24),  # time horizon for the decision model
    initial_time=DateTime("2025-01-08T00:00:00"),  # initial time for the decision model
    optimizer=solver,  # optimizer for the decision model
)
