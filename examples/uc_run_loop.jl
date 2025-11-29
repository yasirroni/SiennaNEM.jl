# NOTE:
#   This function accept additional keyword arguments to pass to `DecisionModel`]
# constructor.
results = run_decision_model_loop(
    template_uc, sys;
    schedule_horizon=Hour(24),
    window_shift=Hour(24),
    optimizer=solver,
)
