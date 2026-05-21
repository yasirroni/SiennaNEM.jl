using PISP

# Set parameters (see all parameters below)
reference_trace = 2017  # Use 4006 for the reference trace of the ODP
poe             = 10    # Probability of exceedance (POE) for demand
target_years    = [2040]

# Generate PISP datasets for `target_years`, based on the reference trace 4006 (ODP of the 2024 ISP) for 10% POE demand
PISP.build_ISP24_datasets(
    downloadpath = joinpath(@__DIR__, "..", "data", "pisp-downloads"),
    poe          = poe,
    reftrace     = reference_trace,
    drange       = [("09-02-2040", "11-02-2040")],  # peak heat-wave scenario
    output_root  = joinpath(@__DIR__, "..", "data", "pisp-datasets"),
    write_csv    = true,
    write_arrow  = true,
    scenarios    = [2])
