module SiennaNEM

using PowerSystems
using DataFrames
using CSV
using Arrow

const PSY = PowerSystems
const DF = DataFrames

include("const.jl")
include("read_data.jl")
include("create_system.jl")
include("utils.jl")
include("add_ts.jl")
include("problem/uc.jl")
include("dev/forward_fill_sched.jl")

# Environment variables for configuration
const ENV_HYDRORES_AS_THERMAL = true
const ENV_HYDROPUMP_AS_BATTERY = true

# Exported functions and constants
export read_system_data, read_ts_data!, preprocess_date!, update_system_data_bound!
export create_system!, add_ts!
export tech_to_primemover, tech_to_datatype, tech_to_fuel
export get_flat_generators, get_generator_units, count_all_generators
export groupbyd, groupby_scenario_at_init_day, groupby_scenario_at_day
export add_sts!, add_st!
export diff_df, show_parameter, sort_res_cols
export get_full_ts_df, add_tsf_data!

export build_problem_base_uc

end