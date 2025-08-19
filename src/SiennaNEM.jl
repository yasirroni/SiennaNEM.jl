module SiennaNEM

using PowerSystems
using DataFrames
using CSV

const PSY = PowerSystems
const DF = DataFrames

include("const.jl")
include("read_data.jl")
include("create_system.jl")
include("utils.jl")
include("add_ts.jl")

# Environment variables for configuration
const ENV_HYDRORES_AS_THERMAL = true
const ENV_HYDROPUMP_AS_BATTERY = true

# Exported functions and constants
export preprocess_date!, read_data_csv
export create_system!, add_ts!
export type_to_primemover, type_to_datatype, type_to_fuel
export get_flat_generators, get_generator_units, count_all_generators
export groupbyd, groupby_scenario_at_init_day, groupby_scenario_at_day
export add_sts!, add_st!
export show_parameter
export sort_cols, sort_nested_cols
export diff_df

end