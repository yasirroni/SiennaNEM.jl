ids_gen = unique(data["generator_pmax_ts"][!, "id_gen"])
cols = [:fuel, :tech, :type, :DataType, :ThermalFuels]
data["generator"][ids_gen, cols]
