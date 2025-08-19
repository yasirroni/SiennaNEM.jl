using Test
using SiennaNEM

@testset "SiennaNEM Tests" begin
    include("test_read_data.jl")
    include("test_create_system.jl")
end