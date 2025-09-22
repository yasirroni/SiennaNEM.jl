using Test
using SiennaNEM

@testset "SiennaNEM Tests" begin
    include("test_create_system.jl")
    include("test_forward_fill.jl")
end