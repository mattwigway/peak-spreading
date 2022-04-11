using Test
# SafeTestsets would be preferable

@testset "Period construction" begin include("period_construction.jl") end
@testset "Peak calculation" begin include("peak_calculation.jl") end
@testset "Volume delay functions" begin include("vdf.jl") end