using Test
# SafeTestsets would be preferable

@testset "Period construction" begin include("period_construction.jl") end
@testset "Peak calculation" begin include("peak_calculation.jl") end
@testset "Time for index" begin include("array_indices.jl") end
@testset "Permutation test" begin include("permutation.jl") end
@testset "Fundamental diagrams" begin include("fundamental_diagrams.jl") end