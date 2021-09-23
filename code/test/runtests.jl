using SafeTestsets

@safetestset "Period construction" begin include("period_construction.jl") end
@safetestset "Peak calculation" begin include("peak_calculation.jl") end