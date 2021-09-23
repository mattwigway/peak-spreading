# Automated tests

using Dates
using Test
include("../KFactors.jl")
using .KFactors

@testset "Period construction" begin
    @testset "Period construction for 2021" begin
        @test all(KFactors.period_for_year(2021) .== [Date(2021, 06, 15), Date(2021, 07, 17)])
    end

    @testset "Period construction for $y" for y in 2016:2020
        # make sure 
        s, e = KFactors.period_for_year(y)
        @test e - s == Dates.Day(32)
        # make sure it's the third Tuesday in June
        @test Dates.month(s) == Dates.June
        @test Dates.year(s) == y
        @test Dates.dayofweek(s) == Dates.Tuesday
        @test Dates.day(s) >= 15
        @test Dates.day(s) <= 21
    end
end