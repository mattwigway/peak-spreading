include("../KFactorPeaks.jl")
using .KFactorPeaks
using StatsBase
using Test
using Dates
using Random

function normalize(v)
    return v ./ sum(v)
end

normentropy = entropy ∘ normalize

@testset "Entropy" begin
    # sanity check
    @test KFactorPeaks.occupancy_entropy([1, 0, 0]) ≈ 0
    @test KFactorPeaks.occupancy_entropy([0, 0, 0]) ≈ 0

    # now check against library for more complicated stuff
    # should match library when nonmissing
    @test KFactorPeaks.occupancy_entropy([2, 3, 5]) ≈ normentropy([2, 3, 5])

    @test_throws AssertionError KFactorPeaks.occupancy_entropy([-1, -2, -3])
    @test_throws AssertionError KFactorPeaks.occupancy_entropy([-1, 1])  # sums to 0, but not all 0


    @test ismissing(KFactorPeaks.occupancy_entropy([missing, 1, 2]))
end

@testset "K-factor" begin
    times = Dates.Minute.(0:5:(24 * 60 - 1)) .+ Dates.Time(0, 0, 0)
    occ = ones(Float64, length(times)) ./ 100
    # peak hour is 7 - 8 am
    # + 1 - one based indexing
    occ[(7 * 60 ÷ 5 + 1):(8 * 60 ÷ 5)] .= 0.05

    res = KFactorPeaks.peak_hour_factor_binary(times, occ)
    @test res.peak_hour_start == Time(7, 0, 0)
    # one hour of 0.05 in 5-minute increments, over 23 hours of 0.01 and 1 hour of 0.05
    expected_peak_occ = (0.05 * 60 / 5) / sum(occ)
    @test res.peak_hour_occ ≈ expected_peak_occ

    # now, test out of order - function should use times to put back in order
    rng = MersenneTwister(27599)
    new_order = collect(1:length(times))
    shuffle!(rng, new_order)
    shuffled_time = times[new_order]
    shuffled_occ = occ[new_order]
    orig_times = copy(shuffled_time)
    orig_occ = copy(shuffled_occ)
    res = KFactorPeaks.peak_hour_factor_binary(shuffled_time, shuffled_occ)
    # result should be the same
    @test res.peak_hour_start == Time(7, 0, 0)
    # one hour of 0.05 in 5-minute increments, over 23 hours of 0.01 and 1 hour of 0.05
    @test res.peak_hour_occ ≈ expected_peak_occ

    # original arrays should not be modified
    @test all(orig_times .== shuffled_time)
    @test all(orig_occ .== shuffled_occ)

    # if anything is missing, should return missing
    miss_time = convert(Vector{Union{Dates.Time, Missing}}, copy(times))
    miss_time[42] = missing
    miss_occ = convert(Vector{Union{Float64, Missing}}, copy(occ))
    miss_occ[42] = missing

    res = KFactorPeaks.peak_hour_factor_binary(miss_time, occ)
    @test ismissing(res.peak_hour_start)
    @test ismissing(res.peak_hour_occ)

    res = KFactorPeaks.peak_hour_factor_binary(time, miss_occ)
    @test ismissing(res.peak_hour_start)
    @test ismissing(res.peak_hour_occ)

    res = KFactorPeaks.peak_hour_factor_binary(miss_time, miss_occ)
    @test ismissing(res.peak_hour_start)
    @test ismissing(res.peak_hour_occ)
end

@testset "Periods imputed" begin
    @test KFactorPeaks.periods_imputed([0, 100, 50]) == 2
    @test ismissing(KFactorPeaks.periods_imputed([0, 50, missing]))
end

@testset "Longest imputed time" begin
    times = Dates.Minute.(0:5:(24 * 60 - 1)) .+ Dates.Time(0, 0, 0)
    pct_obs = convert(Vector{Union{Int64, Missing}}, fill(100, 288))

    @test KFactorPeaks.longest_imputed_time(times, pct_obs) == 0

    pct_obs[22:33] .= 50 # 12 consecutive periods, i.e. 60 minutes, unobserved
    pct_obs[55:57] .= 50 # should affect nothing, not the longest period
    @test KFactorPeaks.longest_imputed_time(times, pct_obs) == 60

    # make sure it works when shuffled
    rng = MersenneTwister(85287)
    new_order = collect(1:length(times))
    shuffle!(rng, new_order)
    shuffled_times = times[new_order]
    shuffled_obs = pct_obs[new_order]
    orig_times = copy(shuffled_times)
    orig_obs = copy(shuffled_obs)

    @test KFactorPeaks.longest_imputed_time(shuffled_times, shuffled_obs) == 60
    @test all(shuffled_times .== orig_times)
    @test all(shuffled_obs .== orig_obs)

    # should handle missings
    @test ismissing(KFactorPeaks.longest_imputed_time([Dates.Time(12, 15, 0), missing], [1, 2]))
    @test ismissing(KFactorPeaks.longest_imputed_time([Dates.Time(12, 15, 0), Dates.Time(15, 0, 0)], [1, missing]))

    # break a test to make sure the CI works
    @test false
end