using KFactors, StatsBase, Test, Dates, Random, CSV, DataFrames

function normalize(v)
    return v ./ sum(v)
end

function entropy2(p::AbstractArray{T}) where T<:Real
    # Modified directly from the Julia source code
    # https://github.com/JuliaStats/StatsBase.jl/blob/42feefb84aa81d9f8918500ecf159dbee5bcb91f/src/scalarstats.jl#L396-L401
    s = zero(T)
    z = zero(T)
    for i = 1:length(p)
        @inbounds pi = p[i]
        if pi > z
            s += pi * log2(pi)
        end
    end
    return -s
end


normentropy = entropy2 ∘ normalize

@testset "Daytime hours" begin
    # 4:59am is not in daytime
    @test !KFactors.is_in_daytime(Time(4, 59))

    # 5am is in daytime
    @test KFactors.is_in_daytime(Time(5, 0))

    # 7:59pm is in daytime
    @test KFactors.is_in_daytime(Time(19, 59))

    # 8:00pm is not in daytime
    @test !KFactors.is_in_daytime(Time(20, 0))
end

@testset "Entropy" begin
    # sanity check
    @test KFactors.occupancy_entropy([1, 0, 0]) ≈ 0
    @test KFactors.occupancy_entropy([0, 0, 0]) ≈ 0

    # now check against library for more complicated stuff
    # should match library when nonmissing
    @test KFactors.occupancy_entropy([2, 3, 5]) ≈ normentropy([2, 3, 5])

    @test_throws AssertionError KFactors.occupancy_entropy([-1, -2, -3])
    @test_throws AssertionError KFactors.occupancy_entropy([-1, 1])  # sums to 0, but not all 0

    @test ismissing(KFactors.occupancy_entropy([missing, 1, 2]))

    # Check that daytime entropy calculation filters out correctly
    times = [
        Time(4, 59),
        Time(5, 0),
        Time(12, 0),
        Time(19, 59),
        Time(20, 0)
    ]
    
    @test KFactors.occupancy_entropy_daytime(times, [1.0, 2.0, 3.0, 5.0, 6.0]) ≈ normentropy([2.0, 3.0, 5.0])

end

@testset "peak hour factor binary" begin
    times = Dates.Minute.(0:5:(24 * 60 - 1)) .+ Dates.Time(0, 0, 0)
    @test maximum(times) == Dates.Time(23, 55, 0)
    occ = ones(Float64, length(times)) ./ 100
    # peak hour is 7 - 8 am
    # + 1 - one based indexing
    occ[(7 * 60 ÷ 5 + 1):(8 * 60 ÷ 5)] .= 0.05
    flow = ones(Float64, length(times)) .* 2
    # flow is lower at peak (represents oversaturation in the real world, but
    # also makes sure flow calculation is correctly based on occupancy peak).
    flow[(7 * 60 ÷ 5 + 1):(8 * 60 ÷ 5)] .= 1 

    res = KFactors.peak_hour_factor_binary(times, occ, flow)
    @test res.peak_hour_start == Time(7, 0, 0)
    # one hour of 0.05 in 5-minute increments, over 23 hours of 0.01 and 1 hour of 0.05
    expected_peak_occ = (0.05 * 60 / 5) / sum(occ)
    @test res.peak_hour_occ ≈ expected_peak_occ
    # 1 car per period for one hour
    expected_peak_flow = (1 * 60 / 5) / sum(flow)
    @test res.peak_hour_flow ≈ expected_peak_flow

    # now, test out of order - function should use times to put back in order
    rng = MersenneTwister(27599)
    new_order = collect(1:length(times))
    shuffle!(rng, new_order)
    @test !issorted(new_order)
    shuffled_time = times[new_order]
    shuffled_occ = occ[new_order]
    shuffled_flow = flow[new_order]
    orig_times = copy(shuffled_time)
    orig_occ = copy(shuffled_occ)
    orig_flow = copy(shuffled_flow)
    res = KFactors.peak_hour_factor_binary(shuffled_time, shuffled_occ, shuffled_flow)
    # result should be the same
    @test res.peak_hour_start == Time(7, 0, 0)
    # one hour of 0.05 in 5-minute increments, over 23 hours of 0.01 and 1 hour of 0.05
    @test res.peak_hour_occ ≈ expected_peak_occ
    @test res.peak_hour_flow ≈ expected_peak_flow

    # original arrays should not be modified
    @test all(orig_times .== shuffled_time)
    @test all(orig_occ .== shuffled_occ)
    @test all(orig_occ .== shuffled_occ)
    @test all(orig_flow .== shuffled_flow)

    # if anything is missing, should return missing
    miss_time = convert(Vector{Union{Dates.Time, Missing}}, copy(times))
    miss_time[42] = missing
    miss_occ = convert(Vector{Union{Float64, Missing}}, copy(occ))
    miss_occ[42] = missing
    miss_flow = convert(Vector{Union{Float64, Missing}}, copy(flow))
    miss_flow[42] = missing

    res = KFactors.peak_hour_factor_binary(miss_time, occ, flow)
    @test ismissing(res.peak_hour_start)
    @test ismissing(res.peak_hour_occ)
    @test ismissing(res.peak_hour_flow)

    res = KFactors.peak_hour_factor_binary(time, miss_occ, flow)
    @test ismissing(res.peak_hour_start)
    @test ismissing(res.peak_hour_occ)
    @test ismissing(res.peak_hour_flow)

    res = KFactors.peak_hour_factor_binary(miss_time, miss_occ, flow)
    @test ismissing(res.peak_hour_start)
    @test ismissing(res.peak_hour_occ)
    @test ismissing(res.peak_hour_flow)

    res = KFactors.peak_hour_factor_binary(time, occ, miss_flow)
    @test ismissing(res.peak_hour_start)
    @test ismissing(res.peak_hour_occ)
    @test ismissing(res.peak_hour_flow)

    # and a daylight savings time day
    dst_idxes = [1:(KFactors.idx_for_time(Time(2, 0, 0)) - 1)..., KFactors.idx_for_time(Time(3, 0, 0)):288...]
    dst_time = times[dst_idxes]
    dst_occ = occ[dst_idxes]
    dst_flow  = flow[dst_idxes]

    res = KFactors.peak_hour_factor_binary(dst_time, dst_occ, dst_flow)
    @test res.peak_hour_start == Time(7, 0, 0)
    # similar to expected but different normalization
    # normalization difference is small enough we can ignore in analysis, and
    # our main analysis doesn't include Sundays anyways
    @test res.peak_hour_occ ≈ (0.05 * 60 / 5) / sum(dst_occ)
    @test res.peak_hour_flow ≈ (1 * 60 / 5) / sum(dst_flow)

    # right length, wrong hour missing
    incorrect_dst = 1:(288 - 12)
    @test length(incorrect_dst) == length(dst_idxes)
    incorrect_dst_time = times[incorrect_dst]
    incorrect_dst_occ = occ[incorrect_dst]
    incorrect_dst_flow = flow[incorrect_dst]
    res = KFactors.peak_hour_factor_binary(incorrect_dst_time, incorrect_dst_occ, incorrect_dst_flow)
    @test ismissing(res.peak_hour_start)
    @test ismissing(res.peak_hour_occ)
    @test ismissing(res.peak_hour_flow)

    # wrong length
    res = KFactors.peak_hour_factor_binary(times[1:42], occ[1:42], flow[1:42])
    @test ismissing(res.peak_hour_start)
    @test ismissing(res.peak_hour_occ)
    @test ismissing(res.peak_hour_flow)
end

@testset "Periods imputed" begin
    @test KFactors.periods_imputed([0, 100, 50]) == 2
    @test ismissing(KFactors.periods_imputed([0, 50, missing]))
end

@testset "Longest imputed time" begin
    times = Dates.Minute.(0:5:(24 * 60 - 1)) .+ Dates.Time(0, 0, 0)
    pct_obs = convert(Vector{Union{Int64, Missing}}, fill(100, 288))

    @test KFactors.longest_imputed_time(times, pct_obs) == 0

    pct_obs[22:33] .= 50 # 12 consecutive periods, i.e. 60 minutes, unobserved
    pct_obs[55:57] .= 50 # should affect nothing, not the longest period
    @test KFactors.longest_imputed_time(times, pct_obs) == 60

    # make sure it works when shuffled
    rng = MersenneTwister(85287)
    new_order = collect(1:length(times))
    shuffle!(rng, new_order)
    shuffled_times = times[new_order]
    shuffled_obs = pct_obs[new_order]
    orig_times = copy(shuffled_times)
    orig_obs = copy(shuffled_obs)

    @test KFactors.longest_imputed_time(shuffled_times, shuffled_obs) == 60
    @test all(shuffled_times .== orig_times)
    @test all(shuffled_obs .== orig_obs)

    # should handle missings
    @test ismissing(KFactors.longest_imputed_time([Dates.Time(12, 15, 0), missing], [1, 2]))
    @test ismissing(KFactors.longest_imputed_time([Dates.Time(12, 15, 0), Dates.Time(15, 0, 0)], [1, missing]))
end

@testset "isnodata" begin
    @test KFactors.isnodata(missing)
    @test KFactors.isnodata(nothing)
    @test KFactors.isnodata(NaN)
    @test !KFactors.isnodata(42)
    @test !KFactors.isnodata(0)
    @test !KFactors.isnodata("")
end

@testset "complete_enough_sensors" begin
    # create a fake dataset
    # station 0 is 100% missing in both periods
    # station 1 is 100% complete in both periods
    # station 2 is 50% complete in both periods
    # station 3 is 25% complete in both periods
    # station 4 is 50% complete in prepandemic, 100% postlockdown
    # station 5 is 50% complete in postlockdown, 100% prepandemic
    # station 6 is 100% complete in both periods but missing in lockdown (should not matter)
    # note that "incomplete" means imputed anywhere from 1-288, and different values are used to confirm
    # this is working as intended
    data = CSV.read(joinpath(Base.source_dir(), "completeness_test.csv"), DataFrame,
        types=Dict(:station=>Int64, :period=>Symbol, :periods_imputed => Int64))

    prepandemic = data[coalesce.(data.period .== :prepandemic, false), :]
    postlockdown = data[coalesce.(data.period .== :postlockdown, false), :]

    # four days, seven sensors
    @test nrow(prepandemic) == 4 * 7
    @test nrow(postlockdown) == 4 * 7
    
    prepandemic_period = [[Date(2019, 05, 13), Date(2019, 05, 16)]] # four days, inclusive
    postlockdown_period = [[Date(2022, 4, 11), Date(2022, 4, 12)], [Date(2022, 4, 18), Date(2022, 4, 19)]] # four days, split

    @test length(KFactors.Periods.filter_days(prepandemic_period[1][1], prepandemic_period[1][2])) == 4
    @test length(KFactors.Periods.filter_days(postlockdown_period[1][1], postlockdown_period[1][2])) == 2
    @test length(KFactors.Periods.filter_days(postlockdown_period[2][1], postlockdown_period[2][2])) == 2

    # with default settings, all stations except 0 should be retained as all are 25% complete in each period
    @test intersect(
        KFactors.complete_enough_sensors(prepandemic, prepandemic_period, KFactors.DEFAULT_MIN_COMPLETE),
        KFactors.complete_enough_sensors(postlockdown, postlockdown_period, KFactors.DEFAULT_MIN_COMPLETE)
    ) == Set(1:6)

    # with a 50% min complete, 3 should additionally be missing
    @test intersect(
        KFactors.complete_enough_sensors(prepandemic, prepandemic_period, 0.5),
        KFactors.complete_enough_sensors(postlockdown, postlockdown_period, 0.5)
    ) == Set([1, 2, 4, 5, 6])

    # with 75% min complete, only 1 and 6 should be present
    @test intersect(
        KFactors.complete_enough_sensors(prepandemic, prepandemic_period, 0.75),
        KFactors.complete_enough_sensors(postlockdown, postlockdown_period, 0.75)
    ) == Set([1, 6])

    # with 0% min complete, everything should be present
    @test intersect(
        KFactors.complete_enough_sensors(prepandemic, prepandemic_period, 0.0),
        KFactors.complete_enough_sensors(postlockdown, postlockdown_period, 0.0)
    ) == Set(0:6)

    # with 75% min complete, 4 should be present postlockdown but not prepandemic
    KFactors.complete_enough_sensors(prepandemic, prepandemic_period, 0.75) == Set([1, 4, 6])

    # and 5 should be present prepandemic but not postlockdown
    KFactors.complete_enough_sensors(postlockdown, postlockdown_period, 0.75) == Set([1, 5, 6])
end

@testset "create_test_data" begin
    # same fake dataset from previous test
    full_data = CSV.read(joinpath(Base.source_dir(), "completeness_test.csv"), DataFrame,
        types=Dict(:station=>Int64, :period=>Symbol, :periods_imputed => Int64), dateformat="yyyy-mm-dd")

    period = Dict(
        :prepandemic => [[Date(2019, 05, 13), Date(2019, 05, 16)]], # four days, inclusive
        :lockdown => [[Date(2020, 3, 11), Date(2020, 3, 11)]],
        :postlockdown => [[Date(2022, 4, 11), Date(2022, 4, 12)], [Date(2022, 4, 18), Date(2022, 4, 19)]]
    )

    # blank out period, make it re-create
    full_data = select(full_data, Not(:period))

    # make sure we have some dates for it to filter out
    @test any(full_data.date .== Date(2019, 11, 4))

    @test any(.!ismissing.(KFactors.Periods.period_for_date.(full_data.date, Ref(period))))

    # Filter the data using default settings. We should only have sensors 1:6, not 0
    data = KFactors.create_test_data(full_data, period)
    @test Set(unique(data.station)) == Set(1:6)

    # six sensors * 9 observations per sensor
    @test nrow(data) == 6 * 9
    
    # the non-period data should be gone
    @test !any(data.date .== Date(2019, 11, 4))

    # periods should be computed correctly (tested above)
    @test KFactors.Periods.period_for_date.(data.date, Ref(period)) == data.period

    # with a 50% min complete, 3 should additionally be missing
    @test Set(unique(KFactors.create_test_data(full_data, period, min_complete=0.5).station)) ==
        Set([1, 2, 4, 5, 6])

    # with 75% min complete, only 1 and 6 should be present
    @test Set(unique(KFactors.create_test_data(full_data, period, min_complete=0.75).station)) == Set([1, 6])

    # with 0% min complete, everything should be present
    @test Set(unique(KFactors.create_test_data(full_data, period, min_complete=0).station)) == Set(0:6)
end