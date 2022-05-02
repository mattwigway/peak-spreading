# Test that functions to convert back and forth between times and indices work

@testset "Time indices" begin
    roundtrip = KFactors.time_for_idx ∘ KFactors.idx_for_time

    @test all(
        roundtrip.(Dates.Time(0):Dates.Minute(5):Dates.Time(23, 55)) .== Dates.Time(0):Dates.Minute(5):Dates.Time(23, 55)
    )

    # make sure errors are thrown for inexact or out-of-range times
    @test_throws InexactError KFactors.idx_for_time(Dates.Time(12, 8))
    @test_throws ArgumentError KFactors.time_for_idx(0)
    @test_throws ArgumentError KFactors.time_for_idx(289)
end