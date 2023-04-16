import Distributions


@testset "resample" begin
    mtx = [
        1  2  3  4  5  6  7;
        2  3  4  5  6  7  8;
        3  4  5  6  7  8  9;
        4  5  6  7  8  9 10;
        5  6  7  8  9 10 11;
    ]

    ex_result = [
        mean([1, 2, 2, 3, 3, 4]) mean([3, 4, 4, 5, 5, 6]) mean([5, 6, 6, 7, 7, 8]) mean([7, 8, 9]); # last one smaller, not multiple of block size
        mean([4, 5, 5, 6]) mean([6, 7, 7, 8]) mean([8, 9, 9, 10]) mean([10, 11])
    ]

    # each resampled block should be mean of three rows and two columns, excluding anything outside the array
    @test KFactors.FundamentalDiagrams.resample(mtx, (3, 2)) == ex_result
end

@testset "toquantiles" begin
    # quantile definitions are not well defined, see ?quantile in Julia for details
    # None of this matters at high sample sizes really.
    # The deciles of 0:10 are 0.1, 0.2, ... 1.0 - even though 0:10 is 11 values
    # we just run with this rather than doing somethign tricky by using a non-standard percentile definition.
    # We add one to the array so that results are not affected when skipzero = true
    arr = convert.(Float64, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10] .+ 1)
    exp = (arr .- 1) ./ 10
    @test KFactors.FundamentalDiagrams.toquantiles(arr, 10) ≈ exp
    # adding zeros with skipzero=true should not affect anything
    @test KFactors.FundamentalDiagrams.toquantiles([0, arr..., 0, 0], 10; skipzero=true) ≈ [0.0, exp..., 0.0, 0.0]
    # when skipzero is false, should handle zeros correctly
    @test KFactors.FundamentalDiagrams.toquantiles(arr .- 1, 10) ≈ exp
end

@testset "normalkernel" begin
    vals = [1, 2, 3, 4, 5, 6, 7]
    scale = [1, 1, 2, 2, 3, 3, 4]
    exp = map(enumerate(scale)) do (idx, scale)
        dist = Distributions.Normal(idx, scale)
        w = Distributions.pdf.(dist, 1:length(vals))
        sum(vals .* w) / sum(w)
    end

    @test KFactors.FundamentalDiagrams.normalkernel(vals, scale) ≈ exp
    missw = Distributions.pdf.(Distributions.Normal(0, 1), 1:length(vals))
    miss = sum(vals .* missw) / sum(missw)
    # should wash out missing values
    @test KFactors.FundamentalDiagrams.normalkernel([missing, vals...], [1, scale...]) ≈ [miss, exp...]

end

@testset "getlines" begin
    # getlines does two things:
    # - finds the ~modal~ median value of each column
    # - smooths it with a normalkernel
    mtx = [
        6 4 9 2 2;
        4 4 1 2 2;
        2 8 1 3 2;
        1 1 1 2 2;
    ]

    medians = [
        # like quantiles, medians are ill-defined, so use the default Julia def for internal consistency
        median(1:4, weights([6, 4, 2, 1])),
        median(1:4, weights([4, 4, 8, 1])),
        median(1:4, weights([9, 1, 1, 1])),
        median(1:4, weights([2, 2, 3, 2])),
        median(1:4, weights([2, 2, 2, 2]))
    ]

    exp = KFactors.FundamentalDiagrams.normalkernel(medians, 2)

    r = KFactors.FundamentalDiagrams.getlines(mtx; scale=2)
    @test r[1] == 1:5
    @test r[2] ≈ exp
end