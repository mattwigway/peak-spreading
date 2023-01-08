using DataFrames, Dates, HypothesisTests

get_data() = DataFrame(
        period=[fill(:prepandemic, 100); fill(:postlockdown, 100)],
        date=[
            # two obs per day
            Date(2020, 1, 1):Day(1):(Date(2020, 1, 1) + Day(49));
            Date(2020, 1, 1):Day(1):(Date(2020, 1, 1) + Day(49));
            Date(2022, 1, 1):Day(1):(Date(2022, 1, 1) + Day(49));
            Date(2022, 1, 1):Day(1):(Date(2022, 1, 1) + Day(49));
        ],
        val=[fill(0, 100); fill(1, 100)]
    )

@testset "permute (probabilistic test, changes to Julia RNG algorithm or machine architecture may cause spurious failures, investigate this first)" begin
    # True difference is 1, permuted differences should be zero
    # and means should be around 0.5
    data = get_data()

    @test nrow(data) == 200
    @test length(unique(data.date)) == 100  # each day should have two records

    means = KFactors.permute(data, 10_000, :val)

    @test size(means) == (2, 10_000)

    # NB the permutation test uses MersenneTwister for historic reasons, rather than StableRNG
    # The MersenneTwister has a constant seed, but Julia does not guarantee consistency between
    # platforms or releases.
    @test pvalue(EqualVarianceTTest(means[1, :], means[2, :])) > 0.1
    @test pvalue(OneSampleTTest(means[1, :], 0.5)) > 0.1
    @test pvalue(OneSampleTTest(means[2, :], 0.5)) > 0.1

    # because of the block bootstrapping, there can be anywhere from 0 to 100 ones, in blocks of two
    @test all(means .∈ Ref(0.0:0.02:1.0))

    # we can analytically calculate the standard error of the distribution of the means
    # We have 10,000 random samples of size 50 from a distribution that has mean 0.5 and standard deviation 0.5.
    # (size 50 because we permute days, so each set of two samples is forced to stay together).
    # 
    # (derivation of standard deviation: we have 200 observations. Each observation is 0 or 1, so it
    # deviates from the mean by +0.5 or -0.5, so the variance is n * (±0.5²) / n = (200 / 200) * 0.25 =
    # 0.25. The standard deviation is 0.5.
    # NB not using sample variance since from the standpoint of the permutation test this is effectively
    # the population.
    # By the central limit theorem, the standard deviation of samples of size 50 drawn from this distribution
    # is 0.5 / √50. Normal distribution not needed to justify central limit theorem with large n.
    # effective sample size is 50 due to block bootstrapping.
    # However, since the sample is a nontrivial proportion of the "population," we need to apply a finite-sample
    # correction, which is sqrt(N-n/N), where N = 100 due to blocking. See Knaub J, Finite Sample Correction,
    # SAGE Encyclopedia of Survey Research Methods
    @test std(means[1, :]) ≈ 0.5 / sqrt(50) * sqrt((100 - 50) / 100) atol=0.001
    @test std(means[2, :]) ≈ 0.5 / sqrt(50) * sqrt((100 - 50) / 100) atol=0.001

    # now we test block-bootstrapping
    # we do this by creating a data frame where there are only four days: two prepandemic days and two postlockdown days
    # since they should get bootstrapped together and sample sizes stay the same, there are three possible outcomes for the
    # bootstrapped means: 1 (both postlockdown days), 0.5 (one of each), and 0 (both prepandemic).
    blockdata = data[:,:]
    blockdata.date = [
        fill(Date(2020, 1, 1), 50);
        fill(Date(2020, 1, 2), 50);
        fill(Date(2021, 1, 1), 50);
        fill(Date(2021, 1, 2), 50);
    ]
    
    blockmeans = KFactors.permute(blockdata, 10_000, :val)

    # flatten, only use one set of means since other set of means is deterministic conditional
    # on first set.
    blockmeans = blockmeans[1,:]

    @test all(
        (blockmeans .== 0.0) .|
        (blockmeans .== 1.0) .|
        (blockmeans .== 0.5)
    )

    # permuting is sampling without replacement, so we can't assume probabilities are independent,
    # and since we're only permuting four days, the with/without is important
    # Probability of 1.0: p(post and post) = p(post) * p(post|post) = 0.5 * 0.33 ≈ 0.165
    # Probability of 0.0: p(pre and pre) = p(pre) * p(pre|pre) = 0.5 * 0.33 ≈ 0.165
    # Probability of 0.5: p(post and pre) + p(pre and post) = 0.5 * 0.67 * 2 ≈ 0.67
    @test pvalue(BinomialTest(blockmeans .== 0.0, 0.5 * 1/3)) > 0.1
    @test pvalue(BinomialTest(blockmeans .== 1.0, 0.5 * 1/3)) > 0.1
    @test pvalue(BinomialTest(blockmeans .== 0.5, 2 * 0.5 * 2/3)) > 0.1
end

@testset "permutation_test convenience function (probabilistic test)" begin
    data = get_data()

    tstat = KFactors.permutation_test(data, :val, n_permutations=10_000)
    @test tstat.ptest == 1
    @test tstat.pval ≤ 0.0001

    # now, force before and after to have same mean
    data.val = [
        fill(0.0, 25);
        fill(1.0, 25);
        fill(0.0, 25);
        fill(1.0, 25);
        fill(0.0, 25);
        fill(1.0, 25);
        fill(0.0, 25);
        fill(1.0, 25);
    ]

    data.station = [1:50; 1:50; 1:50; 1:50]

    tstat = KFactors.permutation_test(data, :val, n_permutations=10_000)
    @test tstat.ptest == 0
    @test tstat.pval ≈ 1
    @test tstat.n_sensors == 50
end