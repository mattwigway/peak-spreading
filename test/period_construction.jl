# Automated tests

using Dates
using Test
using KFactors

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

@testset "Day set materialization" begin
    days = KFactors.period_days_for_year(2021)

    @test all(days .>= Dates.Date(2021, 6, 15))
    @test all(days .<= Dates.Date(2021, 7, 16))
    @test in(Dates.Date(2021, 6, 15), days)
    @test in(Dates.Date(2021, 6, 22), days)
    @test in(Dates.Date(2021, 7, 15), days)
    @test !any(Dates.dayofweek.(days) .== Dates.Saturday)
    @test !any(Dates.dayofweek.(days) .== Dates.Sunday)
    @test !in(Dates.Date(2021, 7, 5), days)  # Monday July 5 2021 was a holiday
    @test !in(Dates.Date(2021, 7, 6), days)  # Day after a holiday should also be excluded

    days2017 = KFactors.period_days_for_year(2017)
    # Day before a holiday should be excluded. Test 2017 since the day before July 4 was a weekday.
    @test !in(Dates.Date(2017, 7, 3), days2017)
end

@testset "Period for date" begin
    @test KFactors.period_for_date(Date(2021, 6, 16)) == "postpandemic"
    @test KFactors.period_for_date(Date(2020, 7, 7)) == "pandemic"
    @test KFactors.period_for_date(Date(2019, 7, 5)) == "prepandemic"
    @test KFactors.period_for_date(Date(2018, 6, 29)) == "prepandemic"
    @test KFactors.period_for_date(Date(2017, 7, 9)) == "prepandemic"
    @test KFactors.period_for_date(Date(2016, 6, 23)) == "prepandemic"
    @test KFactors.period_for_date(Date(2021, 2, 16)) |> ismissing
    @test KFactors.period_for_date(Date(2020, 4, 7)) |> ismissing
    @test KFactors.period_for_date(Date(2019, 3, 5)) |> ismissing
    @test KFactors.period_for_date(Date(2018, 5, 29)) |> ismissing
    @test KFactors.period_for_date(Date(2017, 3, 9)) |> ismissing
    @test KFactors.period_for_date(Date(2016, 9, 19)) |> ismissing
    @test KFactors.period_for_date(Date(2015, 6, 28)) |> ismissing
end