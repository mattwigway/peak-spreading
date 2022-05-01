# Automated tests

using Dates
using Test
using KFactors

@testset "Period construction for 2022" begin
    @test (KFactors.Periods.create_period(Date(2022, 2, 16), Date(2022, 4, 18)))(2022) == (Date(2022, 02, 16), Date(2022, 04, 18))
end

@testset "Create periods and period construction for $y" for y in 2016:2022
    s, e = KFactors.Periods.create_period(Date(2022, 2, 16), Date(2022, 4, 18))(y)
    @test e - s == Dates.Day(61)
    # make sure it's the third Wednesday in February
    @test Dates.month(s) == Dates.February
    @test Dates.year(s) == y
    @test Dates.dayofweek(s) == Dates.Wednesday
    @test Dates.day(s) >= 15
    @test Dates.day(s) <= 21
end

@testset "Day set materialization" begin
    days = KFactors.Periods.period_days_for_year(2022, KFactors.Periods.spring_2022_period)

    @test all(days .>= Dates.Date(2022, 2, 16))
    @test all(days .<= Dates.Date(2022, 4, 18))
    @test in(Dates.Date(2022, 2, 16), days)
    @test in(Dates.Date(2022, 3, 14), days)
    @test in(Dates.Date(2022, 4, 18), days)
    @test !any(Dates.dayofweek.(days) .== Dates.Saturday)
    @test !any(Dates.dayofweek.(days) .== Dates.Sunday)
    @test !in(Dates.Date(2021, 2, 21), days)  # President's day
    @test !in(Dates.Date(2021, 2, 22), days)  # Day after a holiday should also be excluded
end

@testset "Period for date" begin
    @test KFactors.Periods.period_for_date(Date(2022, 3, 16), KFactors.Periods.SPRING_2022) == :postlockdown
    @test KFactors.Periods.period_for_date(Date(2021, 3, 16), KFactors.Periods.SPRING_2022) == :lockdown
    @test KFactors.Periods.period_for_date(Date(2019, 3, 16), KFactors.Periods.SPRING_2022) == :prepandemic
    @test KFactors.Periods.period_for_date(Date(2018, 3, 16), KFactors.Periods.SPRING_2022) == :prepandemic
    @test KFactors.Periods.period_for_date(Date(2017, 3, 20), KFactors.Periods.SPRING_2022) == :prepandemic
    @test KFactors.Periods.period_for_date(Date(2016, 3, 20), KFactors.Periods.SPRING_2022) == :prepandemic
    @test KFactors.Periods.period_for_date(Date(2022, 2, 21), KFactors.Periods.SPRING_2022) |> ismissing # president's day
    @test KFactors.Periods.period_for_date(Date(2022, 2, 22), KFactors.Periods.SPRING_2022) |> ismissing # day after president's day
    @test KFactors.Periods.period_for_date(Date(2022, 5, 10), KFactors.Periods.SPRING_2022) |> ismissing
    @test KFactors.Periods.period_for_date(Date(2022, 1, 7), KFactors.Periods.SPRING_2022) |> ismissing
    @test KFactors.Periods.period_for_date(Date(2019, 5, 18), KFactors.Periods.SPRING_2022) |> ismissing
    @test KFactors.Periods.period_for_date(Date(2018, 5, 18), KFactors.Periods.SPRING_2022) |> ismissing
    @test KFactors.Periods.period_for_date(Date(2017, 5, 16), KFactors.Periods.SPRING_2022) |> ismissing
    @test KFactors.Periods.period_for_date(Date(2016, 5, 16), KFactors.Periods.SPRING_2022) |> ismissing
    @test KFactors.Periods.period_for_date(Date(2015, 2, 26), KFactors.Periods.SPRING_2022) |> ismissing
end