module Periods
using Dates, KFactors

# Get the relevant period for a particular year - from the third Tuesday of June to 32 days later
# dates where there was no mask mandate in California
# statewide mask mandate lifted June 15: https://www.latimes.com/science/story/2021-07-27/timeline-cdc-mask-guidance-during-covid-19-pandemic
# LA county requires masks again after July 17: https://www.latimes.com/california/story/2021-07-15/l-a-county-will-require-masks-indoors-amid-covid-19-surge
# function period_for_year(year)
#     start = Dates.tonext(d -> Dates.dayofweek(d) == Dates.Tuesday, Date(year, 5, 31)) + Dates.Week(2)
#     endd = start + Dates.Day(32)
#     return start, endd
# end

# function period_for_year(year)
#     start = Dates.tonext(d -> Dates.dayofweek(d) == Dates.Wednesday, Date(year, 1, 31)) + Dates.Week(2)
#     endd = start + Dates.Day(61)
#     return start, endd
# end

# create a period_for_year function based on the pandemic period of interest
function create_period(start, endd)
    start_day_of_week = Dates.dayofweek(start)
    start_month = Dates.month(start)
    week_of_month = Dates.dayofweekofmonth(start)
    period_length = endd - start

    function period_for_year(year)
        pstart = Dates.tonext(d -> Dates.month(d) == start_month && Dates.dayofweekofmonth(d) == week_of_month && Dates.dayofweek(d) == start_day_of_week, Dates.Date(year - 1, 12, 31))
        pend = pstart + period_length
        return pstart, pend
    end
end

function period_days_for_year(year, period_for_year)
    start, endd = period_for_year(year)
    filter_days(start, endd)
end

function filter_days(start, endd)
    days = filter(start:Dates.Day(1):endd) do d
        Dates.dayofweek(d) != Dates.Saturday &&
        Dates.dayofweek(d) != Dates.Sunday &&
        !in(d, HOLIDAYS_Δ1)
    end
    collect(days)
end

# const PERIODS = Dict(
#     "postpandemic" => [period_for_year(2022)],
#     "pandemic" => [period_for_year(2021)],
#     "prepandemic" => [
#         period_for_year(2019),
#         period_for_year(2018),
#         period_for_year(2017),
#         period_for_year(2016)
#     ]
# )

function period_for_date(date, periods)
    if date ∈ HOLIDAYS_Δ1
        return missing
    end

    for (name, dates) in periods
        for range in dates
            if date >= range[1] && date <= range[2]
                return name
            end
        end
    end
    return missing
end


# Find matching periods in previous years
spring_2022_period = create_period(Date(2022, 2, 16), Date(2022, 4, 18))
const SPRING_2022 = Dict(
    :postlockdown => [spring_2022_period(2022)],
    :lockdown => [spring_2022_period(2021)],
    :prepandemic => collect(spring_2022_period.(2016:2019))
)

# create some additional datasets/periods
march_2022_period_for_year = create_period(Date(2022, 3, 12), Date(2022, 4, 18))
const MARCH_2022 = Dict(
    :postlockdown => [march_2022_period_for_year(2022)],
    :lockdown => [march_2022_period_for_year(2021)],
    :prepandemic => collect(march_2022_period_for_year.(2016:2019))
)

summer_2021_period_for_year = create_period(Date(2021, 6, 15), Date(2021, 7, 17))
const SUMMER_2021 = Dict(
    :postlockdown => [summer_2021_period_for_year(2021)],
    :lockdown => [summer_2021_period_for_year(2020)],
    :prepandemic => collect(summer_2021_period_for_year.(2016:2019))
)
end