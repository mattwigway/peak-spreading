# Federal holidays, 2016-2021

# federal holidays, observed
# https://www.opm.gov/policy-data-oversight/pay-leave/federal-holidays/#url=2021
const HOLIDAYS = Set([
    Date(2016, 1, 1),
    Date(2016, 1, 18),
    Date(2016, 2, 15),
    Date(2016, 5, 30),
    Date(2016, 7, 4),
    Date(2016, 9, 5),
    Date(2016, 10, 10),
    Date(2016, 11, 11),
    Date(2016, 11, 24),
    Date(2016, 12, 26),
    
    Date(2017, 1, 2),
    Date(2017, 1, 16),
    Date(2017, 2, 20),
    Date(2017, 5, 29),
    Date(2017, 7, 4),
    Date(2017, 9, 4),
    Date(2017, 10, 9),
    Date(2017, 11, 10),
    Date(2017, 11, 23),
    Date(2017, 12, 25),
    
    Date(2018, 1, 1),
    Date(2018, 1, 15),
    Date(2018, 2, 19),
    Date(2018, 5, 28),
    Date(2018, 7, 4),
    Date(2018, 9, 3),
    Date(2018, 10, 8),
    Date(2018, 11, 12),
    Date(2018, 11, 22),
    Date(2018, 12, 25),
    
    Date(2019, 1, 1),
    Date(2019, 1, 21),
    Date(2019, 2, 18),
    Date(2019, 5, 27),
    Date(2019, 7, 4),
    Date(2019, 9, 2),
    Date(2019, 10, 14),
    Date(2019, 11, 11),
    Date(2019, 11, 28),
    Date(2019, 12, 25),
        
    Date(2020, 1, 1),
    Date(2020, 1, 20),
    Date(2020, 2, 17),
    Date(2020, 5, 25),
    Date(2020, 7, 3),
    Date(2020, 9, 7),
    Date(2020, 10, 12),
    Date(2020, 11, 26),
    Date(2020, 12, 25),
        
    Date(2021, 1, 1),
    Date(2021, 1, 18),
    # not including inauguration day, not a holiday in CA
    Date(2021, 2, 15),
    Date(2021, 5, 31),
    Date(2021, 6, 18),
    Date(2021, 7, 5),
    Date(2021, 9, 6),
    Date(2021, 10, 11),
    Date(2021, 11, 11),
    Date(2021, 11, 25),
    Date(2021, 12, 24),
    Date(2021, 12, 31),

    Date(2022, 1, 17),
    Date(2022, 2, 21),
    Date(2022, 5, 30),
    Date(2022, 6, 20),
    Date(2022, 7, 4),
    Date(2022, 9, 5),
    Date(2022, 10, 10),
    Date(2022, 11, 11),
    Date(2022, 11, 24),
    Date(2022, 12, 26)
])

# all holidays and the days before and after
const HOLIDAYS_Δ1 = Set([
        HOLIDAYS...,
        (HOLIDAYS .+ Dates.Day(1))...,
        (HOLIDAYS .- Dates.Day(1))...
])