# Factor out common code for computing K-factors
# use in a notebook by doing include("path/to/KFactors.jl"); using .KFactors
# Author: Matt Bhagat-Conway <mwbc@unc.edu> and Sam Zhang, 2021-09-17

module KFactors

using Dates, CSV, Parquet, DataFrames, Logging, StatsBase, Statistics, Pipe,
    CategoricalArrays, Random, Printf, CodecZlib, Suppressor, Missings, Logging

include("holidays.jl")
include("geo.jl")
include("KFactorPeaks.jl")
include("computation.jl")
include("periods.jl")

export HOLIDAYS, HOLIDAYS_Δ1, read_data, permutation_test, period_for_year, period_days_for_year, parse_file, VDF
end
