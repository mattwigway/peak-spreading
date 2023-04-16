# Utility code for creating fundamental diagrams

module FundamentalDiagrams

import StatsBase: mean, quantile, weights, median
import Distributions: Normal, pdf

# resample an array to be smaller by averaging adjacent cells
function resample(array, blocksize::NTuple{N, <:Integer}) where N
    shp = ceil.(Int64, size(array) ./ blocksize)
    out = zeros(Float64, shp)
    
    for x in 1:shp[1]
        for y in 1:shp[2]
            xfr = (x - 1) * blocksize[1] + 1
            xto = min(x * blocksize[1], size(array)[1])
            yfr = (y - 1) * blocksize[2] + 1
            yto = min(y * blocksize[2], size(array)[2])
            out[x, y] = mean(@view array[xfr:xto, yfr:yto])
        end
    end
    
    out
end

# Convert an array of numbers to an array of quantiles
function toquantiles(vector, n; skipzero=false)
    if skipzero
        qvector = filter(x -> !(x ≈ 0), reshape(vector, :))
    else
        qvector = reshape(vector, :)
    end
    # n + 1 b/c both 0 and 1 are in range, so 0:0.1:1 is 11 values
    probs = range(0, 1; length=n + 1)
    quantiles = quantile(qvector, probs)
    
    map(x -> probs[findfirst(x .≤ quantiles)], vector)
end

# Functions to get trendline
# The way this works is we find the highest value in each column,
# then use a normal kernel to smooth that number

function normalkernel(arr, scale)
    if scale isa Number
        scale = fill(scale, length(arr))
    end
        

    nm = .!ismissing.(arr)

    map(1:length(arr)) do i
        dist = Normal(i, scale[i])
        wgts = pdf.(dist, 1:length(arr))
        mean(arr[nm], weights(wgts[nm]))
    end
end


function getlines(arr;scale=5)
    xs = 1:(size(arr)[2])
    
    ys = map(xs) do x
        #argmax(arr[x,:])
        w = arr[:,x]
        if sum(w) ≈ 0
            missing
        else
            median(1:size(arr)[1], weights(w))
        end
    end
    
    xs, normalkernel(ys, scale)
end

export resample, toquantiles, normalkernel, getlines

end