# Volume Delay Functions
# This file contains functions to compute hourly flow based on demand and
# demand based on hourly flow, using the Bureau of Public Roads delay
# functions used in the Southern California Association of Governments
# travel demand model. It is defined on page 9-4 (180) of
# https://scag.ca.gov/sites/main/files/file-attachments/scag_rtdm_2012modelvalidation.pdf

# Stick these in a submodule to organize the namespace
module VDF
function bpr_demand_to_speed(ff_speed, demand, capacity=2400; α=0.6, βff=5, βcongested=8)
    # this is the function written out in the SCAG documentation
    # note that speed can be miles per hour, km per hour, knots, furlongs per fortnight, etc - the
    # return value will be in the same units.
    β = demand ≤ capacity ? βff : βcongested
    ff_speed / (1 + α  * (demand / capacity) ^ β)
end

function bpr_speed_to_demand(ff_speed_mph, obs_speed_mph; α=0.6, βff=5, βcongested=8, capacity=2400)
    if βff < 1 || βcongested < 1
        error("β must be ≥ 1!")
    end

    if obs_speed_mph > ff_speed_mph
        error("Observed speed cannot be greater than freeflow speed!")
    end
    
    # since we know β ≥ 1, the part inside the root must be greater than 1 for xi to be larger than ci
    # which controls which β we use.
    inner  = (ff_speed_mph / obs_speed_mph - 1) / α
    
    β = inner > 1 ? βcongested : βff
    
    capacity * inner ^ (1 / β) 
end
end