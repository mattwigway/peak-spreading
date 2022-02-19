# Volume Delay Functions
# This file contains functions to compute hourly flow based on demand and
# demand based on hourly flow, using the Bureau of Public Roads delay
# functions used in the Southern California Association of Governments
# travel demand model. It is defined on page 9-4 (180) of
# https://scag.ca.gov/sites/main/files/file-attachments/scag_rtdm_2012modelvalidation.pdf

# Stick these in a submodule to organize the namespace
module VDF

using Interpolations

function bpr_demand_to_speed(ff_speed, demand; capacity=2400, α=0.6, βff=5, βcongested=8)
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

function bpr_speed_flow_to_demand(ff_speed, speed, flow, capacity)
    est_demand = bpr_speed_to_demand(ff_speed, speed, capacity=capacity)
    if est_demand < capacity
        # at low levels of demand, the speed-demand function is pretty flat, so slight
        # speed fluctuations could cause large estimated demand functions. In uncongested
        # flow situations, the actual flow is going to be a better measure of demand
        # than the speed.
        flow
    else
        est_demand
    end
end

"""
Estimate the heavy vehicle adjustment factor (HCM equation 12-10). Multiplying ideal capacity by this
results in estimated capacity given the influence of heavy vehicles on the traffic stream.

Passenger-car equivalents for different scenarios can be found at the end of chapter 12 of the HCM.
"""
heavy_vehicle_adjustment_factor(passenger_car_equivalent, prop_of_total_flow) =
    prop_of_total_flow > 1 ? error("Proportion should not be a percent") : 1 / (1 + prop_of_total_flow * passenger_car_equivalent)

"""
Estimate the capacity of a weaving segment. This adjusts capacity downwards to account for disruptions due
to weaving. HCM equation 13-5.

weaving_lanes is a little confusing the way it's defined in the HCM, but basically, it's just the number of lanes involved
in the weave.
"""
weaving_capacity(basic_capacity, weave_proportion, weave_length, weaving_lanes) = 
    basic_capacity - 438.2 * (1 + weave_proportion) ^ 1.6 + 0.0765 * weave_length + 119.8 * weaving_lanes


# this is exhibit 12-4 from the HCM
# Per-lane capacity for basic freeway segments, from HCM exhibit 12-4
# I know it's weird to have a lowercased const, but this is effectively a function
const basic_capacity_per_lane = LinearInterpolation(
    float.([55, 60, 65, 70, 75]),  # speeds in mph
    float.([2250, 2300, 2350, 2400, 2400])  # capacities in pc/lane/hr
)

# Truck passenger-car-equivalents for flat roads in urban area (50/50 single unit/tractor-trailer)
# HCM suggests 50/50 ratio for urban areas
const urban_truck_pce = LinearInterpolation(
    [0.02, 0.04, 0.05, 0.06, 0.08, 0.1,  0.15, 0.2, 0.25, 1],  # percentage of trucks
    [2.67, 2.38, 2.31, 2.25, 2.16, 2.11, 2.02, 1.97, 1.93, 1.93]  # PCE
)
end