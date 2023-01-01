abstract type JoinCondition end

is_match(by::JoinCondition, a) = b -> is_match(by, a, b)

struct CompositeCondition{TC} <: JoinCondition
    conds::TC
end

is_match(by::CompositeCondition, a, b) = all(by1 -> is_match(by1, a, b), by.conds)

Base.:(&)(a::JoinCondition, b::JoinCondition) = CompositeCondition((a, b))
Base.:(&)(a::CompositeCondition, b::JoinCondition) = CompositeCondition((a.conds..., b))
Base.:(&)(a::JoinCondition, b::CompositeCondition) = CompositeCondition((a, b.conds))
Base.:(&)(a::CompositeCondition, b::CompositeCondition) = CompositeCondition((a.conds..., b.conds...))

normalize_arg(cond::CompositeCondition, datas) = CompositeCondition(map(c -> normalize_arg(c, datas), cond.conds))


function closest end

struct ByDistance{TF, TD, TP} <: JoinCondition
    func::TF
    dist::TD
    pred::TP
    max::Float64
end

by_distance(func, max) = by_distance(func, Euclidean(), max)
by_distance(func, dist, max::Real) = ByDistance(func, dist, <=, Float64(max))
by_distance(func, dist, maxpred::Base.Fix2) = ByDistance(func, dist, maxpred.f, Float64(maxpred.x))

is_match(by::ByDistance, a, b) = by.pred(by.dist(by.func(a), by.func(b)), by.max)

# extra(::Val{:distance}, by::ByDistance, a, b) = by.dist(by.func(a), by.func(b))
# extra(::Val{:distance}, by::ByDistance, a::Nothing, b) = nothing
# extra(::Val{:distance}, by::ByDistance, a, b::Nothing) = nothing
