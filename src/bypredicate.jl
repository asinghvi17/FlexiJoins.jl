struct ByPred{TP, TL, TR} <: JoinCondition
    Lf::TL
    Rf::TR
    pred::TP
end

by_pred(Lf, pred, Rf) = ByPred(Lf, Rf, pred)
# by_pred(Lf, pred::typeof(isequal), Rf) = by_key((Lf, Rf))
# is_match(by::ByPred, a, b) = by.pred(a, b)

innerfunc(f::ComposedFunction) = innerfunc(f.inner)
innerfunc(f) = f
stripinner(f::ComposedFunction) = f.inner isa ComposedFunction ? f.outer ∘ stripinner(f.inner) : f.outer

normalize_arg(cond::ByPred, datas) = (@assert length(datas) == 2; cond)


supports_mode(::Mode.NestedLoop, ::ByKey, datas) = true
is_match(by::ByPred, a, b) = by.pred(by.Lf(a), by.Rf(b))
findmatchix(::Mode.NestedLoop, cond::ByPred{<:Union{typeof.((<, <=, >=, >))...}}, a, B, multi::Closest) =
    @p B |>
        findall(b -> is_match(cond, a, b)) |>
        sort(by=i -> abs(cond.Lf(a) - cond.Rf(B[i]))) |>
        first(__, 1)


supports_mode(::Mode.SortChain, ::ByPred{typeof(==)}, datas) = true
supports_mode(::Mode.Sort, ::ByPred{<:Union{typeof.((<, <=, ==, >=, >, ∋))...}}, datas) = true

sort_byf(cond::ByPred{<:Union{typeof.((<, <=, ==, >=, >, ∋))...}}) = cond.Rf

searchsorted_matchix(cond::ByPred{typeof(<)}, a, B, perm) =
    @view perm[searchsortedlast(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)) + 1:end]

searchsorted_matchix(cond::ByPred{typeof(<=)}, a, B, perm) =
    @view perm[searchsortedfirst(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)):end]

searchsorted_matchix(cond::ByPred{typeof(==)}, a, B, perm) =
    @view perm[searchsorted(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a))]

searchsorted_matchix(cond::ByPred{typeof(>=)}, a, B, perm) =
    @view perm[begin:searchsortedlast(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a))]

searchsorted_matchix(cond::ByPred{typeof(>)}, a, B, perm) =
    @view perm[begin:searchsortedfirst(mapview(i -> cond.Rf(B[i]), perm), cond.Lf(a)) - 1]

searchsorted_matchix_closest(cond::ByPred{<:Union{typeof(<), typeof(<=)}}, a, B, perm) =
    first(searchsorted_matchix(cond, a, B, perm), 1)
searchsorted_matchix_closest(cond::ByPred{<:Union{typeof(>), typeof(>=)}}, a, B, perm) =
    last(searchsorted_matchix(cond, a, B, perm), 1)

function searchsorted_matchix(cond::ByPred{typeof(∋)}, a, B, perm)
    rng = cond.Lf(a)
    @assert rng isa Interval
    arr = mapview(i -> cond.Rf(B[i]), perm)
    @view perm[searchsortedfirst(arr, minimum(rng)):searchsortedlast(arr, maximum(rng))]
end
