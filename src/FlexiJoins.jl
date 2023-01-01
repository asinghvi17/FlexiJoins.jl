module FlexiJoins

using StructArrays
using Static: StaticInt, known
using Accessors
using DataPipes
using Indexing
using SplitApplyCombine: mapview
using IntervalSets


export
    flexijoin, joinindices, @optic,
    by_key, by_distance, by_pred,
    keep, drop


include("nothingindex.jl")
include("conditions.jl")
include("bykey.jl")
include("bydistance.jl")
include("bypredicate.jl")
include("normalize_specs.jl")
include("ix_compute.jl")


function flexijoin(datas, cond; kwargs...)
	IXs = joinindices(datas, cond; kwargs...)
    myview(datas, IXs)
end

function joinindices(datas::NamedTuple{NS}, cond; kwargs...) where {NS}
    IXs_unnamed = _joinindices(datas, cond; kwargs...)
    return StructArray(NamedTuple{NS}(StructArrays.components(IXs_unnamed)))
end

function joinindices(datas::Tuple, cond; kwargs...)
    IXs_unnamed = _joinindices(datas, cond; kwargs...)
    return StructArray(StructArrays.components(IXs_unnamed))
end

function _joinindices(datas, cond; kwargs...)
    _joinindices(
        values(datas),
        normalize_arg(cond, datas),
        normalize_arg(get(kwargs, :multi, nothing), datas; default=identity),
        normalize_arg(get(kwargs, :nonmatches, nothing), datas; default=drop),
        normalize_groupby(get(kwargs, :groupby, nothing), datas),
        normalize_arg(get(kwargs, :cardinality, nothing), datas; default=*),
        get(kwargs, :mode, nothing),
    )
end

function _joinindices(datas::NTuple{2, Any}, cond::JoinCondition, multi, nonmatches, groupby, cardinality, mode)
    mode = @something(mode, best_mode(cond, datas))
    if any(@. multi !== identity && nonmatches !== drop)
        error("Values of arguments don't make sense together: ", (; nonmatches, multi))
    end
	IXs = create_ix_array(datas, nonmatches, groupby)
	fill_ix_array!(mode, IXs, datas, cond, multi, nonmatches, groupby, cardinality)
end


materialize_views(A::StructArray) = StructArray(map(materialize_views, StructArrays.components(A)))
materialize_views(A::ViewVector) = collect(A)
materialize_views(A::Vector{<:ViewVector}) = map(materialize_views, A)
materialize_views(A) = A

end
