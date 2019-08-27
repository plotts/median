CREATE OR REPLACE FUNCTION _median_transfn(state internal, val anyelement)
RETURNS internal
AS '/usr/lib/postgresql/11/lib/median.so', 'median_transfn'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _median_finalfn(state internal, val anyelement)
    RETURNS anyelement
AS '/usr/lib/postgresql/11/lib/median.so', 'median_finalfn'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _median_invfn(state internal, val anyelement)
    RETURNS internal
AS '/usr/lib/postgresql/11/lib/median.so', 'median_invfn'
    LANGUAGE C IMMUTABLE;

DROP AGGREGATE IF EXISTS median (ANYELEMENT);
CREATE AGGREGATE median (ANYELEMENT)
(
    sfunc = _median_transfn,
    stype = internal,
    finalfunc = _median_finalfn,
    finalfunc_extra,
    msfunc = _median_transfn,
    mstype = internal,
    minvfunc = _median_invfn,
    mfinalfunc=_median_finalfn,
    mfinalfunc_extra,
    PARALLEL=UNSAFE
);
