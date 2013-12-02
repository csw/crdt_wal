%% passive_set.erl: client interface for passive Set CRDTs.

-module(passive_set).

-export_type([set/1]).

-export([new/3, value/1, update/2, sync/2]).

-type smod()         :: module().
-type replica_id()   :: term().
-type counter()      :: non_neg_integer().
-type mac()          :: term().

-type member()     :: term().
-type set_op()     :: {'add', member()} | {'remove', member()}.

-record(set, {mod                  :: module(),
              crdt                 :: term(),
              id                   :: replica_id(),
              next_req=0           :: counter(),
              mac=none             :: mac(),
              adds=[]              :: ordsets:ordset(member()),
              rems=[]              :: ordsets:ordset(member()),
              pending=none         :: counter() | 'none',
              pending_replica=none :: term() | 'none'
             }).
-type set(CRDT)                    :: #set{crdt :: CRDT}.

-spec new(smod(), CRDT, replica_id()) -> set(CRDT).
new(Mod, CRDT, ID) ->
    #set{mod=Mod, crdt=CRDT, id=ID}.

-spec value(set(_CRDT)) -> [member()].
value(#set{mod=Mod, crdt=CRDT, adds=Adds, rems=Rems}) ->
    Orig = Mod:value(CRDT),
    ordsets:union(ordsets:subtract(Orig, Rems), Adds).

-spec update(set_op(), set(CRDT)) -> set(CRDT).
update({add, E}, S=#set{adds=Adds, rems=Rems}) ->
    S#set{adds=ordsets:add_element(E, Adds),
          rems=ordsets:del_element(E, Rems)};
update({remove, E}, S=#set{mod=CMod, adds=Adds, rems=Rems}) ->
    case CMod:value({contains, E}) of
        true ->
            %% if in the CRDT, remove from Adds, add to Rems
            S#set{adds=ordsets:del_element(E, Adds),
                  rems=ordsets:add_element(E, Rems)};
        false ->
            %% if in Adds but not CRDT, remove from adds
            S#set{adds=ordsets:del_element(E, Adds)}
    end.

%% For riak_dt_orswot:
%%
%%  prepare({passive_add, E})             -> {add, E}
%%  prepare({passive_remove, E})          -> {remove_versions, E, Dots}
%%
%%  prepare({add, E})   -> {add_at, E, Dot}
%%  prepare({remove_versions, E, Dots}) -> {remove_versions, E, Dots}
%%
%%  update({add_at, E, Dot})
%%  update({remove_versions, E, Dots})
%%    (check clocks, not dot presence)
%%

-spec sync(set(CRDT), term()) -> {'ok', set(CRDT)} |
                                 {'retry', set(CRDT)}.
sync(S=#set{}, _Service) ->
    {retry, S}.
