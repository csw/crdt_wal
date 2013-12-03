%% passive_set.erl: client interface for passive Set CRDTs.

-module(passive_set).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([set/1]).

-export([new/4, value/1, value/2, update/2, sync/2]).

-type smod()         :: module().
-type replica_id()   :: term().
-type counter()      :: non_neg_integer().
-type mac()          :: term().

-type member()     :: term().
-type set_op()     :: {'add', member()} | {'remove', member()}.

-record(set, {mod                  :: module(),
              crdt                 :: term(),
              cid                  :: term(),
              rid                  :: replica_id(),
              next_req=0           :: counter(),
              mac=none             :: mac(),
              adds=[]              :: ordsets:ordset(member()),
              rems=[]              :: ordsets:ordset(member()),
              pending=none         :: counter() | 'none',
              pending_replica=none :: term() | 'none'
             }).
-type set(CRDT)                    :: #set{crdt :: CRDT}.

-spec new(smod(), CRDT, term(), replica_id()) -> set(CRDT).
new(Mod, CRDT, CID, RID) ->
    #set{mod=Mod, crdt=CRDT, cid=CID, rid=RID}.

-spec value(set(_CRDT)) -> [member()].
value(#set{mod=Mod, crdt=CRDT, adds=Adds, rems=Rems}) ->
    Orig = Mod:value(CRDT),
    ordsets:union(ordsets:subtract(Orig, Rems), Adds).

-spec value({'contains', term()}, set(_CRDT)) -> boolean().
value({contains, Elem},
      #set{mod=Mod, crdt=CRDT, adds=Adds, rems=Rems}) ->
    %% in Adds or (in CRDT and not in Rems)
    ordsets:is_element(Elem, Adds)
        orelse (Mod:value({contains, Elem}, CRDT)
                andalso not ordsets:is_element(Elem, Rems)).


-spec update(set_op(), set(CRDT)) -> set(CRDT).
update({add, E}, S=#set{adds=Adds, rems=Rems}) ->
    S#set{adds=ordsets:add_element(E, Adds),
          rems=ordsets:del_element(E, Rems)};
update({remove, E},
       S=#set{mod=CMod, crdt=CRDT, adds=Adds, rems=Rems}) ->
    case CMod:value({contains, E}, CRDT) of
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
%%  effect({add_at, E, Dot})
%%  effect({remove_versions, E, Dots})
%%    (check clocks, not dot presence)
%%

-spec sync(set(CRDT), term()) -> {'ok', set(CRDT)} |
                                 {'retry', set(CRDT)}.
sync(S=#set{adds=[], rems=[]}, _Service) ->
    {ok, S};
sync(S=#set{adds=Adds, rems=Rems, cid=CID}, Service) ->
    Ops = ([ {passive_remove, E} || E <- Rems ]
           ++ [ {passive_add, E} || E <- Adds ]),
    Sender = crdt_service:send_passive_fun(Service, CID),
    S3 = lists:foldl(fun(Op, S1) ->
                             {ok, S2} = sync_op(Op, S1, Sender),
                             S2
                     end,
                     S,
                     Ops),
    {ok, S3}.

sync_op(Op, S=#set{mod=CMod, crdt=CRDT, rid=RID, next_req=Req},
        Sender) ->
    case CMod:prepare(Op, nil, CRDT) of
        {ok, Prep, CRDT1} when CRDT == CRDT1 ->
            RequestID = {RID, Req},
            {ok, ResC, ResMAC} =
                Sender(RequestID, Prep),

            %% XXX: merging invalidates the MAC, of course.
            %%
            %% Either keep a pristine one, or have the server ensure
            %% its state is a true causal descendant of ours.
            %%
            %% Or compute the delta (should be all removed) and send
            %% an appropriate remove operation, until the merge returns
            %% an identical CRDT.

            Merged = CMod:merge(CRDT, ResC),
            {ok, S#set{next_req=Req+1, crdt=Merged, mac=ResMAC}}
    end.

-ifdef(TEST).

local_ops_test() ->
    {ok, S} = logged_orswot:update({add_all, [a, b, c]},
                                   actor,
                                   logged_orswot:new()),
    ?assertEqual([a,b,c], logged_orswot:value(S)),
    PS1 = new(logged_orswot, S, <<"anteater">>, <<"C0256">>),
    ?assertEqual([a,b,c], lists:sort(value(PS1))),
    PS2 = update({add, d}, PS1),
    ?assertEqual([a,b,c,d], lists:sort(value(PS2))),
    PS3 = update({remove, b}, PS2),
    ?assertEqual([a,c,d], lists:sort(value(PS3))),
    PS4 = update({add, b}, PS3),
    ?assertEqual([a,b,c,d], lists:sort(value(PS4))),
    #set{adds=Adds, rems=Rems} = PS4,
    ?assert(ordsets:is_element(b, Adds)),
    ?assertNot(ordsets:is_element(b, Rems)),
    ?assert(ordsets:is_element(d, Adds)),
    #set{adds=Adds5, rems=Rems5} = update({remove, d}, PS4),
    ?assertNot(ordsets:is_element(d, Adds5)),
    ?assertNot(ordsets:is_element(d, Rems5)).

-endif.
