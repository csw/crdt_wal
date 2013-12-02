-module(crdt_server).

-behaviour(gen_server).

%% API
-export([start_link/3, forward/3]).
-export([recover/3, finish_recovery/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export_type([crdt_id/0, crdt_bin/0, mac_key/0, mac/0]).

-define(SERVER, ?MODULE).

-define(MAC_KEY_LEN, 20). % SHA-1

-type crdt_id()    :: binary().
-type request_id() :: term().
-type crdt_op()    :: term().
-type crdt_bin()   :: binary().
-type mac_key()    :: binary().
-type mac()        :: binary().

-record(state, {cid         :: crdt_id(),
                mode=normal :: 'normal' | 'recovery',
                mod         :: module(),
                actor       :: riak_dt:actor(),
                mac_key     :: mac_key(),
                crdt=none   :: riak_dt:crdt() | 'none'
               }).

-type crdt_log_rec() :: {'passive_op', crdt_id(), request_id(), crdt_op()}.

%%%===================================================================
%%% API
%%%===================================================================

start_link(Actor, Key, Mode) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE,
                          {Actor, Key, Mode},
                          []).


recover(Pid, LSN, Op) ->
    gen_server:call(Pid, {recover, LSN, Op}).

finish_recovery(Pid) ->
    gen_server:call(Pid, finish_recovery).

forward(Pid, Request, From) ->
    gen_server:cast(Pid, {Request, From}),
    ok.

%% start_existing(Mod, CID) ->
%%     gen_server:start_link({local, ?SERVER}, ?MODULE, {existing, Mod, CID}, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({Actor, CID, {new, Mod}}) ->
    Key = crypto:strong_rand_bytes(?MAC_KEY_LEN),
    gen_server:cast(self(), sync_write),
    {ok, #state{cid=CID, mod=Mod, actor=Actor, mac_key=Key,
                crdt=Mod:new()}};

init({Actor, CID, recovery}) ->
    {ok, {CID, Mod, MACKey, CBin}} = storage:load_crdt(CID),
    CRDT = Mod:from_binary(CBin),
    {ok, #state{cid=CID, mod=Mod, actor=Actor, mac_key=MACKey,
                crdt=CRDT, mode=recovery}};

init({Actor, CID, normal}) ->
    {ok, {CID, Mod, MACKey, CBin}} = storage:load_crdt(CID),
    CRDT = Mod:from_binary(CBin),
    {ok, #state{cid=CID, mod=Mod, actor=Actor, mac_key=MACKey,
                crdt=CRDT, mode=normal}}.


%% init({existing, Mod, CID}) ->
%%     {ok, #state{}}.

handle_call({passive_op, RequestID, Op}, _From, S0=#state{mode=normal}) ->
    {ok, S} = do_passive(RequestID, Op, S0),
    {reply, ret_crdt(S), S};

handle_call(fetch, _From, S=#state{mode=normal}) ->
    {reply, ret_crdt(S), S};

handle_call({recover, LSN, Rec}, _From, S0=#state{mode=recovery}) ->
    {ok, S} = apply_record(Rec, LSN, S0),
    {reply, ok, S};

handle_call(finish_recovery, _From, S0=#state{mode=recovery}) ->
    {reply, ok, S0#state{mode=normal}}.



handle_cast({{passive_op, RequestID, Op}, From}, S0=#state{mode=normal}) ->
    {ok, S} = do_passive(RequestID, Op, S0),
    Reply = ret_crdt(S),
    gen_server:reply(From, Reply),
    {noreply, S};

handle_cast({fetch, From}, S=#state{mode=normal}) ->
    Reply = ret_crdt(S),
    gen_server:reply(From, Reply),
    {noreply, S};

handle_cast({{recover, LSN, Rec}, From}, S0=#state{mode=recovery}) ->
    {ok, S} = apply_record(Rec, LSN, S0),
    gen_server:reply(From, ok),
    {noreply, S};

handle_cast(sync_write, S=#state{}) ->
    ok = storage:store_crdt_sync(stored(S)),
    {noreply, S}.


handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec do_passive(request_id(), term(), #state{}) -> {'ok', #state{}}.
do_passive(RequestID, Op,
           S=#state{cid=CID, mod=Mod, actor=Actor, crdt=CRDT}) ->
    case requests:check(RequestID) of
        unknown ->
            case Mod:prepare(Op, Actor, CRDT) of
                {ok, Prep, _CPrep} ->
                    commit_record({passive_op, CID, RequestID, Prep}, S)
            end;
        committed ->
            %% Duplicate request, do nothing
            {ok, S}
    end.

-spec commit_record(crdt_log_rec(), #state{}) -> {'ok', #state{}}.
commit_record(Record, S=#state{cid=CID}) ->
    {ok, LSN} = wal_mgr:log_durable(CID, Record),
    apply_record(Record, LSN, S).

-spec apply_record(crdt_log_rec(), wal:lsn(), #state{}) -> {'ok', #state{}}.
apply_record({passive_op, CID, RequestID, Prep}=Op,
             LSN,
             S=#state{mod=Mod, actor=Actor, crdt=CRDT}) ->
    io:format("Applying log record ~16.16.0B to CRDT ~p:~n~p~n",
             [LSN, CID, Op]),
    {ok, CEff} = Mod:effect(Prep, Actor, CRDT),
    S1 = S#state{crdt=CEff},
    ok = requests:track(RequestID, committed),
    ok = storage:store_crdt(stored(S1)),
    {ok, S1}.

stored(#state{cid=CID, mod=Mod, mac_key=Key, crdt=CRDT}) ->
    {CID, Mod, Key, Mod:to_binary(CRDT)}.

-spec ret_crdt(#state{}) -> {'ok', crdt_bin(), mac()}.
ret_crdt(S=#state{mod=Mod, crdt=CRDT, mac_key=Key}) ->
    io:format("Returning CRDT from state:~n~p~n", [S]),
    CBin = Mod:to_binary(CRDT),
    MAC = crypto:sha_mac(Key, CBin),
    {ok, CBin, MAC}.
