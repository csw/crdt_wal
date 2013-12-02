-module(storage).

-behaviour(gen_server).

-export([start_link/0, store_crdt/1, store_crdt_sync/1, load_crdt/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-type stored_crdt() :: {crdt_server:crdt_id(),
                        module(),
                        crdt_server:mac_key(),
                        crdt_server:crdt_bin()}.


-define(TABLE, crdt).


%% API

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec store_crdt(stored_crdt()) -> 'ok' | {'error', term()}.
store_crdt(Entry) ->
    gen_server:call(?SERVER, {store_crdt, Entry}).

-spec store_crdt_sync(stored_crdt()) -> 'ok' | {'error', term()}.
store_crdt_sync(Entry) ->
    gen_server:call(?SERVER, {store_crdt_sync, Entry}).

-spec load_crdt(crdt_server:crdt_id()) -> {'ok', stored_crdt()} |
                                          {'error', term()}.
load_crdt(Key) ->
    gen_server:call(?SERVER, {load_crdt, Key}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, _Table} = dets:open_file(?TABLE, []),
    {ok, {}}.

handle_call({store_crdt, Entry}, _From, {}) ->
    {reply, dets:insert(?TABLE, Entry), {}};

handle_call({store_crdt_sync, Entry}, _From, {}) ->
    ok = dets:insert(?TABLE, Entry),
    R = dets:sync(?TABLE),
    {reply, R, {}};

handle_call({load_crdt, Key}, _From, {}) ->
    R = case dets:lookup(?TABLE, Key) of
            [] ->
                io:format("CRDT ~p was not found!~n", [Key]),
                {error, not_found};
            [Stored] ->
                {ok, Stored}
        end,
    {reply, R, {}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok = dets:close(?TABLE),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

